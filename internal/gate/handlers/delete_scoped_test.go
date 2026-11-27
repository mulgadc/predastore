package handlers

import (
	"context"
	"sync"
	"testing"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// racedBlob runs land once, ahead of every scoped shard delete, which is where
// a write the delete did not see lands.
type racedBlob struct {
	*fakeBlob

	once sync.Once
	land func()
}

func (b *racedBlob) DeleteEpoch(ctx context.Context, node config.NodeID, req blob.CommitRequest) (*blob.DeleteResponse, error) {
	b.once.Do(b.land)
	return b.fakeBlob.DeleteEpoch(ctx, node, req)
}

// storedPlacement reads the placement record a key resolves through.
func storedPlacement(t *testing.T, f deleteFixture, key string) ObjectToShardNodes {
	t.Helper()
	objectHash := model.ObjectHash(deleteTestBucket, key)
	raw, err := metaGet(context.Background(), f.write.mc, model.TableObjects, string(objectHash[:]))
	require.NoError(t, err)
	place, err := DecodePlacement(raw)
	require.NoError(t, err)
	return place
}

// An inline delete that read its placement, then lost the race to a write of
// the same key, and then stopped before it removed the placement record, must
// leave that write readable: its shard deletes name the generation it read.
//
// Verifies: ADR-0003:S7.
func TestS7_AnInlineDeleteRacedByAWriteLeavesTheWriteReadable(t *testing.T) {
	t.Parallel()

	f := newDeleteFixture(t, "a.txt")
	ctx := context.Background()
	objectHash := model.ObjectHash(deleteTestBucket, "a.txt")
	read := storedPlacement(t, f, "a.txt")
	body := []byte("the write that landed during the delete")

	bc := &racedBlob{fakeBlob: f.write.bc, land: func() { f.seed(t, "a.txt", body) }}
	// The delete's shard fan-out, then a crash before its placement removal.
	require.NoError(t, deleteObject(ctx, bc, deleteTestBucket, "a.txt", objectHash, read))

	current := storedPlacement(t, f, "a.txt")
	require.NotEqual(t, read.WriteEpoch, current.WriteEpoch, "setup: the write must mint a later generation")
	got, _, err := readObject(ctx, f.write.bc, f.write.cfg, deleteTestBucket, "a.txt", current, int64(len(body)), 0)
	require.NoError(t, err, "ADR-0003:S7: a stale inline delete removed the shards of a later write")
	assert.Equal(t, body, got)
}

// A failed delete writes its tombstone under the epoch prefix, at the key that
// names the placement's generation, so the reaper reclaims that generation.
func TestAFailedDeleteWritesItsTombstoneUnderTheEpochPrefix(t *testing.T) {
	t.Parallel()

	f := newDeleteFixture(t, "a.txt")
	place := storedPlacement(t, f, "a.txt")
	require.NotZero(t, place.WriteEpoch)

	require.NoError(t, deleteStoredObject(context.Background(), f.write.mc, &failingBlob{fakeBlob: f.write.bc}, deleteTestBucket, "a.txt"))

	info := decodeTombstone(t, f.write.mc, deleteTestBucket, "a.txt")
	assert.Equal(t, place.WriteEpoch, info.WriteEpoch)
}

// A reaper of the older release scans only the older prefix and sends an
// unscoped delete for each row it finds there. No failed delete, of an object
// or of a version, may give it a row.
//
// Verifies: ADR-0003:S7.
func TestS7_NoFailedDeleteWritesATombstoneUnderTheOlderPrefix(t *testing.T) {
	t.Parallel()

	f := newDeleteFixture(t, "a.txt", "b.txt")
	ctx := context.Background()
	failing := &failingBlob{fakeBlob: f.write.bc}
	toPreEpoch(t, f, "b.txt")
	versionHash := seedVersion(t, f.write, deleteTestBucket, "c.txt", "V1", []byte("version body"))

	require.NoError(t, deleteStoredObject(ctx, f.write.mc, failing, deleteTestBucket, "a.txt"))
	require.NoError(t, deleteStoredObject(ctx, f.write.mc, failing, deleteTestBucket, "b.txt"))
	require.NoError(t, deleteStoredVersion(ctx, f.write.mc, failing, deleteTestBucket, "c.txt", versionHash))

	older, err := metaScan(ctx, f.write.mc, model.TableObjects, DeletedObjectPrefix, 0)
	require.NoError(t, err)
	assert.Empty(t, older, "ADR-0003:S7: a failed delete wrote a row an older reaper deletes unscoped")
	scoped, err := metaScan(ctx, f.write.mc, model.TableObjects, DeletedEpochPrefix, 0)
	require.NoError(t, err)
	assert.Len(t, scoped, 3, "setup: each failed delete must leave its obligation")
}

// Two failed deletes of one key with a recreate between them owe two
// generations, so each keeps its own row.
func TestTwoFailedDeletesAcrossARecreateKeepTwoRows(t *testing.T) {
	t.Parallel()

	f := newDeleteFixture(t, "a.txt")
	ctx := context.Background()
	failing := &failingBlob{fakeBlob: f.write.bc}
	first := storedPlacement(t, f, "a.txt")
	require.NoError(t, deleteStoredObject(ctx, f.write.mc, failing, deleteTestBucket, "a.txt"))

	f.seed(t, "a.txt", []byte("the recreate"))
	second := storedPlacement(t, f, "a.txt")
	require.NotEqual(t, first.WriteEpoch, second.WriteEpoch, "setup: the recreate must mint a later generation")
	require.NoError(t, deleteStoredObject(ctx, f.write.mc, failing, deleteTestBucket, "a.txt"))

	found := scopedTombstones(t, f.write.mc, deleteTestBucket, "a.txt")
	require.Len(t, found, 2, "the second delete overwrote the first one's obligation: %v", found)
	assert.Contains(t, found, tombstoneKey(deleteTestBucket, "a.txt", first.WriteEpoch))
	assert.Contains(t, found, tombstoneKey(deleteTestBucket, "a.txt", second.WriteEpoch))
}

// toPreEpoch rewrites a stored object as one written before epochs: its record
// and every shard at epoch zero.
func toPreEpoch(t *testing.T, f deleteFixture, key string) ObjectToShardNodes {
	t.Helper()
	objectHash := model.ObjectHash(deleteTestBucket, key)
	place := storedPlacement(t, f, key)
	f.write.bc.mu.Lock()
	for i := range place.AllNodes() {
		id := shardID{key: objectHash, index: uint32(i)}
		shard := f.write.bc.shards[id]
		shard.epoch = 0
		f.write.bc.shards[id] = shard
	}
	f.write.bc.mu.Unlock()
	place.WriteEpoch = 0
	record, err := EncodePlacement(place)
	require.NoError(t, err)
	require.NoError(t, metaPut(context.Background(), f.write.mc, model.TableObjects, string(objectHash[:]), record))
	return place
}

// A delete of an object written before epochs removes its shards at epoch
// zero, and leaves no tombstone.
func TestADeleteOfAPreEpochObjectReclaimsItsShards(t *testing.T) {
	t.Parallel()

	f := newDeleteFixture(t, "a.txt")
	place := toPreEpoch(t, f, "a.txt")
	objectHash := model.ObjectHash(deleteTestBucket, "a.txt")

	require.NoError(t, deleteStoredObject(context.Background(), f.write.mc, f.write.bc, deleteTestBucket, "a.txt"))

	f.write.bc.mu.Lock()
	defer f.write.bc.mu.Unlock()
	for i := range place.AllNodes() {
		_, held := f.write.bc.shards[shardID{key: objectHash, index: uint32(i)}]
		assert.False(t, held, "index %d of a pre-epoch object survived its delete", i)
	}
	assert.False(t, tombstoneExists(t, f.write.mc, deleteTestBucket, "a.txt"))
}

// A failed delete of a pre-epoch object keeps its obligation in a tombstone
// under the epoch prefix at epoch zero, which the reaper does not drop as
// legacy.
func TestAFailedDeleteOfAPreEpochObjectKeepsItsObligation(t *testing.T) {
	t.Parallel()

	f := newDeleteFixture(t, "a.txt")
	toPreEpoch(t, f, "a.txt")

	require.NoError(t, deleteStoredObject(context.Background(), f.write.mc, &failingBlob{fakeBlob: f.write.bc}, deleteTestBucket, "a.txt"))

	info := decodeTombstone(t, f.write.mc, deleteTestBucket, "a.txt")
	assert.Zero(t, info.WriteEpoch)
}

var _ BlobClient = (*racedBlob)(nil)
