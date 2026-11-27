package reaper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"slices"
	"testing"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rowLayout is the key a tombstone row is stored under.
type rowLayout int

const (
	// layoutEpoch is the row a failed delete writes now.
	layoutEpoch rowLayout = iota
	// layoutOlderHashed is the older release's row, keyed by hash and name.
	layoutOlderHashed
	// layoutOlderPlain is the oldest row, keyed by name only.
	layoutOlderPlain
)

// tombstoneRow is one tombstone as a test builds and stores it.
type tombstoneRow struct {
	bucket, key string
	hash        [32]byte
	epoch       uint64
	data, m     []config.NodeID
	layout      rowLayout
}

// storedKey is the full stored key of a row in its layout.
func (row tombstoneRow) storedKey() string {
	switch row.layout {
	case layoutOlderHashed:
		return handlers.TableKey(model.TableObjects,
			handlers.DeletedObjectPrefix+"#"+hex.EncodeToString(row.hash[:])+"/"+row.bucket+"/"+row.key)
	case layoutOlderPlain:
		return handlers.TableKey(model.TableObjects, handlers.DeletedObjectPrefix+row.bucket+"/"+row.key)
	default:
		return handlers.TableKey(model.TableObjects,
			handlers.DeletedObjectKey(row.bucket, row.key, row.hash, row.epoch))
	}
}

// tombstone builds a tombstone naming the given data and parity nodes, keyed
// by a hash unique to the name — deterministic, the way model.ObjectHash is,
// so the recreate case can reuse it.
func tombstone(name string, epoch uint64, data, parity []config.NodeID) tombstoneRow {
	return tombstoneRow{
		bucket: "b", key: name,
		hash: sha256.Sum256([]byte(name)), epoch: epoch,
		data: data, m: parity,
	}
}

// store writes a tombstone row to the cluster's meta store, gob-encoded the
// way a failed delete leaves one, and holds the shards it names on their
// nodes, as a completed write left them.
func (c *cluster) store(t *testing.T, row tombstoneRow) string {
	t.Helper()

	stored := c.storeTombstoneOnly(t, row)

	for i, n := range row.data {
		c.blob.hold(n, row.hash, uint32(i), row.epoch)
	}
	for i, n := range row.m {
		c.blob.hold(n, row.hash, uint32(len(row.data)+i), row.epoch)
	}

	return stored
}

// storeTombstoneOnly writes the tombstone row without holding any shard on
// any node, as if the node had already reclaimed them.
func (c *cluster) storeTombstoneOnly(t *testing.T, row tombstoneRow) string {
	t.Helper()

	info := handlers.DeletedObjectInfo{
		Bucket: row.bucket, Key: row.key, ObjectHash: row.hash,
		WriteEpoch: row.epoch, DataShardNodes: row.data, ParityNodes: row.m,
	}
	var buf bytes.Buffer
	require.NoError(t, gob.NewEncoder(&buf).Encode(info))

	stored := row.storedKey()
	c.meta.put(stored, buf.Bytes())

	return stored
}

// recordLivePlacement writes a placement record at hash, at the given epoch.
// A recreate lands its new write on the same nodes and indices a delete
// tombstoned — object hashes are deterministic on bucket and key — but always
// mints a new WriteEpoch, which is the only thing that tells it apart from
// the record deleteStoredObject has not removed yet for this very tombstone.
func (c *cluster) recordLivePlacement(t *testing.T, hash [32]byte, epoch uint64, data, parity []config.NodeID) {
	t.Helper()
	place := handlers.ObjectToShardNodes{
		Size: 1, WriteEpoch: epoch, DataShardNodes: data, ParityShardNodes: parity,
	}
	raw, err := handlers.EncodePlacement(place)
	require.NoError(t, err)
	c.meta.put(handlers.TableKey(model.TableObjects, string(hash[:])), raw)
}

func nodes(ids ...int) []config.NodeID {
	out := make([]config.NodeID, len(ids))
	for i, id := range ids {
		out[i] = config.NodeID(id)
	}
	return out
}

// A pass must reclaim exactly the shards a tombstone names, at the indices
// the write path assigned them — data shards first, then parity — and then
// remove the tombstone itself.
func TestPassReclaimsExactlyTheNamedShardsThenDropsTheTombstone(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	row := tombstone("object", 5, nodes(1, 2), nodes(3))
	stored := c.store(t, row)

	svc := c.service(8)
	require.NoError(t, svc.Pass(t.Context()))

	assert.False(t, c.blob.has(1, row.hash, 0), "data shard 0 must be reclaimed")
	assert.False(t, c.blob.has(2, row.hash, 1), "data shard 1 must be reclaimed")
	assert.False(t, c.blob.has(3, row.hash, 2), "parity shard must be reclaimed at index 2")
	assert.False(t, c.meta.has(stored), "the tombstone must be dropped once every shard is gone")

	stats := svc.Stats()
	assert.Equal(t, int64(1), stats.Scanned)
	assert.Equal(t, int64(1), stats.Reclaimed)
	assert.Zero(t, stats.Failed)
	assert.Zero(t, stats.Pending)
}

// Object hashes are deterministic, so a key deleted and then recreated lands
// its new write on the same nodes and indices the tombstone names, at a later
// WriteEpoch. The sweep used to drop such a tombstone without a shard delete,
// which stranded the owed generation. It now deletes the tombstone's own
// epoch, which leaves the recreate's shards, and then drops the tombstone.
func TestARecreatedKeyLosesOnlyTheTombstonedGeneration(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	row := tombstone("recreated", 5, nodes(1, 2), nodes(3))
	stored := c.store(t, row)
	c.holdGeneration(row, row.epoch+1)
	c.recordLivePlacement(t, row.hash, row.epoch+1, row.data, row.m)

	svc := c.service(8)
	require.NoError(t, svc.Pass(t.Context()))

	assert.ElementsMatch(t, []uint64{5, 5, 5}, c.blob.epochs, "every shard delete must name the tombstone's generation")
	for i, n := range append(row.data, row.m...) {
		assert.False(t, c.blob.hasEpoch(n, row.hash, uint32(i), row.epoch), "node %d kept the owed generation", n)
		assert.True(t, c.blob.hasEpoch(n, row.hash, uint32(i), row.epoch+1), "node %d lost the recreated shard", n)
	}
	assert.False(t, c.meta.has(stored), "the tombstone must be dropped once its generation is gone")
	assert.Equal(t, int64(1), svc.Stats().Reclaimed)
}

// holdGeneration holds a later generation of a row's shards on every node the
// row names, as a recreate leaves them.
func (c *cluster) holdGeneration(row tombstoneRow, epoch uint64) {
	for i, n := range append(slices.Clone(row.data), row.m...) {
		c.blob.hold(n, row.hash, uint32(i), epoch)
	}
}

// The regression case: deleteStoredObject writes the tombstone before it
// removes the placement record, so a pass can land in that window and read
// the tombstone's own object still in the index. A plain existence check
// would mistake that for a recreate and drop the tombstone without reclaiming
// a single shard — leaking them permanently, on every delete. The epoch
// match is what tells the two apart: the tombstone must survive untouched
// while the placement is still there at the same epoch, and only once the
// placement is gone does a later pass reclaim it.
func TestSameEpochPlacementIsTheDeleteStillInFlightNotARecreate(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	row := tombstone("in-flight", 5, nodes(1, 2), nodes(3))
	stored := c.store(t, row)
	c.recordLivePlacement(t, row.hash, row.epoch, row.data, row.m)

	svc := c.service(8)
	require.NoError(t, svc.Pass(t.Context()))

	assert.Empty(t, c.blob.deletes, "no shard delete may reach the blob client while the delete is still in flight")
	assert.True(t, c.meta.has(stored), "the tombstone must survive while its own placement record is still there")
	stats := svc.Stats()
	assert.Zero(t, stats.Reclaimed)
	assert.Zero(t, stats.Failed, "a delete still in flight is not a failure")
	assert.Equal(t, int64(1), stats.Pending)

	// deleteStoredObject completes: the placement record is gone. The next
	// pass must now reclaim the shards and drop the tombstone normally.
	c.meta.mu.Lock()
	delete(c.meta.rows, handlers.TableKey(model.TableObjects, string(row.hash[:])))
	c.meta.mu.Unlock()

	require.NoError(t, svc.Pass(t.Context()))

	assert.False(t, c.blob.has(1, row.hash, 0))
	assert.False(t, c.blob.has(2, row.hash, 1))
	assert.False(t, c.blob.has(3, row.hash, 2))
	assert.False(t, c.meta.has(stored), "the tombstone must be reclaimed once the placement record is gone")
	assert.Equal(t, int64(1), svc.Stats().Reclaimed)
	assert.Zero(t, svc.Stats().Pending)
}

// A row under the older prefix with WriteEpoch == 0 predates the field: its
// delete already fanned the shards out inline, so the sweep must drop it
// without touching a shard or even consulting the placement table.
func TestLegacyTombstoneIsDroppedWithoutDeletingShards(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	c.meta.failLeader(errors.New("no leader"))
	row := tombstone("legacy", 0, nodes(1, 2), nodes(3))
	row.layout = layoutOlderPlain
	stored := c.store(t, row)

	svc := c.service(8)
	require.NoError(t, svc.Pass(t.Context()))

	assert.Empty(t, c.blob.deletes, "a legacy tombstone's shards were already fanned out inline")
	assert.True(t, c.blob.has(1, row.hash, 0), "shards a legacy tombstone names must be left untouched")
	assert.False(t, c.meta.has(stored), "the legacy tombstone must still be dropped")
	assert.Equal(t, int64(1), svc.Stats().Reclaimed)
	assert.Zero(t, svc.Stats().Failed)
}

// The whole point of the design: a tombstone whose node is down at delete
// time survives the pass rather than leaking the shard, and a later pass
// reclaims it once the node returns.
func TestUnreachableNodeKeepsTheTombstoneUntilItRecovers(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	row := tombstone("stuck", 5, nodes(1, 2), nodes(3))
	stored := c.store(t, row)
	c.blob.stop(2)

	svc := c.service(8)
	require.NoError(t, svc.Pass(t.Context()))

	assert.True(t, c.meta.has(stored), "a tombstone with an unreachable node must survive the pass")
	assert.False(t, c.blob.has(1, row.hash, 0), "reachable shards are reclaimed even when one node is down")
	stats := svc.Stats()
	assert.Equal(t, int64(1), stats.Failed)
	assert.Equal(t, int64(1), stats.Pending)

	c.blob.resume(2)
	require.NoError(t, svc.Pass(t.Context()))

	assert.False(t, c.meta.has(stored), "the tombstone must be reclaimed once the node returns")
	assert.False(t, c.blob.has(2, row.hash, 1))
	assert.Equal(t, int64(1), svc.Stats().Reclaimed)
	assert.Zero(t, svc.Stats().Pending)
}

// DeleteResponse{Deleted: false} means the node never held the shard, which is
// success — a previous pass or the node itself already reclaimed it — and
// must not be treated as a failure that keeps the tombstone around.
func TestAnAlreadyGoneShardIsNotAFailure(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	// No c.blob.hold call: the node has nothing at this address, so Delete
	// answers Deleted: false without an error.
	row := tombstone("already-gone", 5, nodes(1), nil)
	stored := c.storeTombstoneOnly(t, row)

	svc := c.service(8)
	require.NoError(t, svc.Pass(t.Context()))

	assert.False(t, c.meta.has(stored))
	assert.Equal(t, int64(1), svc.Stats().Reclaimed)
	assert.Zero(t, svc.Stats().Failed)
}

// The sweep pages only the tombstone prefix, so listing rows, part keys and
// placement records sharing the objects table must never surface as a task.
func TestSweepIgnoresOtherTableRows(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	row := tombstone("real", 5, nodes(1), nodes(2))
	c.store(t, row)

	otherHash := sha256.Sum256([]byte("some-other-object"))
	for _, key := range []string{
		"arn:aws:s3:::bucket/an-object-key-here",
		string(otherHash[:]),
		"part:0198f2a1-6c3e-7c19-9b1e-4f2d5a6b7c8d:00001",
	} {
		c.meta.put(handlers.TableKey(model.TableObjects, key), []byte("not a tombstone"))
	}

	svc := c.service(8)
	require.NoError(t, svc.Pass(t.Context()))

	assert.Equal(t, int64(1), svc.Stats().Scanned, "only the tombstone row is a reclaim candidate")
}

// TestPagesThroughMoreTombstonesThanOnePage exercises the cursor the same way
// repair's does: the tombstone table does not fit one response on a cluster
// deleting at any volume, and a scan that silently stopped at the first page
// would reclaim only the alphabetical head of the keyspace.
func TestPagesThroughMoreTombstonesThanOnePage(t *testing.T) {
	t.Parallel()

	const count = 37
	c := newCluster(t)

	stored := make([]string, 0, count)
	for i := range count {
		row := tombstone(fmt.Sprintf("object-%03d", i), 1, nodes(1), nil)
		stored = append(stored, c.store(t, row))
	}

	svc, err := New(Config{Meta: c.meta, Blob: c.blob, Workers: 3, PageSize: 4})
	require.NoError(t, err)
	require.NoError(t, svc.Pass(t.Context()))

	assert.Equal(t, int64(count), svc.Stats().Scanned, "every page must be walked")
	assert.Equal(t, int64(count), svc.Stats().Reclaimed)
	for _, key := range stored {
		assert.False(t, c.meta.has(key))
	}
}

func TestRunStopsWithItsContext(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	c.store(t, tombstone("object", 1, nodes(1), nil))
	svc := c.service(8)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()

	cancel()
	require.NoError(t, <-done, "a cancelled sweep is a clean stop, not a failure")
}

func TestNewRejectsAServiceThatCouldNotSweep(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	base := Config{Meta: c.meta, Blob: c.blob}

	t.Run("no clients", func(t *testing.T) {
		t.Parallel()
		cfg := base
		cfg.Meta = nil
		_, err := New(cfg)
		assert.Error(t, err)
	})

	t.Run("defaults fill in", func(t *testing.T) {
		t.Parallel()
		svc, err := New(base)
		require.NoError(t, err)
		assert.Positive(t, svc.workers)
		assert.Equal(t, defaultPageSize, svc.pageSize)
		assert.Equal(t, defaultInterval, svc.interval)
	})
}

// Emptying a versioned bucket deletes every version of a key in succession.
// Each failed fan-out owes its own shards, and one record per key would leave
// the first version's shards unaccounted for while the bucket is allowed to
// disappear.
func TestBothVersionsOfOneKeyAreIndependentlyReclaimed(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	first := tombstone("a.txt\x00V1", 5, nodes(1, 2), nodes(3))
	second := tombstone("a.txt\x00V2", 6, nodes(2, 3), nodes(1))
	first.key, second.key = "a.txt", "a.txt"
	require.NotEqual(t, first.hash, second.hash, "two versions must not share a shard set")

	firstRow := c.store(t, first)
	secondRow := c.store(t, second)
	require.NotEqual(t, firstRow, secondRow, "the two versions share one reclaim record")

	svc := c.service(8)
	require.NoError(t, svc.Pass(t.Context()))

	for _, row := range []tombstoneRow{first, second} {
		for i, n := range row.data {
			assert.False(t, c.blob.has(n, row.hash, uint32(i)),
				"data shard %d of %x was not reclaimed", i, row.hash)
		}
		for i, n := range row.m {
			assert.False(t, c.blob.has(n, row.hash, uint32(len(row.data)+i)),
				"parity shard of %x was not reclaimed", row.hash)
		}
	}
	assert.False(t, c.meta.has(firstRow), "the first version's record survived its reclaim")
	assert.False(t, c.meta.has(secondRow), "the second version's record survived its reclaim")

	stats := svc.Stats()
	assert.Equal(t, int64(2), stats.Scanned)
	assert.Equal(t, int64(2), stats.Reclaimed)
	assert.Zero(t, stats.Failed)
}

// A row written before the upgrade is the one most likely to still owe
// shards, so a pass has to reclaim every layout: the two older ones under the
// older prefix and the current one under the epoch prefix.
//
// The worker is built after the rows are written and holds no state between
// passes, so this is also what a restart reads: durable rows and a fresh
// scan.
func TestEveryRowLayoutReclaimsInOnePass(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	plain := tombstone("plain.txt", 4, nodes(1, 2), nodes(3))
	plain.layout = layoutOlderPlain
	hashed := tombstone("hashed.txt\x00V1", 5, nodes(2, 3), nodes(1))
	hashed.key, hashed.layout = "hashed.txt", layoutOlderHashed
	current := tombstone("current.txt", 6, nodes(3, 1), nodes(2))

	rows := []tombstoneRow{plain, hashed, current}
	stored := make([]string, 0, len(rows))
	for _, row := range rows {
		stored = append(stored, c.store(t, row))
	}

	svc := c.service(8)
	require.NoError(t, svc.Pass(t.Context()))

	for i, row := range rows {
		for j, n := range append(slices.Clone(row.data), row.m...) {
			assert.False(t, c.blob.has(n, row.hash, uint32(j)), "row %q kept index %d on node %d", row.key, j, n)
		}
		assert.False(t, c.meta.has(stored[i]), "row %q survived its reclaim", row.key)
	}
	assert.Equal(t, int64(3), svc.Stats().Reclaimed)
}

// A reclaim that found no placement record, then lost the race to a write that
// recreated the key at the same positions, must leave the write's shards: the
// delete names the tombstone's generation, and the write made a later one.
//
// Verifies: ADR-0003:S7.
func TestS7_AReclaimRacedByARecreateKeepsTheNewGeneration(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	row := tombstone("raced", 4, nodes(1, 2), nodes(3))
	stored := c.store(t, row)
	const recreated = 9
	c.blob.beforeDelete = func() {
		for i, n := range append(row.data, row.m...) {
			c.blob.hold(n, row.hash, uint32(i), recreated)
		}
		c.recordLivePlacement(t, row.hash, recreated, row.data, row.m)
	}

	require.NoError(t, c.service(8).Pass(t.Context()))

	for i, n := range append(row.data, row.m...) {
		epoch, ok := c.blob.epochAt(n, row.hash, uint32(i))
		require.True(t, ok, "ADR-0003:S7: node %d lost the recreated shard at index %d to a stale reclaim", n, i)
		assert.Equal(t, uint64(recreated), epoch)
	}
	assert.ElementsMatch(t, []uint64{4, 4, 4}, c.blob.epochs, "every shard delete must name the tombstone's generation")
	assert.False(t, c.meta.has(stored))
}

// A failed delete of an object written before epochs leaves a row under the
// epoch prefix at epoch zero. It is owed like any other: kept while a node is
// down, and reclaimed at epoch zero once the node returns.
func TestAnEpochZeroRowUnderTheEpochPrefixIsReclaimedOnceTheNodeReturns(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	row := tombstone("pre-epoch", 0, nodes(1, 2), nodes(3))
	stored := c.store(t, row)
	c.blob.stop(2)

	svc := c.service(8)
	require.NoError(t, svc.Pass(t.Context()))
	assert.True(t, c.meta.has(stored), "a row with a shard still owed must stay")
	assert.True(t, c.blob.has(2, row.hash, 1))

	c.blob.resume(2)
	require.NoError(t, svc.Pass(t.Context()))
	for i, n := range append(row.data, row.m...) {
		assert.False(t, c.blob.has(n, row.hash, uint32(i)), "node %d still holds index %d", n, i)
	}
	assert.False(t, c.meta.has(stored))
	for _, epoch := range c.blob.epochs {
		assert.Zero(t, epoch, "every delete must name epoch zero")
	}
}

// reclaimAcrossARecreate stores a row in a layout, recreates its key at a later
// epoch, and holds one node down. The row must stay with that node's shard
// owed, then go once the node returns, and the recreate must stay whole.
func reclaimAcrossARecreate(t *testing.T, layout rowLayout) {
	t.Helper()

	c := newCluster(t)
	row := tombstone("recreated", 4, nodes(1, 2), nodes(3))
	row.layout = layout
	stored := c.store(t, row)
	const recreated = 9
	c.holdGeneration(row, recreated)
	c.recordLivePlacement(t, row.hash, recreated, row.data, row.m)
	c.blob.stop(2)

	svc := c.service(8)
	require.NoError(t, svc.Pass(t.Context()))
	assert.True(t, c.meta.has(stored), "ADR-0003:S7: the row was dropped while node 2 still owes its shard")
	assert.True(t, c.blob.hasEpoch(2, row.hash, 1, row.epoch), "setup: node 2 was down")
	assert.False(t, c.blob.hasEpoch(1, row.hash, 0, row.epoch), "a reachable node kept the owed generation")
	assert.Equal(t, int64(1), svc.Stats().Pending)

	c.blob.resume(2)
	require.NoError(t, svc.Pass(t.Context()))
	assert.False(t, c.meta.has(stored), "the row must go once every shard delete succeeds")
	for i, n := range append(slices.Clone(row.data), row.m...) {
		assert.False(t, c.blob.hasEpoch(n, row.hash, uint32(i), row.epoch), "node %d kept the owed generation", n)
		assert.True(t, c.blob.hasEpoch(n, row.hash, uint32(i), recreated),
			"ADR-0003:S7: node %d lost the recreated shard at index %d", n, i)
	}
	for _, epoch := range c.blob.epochs {
		assert.Equal(t, row.epoch, epoch, "ADR-0003:S7: a shard delete named a generation the row does not owe")
	}
}

// A row under the epoch prefix whose key a recreate now holds at another epoch
// deletes its own epoch, keeps the recreated shards, and stays until its last
// node returns.
//
// Verifies: ADR-0003:S7.
func TestS7_ARowWhoseKeyWasRecreatedReclaimsOnlyItsEpoch(t *testing.T) {
	t.Parallel()
	reclaimAcrossARecreate(t, layoutEpoch)
}

// A positive-epoch row in the older release's format stays through a failed
// shard delete, and a later pass reclaims its epoch while a recreated object
// at another epoch stays intact.
//
// Verifies: ADR-0003:S7.
func TestS7_AnOlderPositiveEpochRowReclaimsOnlyItsEpoch(t *testing.T) {
	t.Parallel()
	reclaimAcrossARecreate(t, layoutOlderHashed)
}

// A follower that lags can answer an older epoch while the leader still names
// the tombstoned one. Read from a follower, that looks like a recreate and
// would delete the current generation. Read from the leader, it is the delete
// still in flight, and the row and its shards stay.
//
// Verifies: ADR-0003:S7.
func TestS7_ALaggingFollowerCannotMakeTheCurrentGenerationLookRecreated(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	row := tombstone("lagging", 9, nodes(1, 2), nodes(3))
	stored := c.store(t, row)
	c.recordLivePlacement(t, row.hash, row.epoch, row.data, row.m)

	older, err := handlers.EncodePlacement(handlers.ObjectToShardNodes{
		Size: 1, WriteEpoch: 4, DataShardNodes: row.data, ParityShardNodes: row.m,
	})
	require.NoError(t, err)
	liveKey := handlers.TableKey(model.TableObjects, string(row.hash[:]))
	c.meta.lag(liveKey, older)
	seen, err := c.meta.Get(t.Context(), liveKey)
	require.NoError(t, err)
	require.Equal(t, older, seen, "setup: the follower must answer the older epoch")

	svc := c.service(8)
	require.NoError(t, svc.Pass(t.Context()))

	assert.Empty(t, c.blob.deletes, "ADR-0003:S7: a lagging follower's read sent a shard delete for the current generation")
	assert.True(t, c.meta.has(stored), "ADR-0003:S7: the row was dropped while its generation is current")
	for i, n := range append(slices.Clone(row.data), row.m...) {
		assert.True(t, c.blob.hasEpoch(n, row.hash, uint32(i), row.epoch), "ADR-0003:S7: node %d lost index %d", n, i)
	}
	assert.Zero(t, svc.Stats().Failed, "a delete still in flight is not a failure")
}

// When the leader cannot answer, the sweep cannot tell a recreate from the
// delete still in flight, so it keeps the row and sends no shard delete. A
// later pass, with a leader, reclaims it.
//
// Verifies: ADR-0003:S7.
func TestS7_ALeaderReadFailureKeepsTheRow(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	row := tombstone("no-leader", 5, nodes(1, 2), nodes(3))
	stored := c.store(t, row)
	c.meta.failLeader(errors.New("no meta replica would serve a leader read"))

	svc := c.service(8)
	require.NoError(t, svc.Pass(t.Context()))

	assert.Empty(t, c.blob.deletes, "ADR-0003:S7: a shard delete was sent with no leader answer")
	assert.True(t, c.meta.has(stored), "ADR-0003:S7: the row was dropped with no leader answer")
	stats := svc.Stats()
	assert.Equal(t, int64(1), stats.Failed)
	assert.Equal(t, int64(1), stats.Pending)

	c.meta.failLeader(nil)
	require.NoError(t, svc.Pass(t.Context()))
	assert.False(t, c.meta.has(stored), "a pass with a leader must reclaim the row")
	assert.False(t, c.blob.has(1, row.hash, 0))
}

// Two failed deletes of one key, with a recreate between them, owe two
// generations. The reclaim of the first is paused while the second delete
// writes its row, and must remove only its own row and its own generation.
//
// Verifies: ADR-0003:S7.
func TestS7_TwoFailedDeletesOfOneKeyKeepTheirOwnRows(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	first := tombstone("twice", 4, nodes(1, 2), nodes(3))
	firstRow := c.store(t, first)
	second := first
	second.epoch = 9

	secondRow := make(chan string, 1)
	c.blob.beforeDelete = func() {
		// The recreate, then a second failed delete that has written its row
		// and not yet removed its placement.
		c.recordLivePlacement(t, second.hash, second.epoch, second.data, second.m)
		secondRow <- c.store(t, second)
	}

	svc := c.service(8)
	require.NoError(t, svc.Pass(t.Context()))
	stored := <-secondRow
	require.NotEqual(t, firstRow, stored, "ADR-0003:S7: two failed deletes of one key share one row")

	assert.False(t, c.meta.has(firstRow), "the first row must go once its generation is gone")
	assert.True(t, c.meta.has(stored), "ADR-0003:S7: the reclaim of the first row removed the second")
	for i, n := range append(slices.Clone(first.data), first.m...) {
		assert.False(t, c.blob.hasEpoch(n, first.hash, uint32(i), first.epoch), "node %d kept the first generation", n)
		assert.True(t, c.blob.hasEpoch(n, second.hash, uint32(i), second.epoch),
			"ADR-0003:S7: node %d lost the second generation at index %d", n, i)
	}

	// The second delete removes its placement, and a later pass reclaims the
	// second row at its own epoch.
	c.meta.mu.Lock()
	delete(c.meta.rows, handlers.TableKey(model.TableObjects, string(second.hash[:])))
	c.meta.mu.Unlock()
	require.NoError(t, svc.Pass(t.Context()))
	assert.False(t, c.meta.has(stored))
	assert.False(t, c.blob.has(1, second.hash, 0))
}
