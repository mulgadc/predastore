package handlers

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"testing"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// deleteStoredObject takes no BlobClient: that it issues no shard RPC is
// guaranteed by its signature, not by a runtime check against a stub that
// fails the test if reached.

// putFailingMeta fails a Put whose key matches failOn, and otherwise behaves
// like the meta store underneath it.
type putFailingMeta struct {
	*fakeMeta

	failOn func(key string) bool
}

func (m *putFailingMeta) Put(ctx context.Context, key string, value []byte) error {
	if m.failOn != nil && m.failOn(key) {
		return errors.New("simulated meta failure")
	}
	return m.fakeMeta.Put(ctx, key, value)
}

var _ MetaClient = (*putFailingMeta)(nil)

// tombstoneKey is the stored key one delete's tombstone lands under.
func tombstoneKey(bucket, key string) string { return DeletedObjectPrefix + bucket + "/" + key }

// decodeTombstone reads and gob-decodes the tombstone a delete left.
func decodeTombstone(t *testing.T, mc MetaClient, bucket, key string) DeletedObjectInfo {
	t.Helper()
	data, err := metaGet(context.Background(), mc, model.TableObjects, tombstoneKey(bucket, key))
	require.NoError(t, err)

	var info DeletedObjectInfo
	require.NoError(t, gob.NewDecoder(bytes.NewReader(data)).Decode(&info))
	return info
}

// A delete no longer talks to shard nodes: the shard fan-out is deferred to
// the reaper sweep. deleteStoredObject has no BlobClient parameter at all, so
// this exercises the ordinary path and confirms the object disappears without
// one being reachable anywhere in the call.
func TestDeleteStoredObjectIssuesNoShardRPC(t *testing.T) {
	t.Parallel()

	f := newDeleteFixture(t, "a.txt")

	err := deleteStoredObject(context.Background(), f.write.mc, deleteTestBucket, "a.txt")

	require.NoError(t, err)
	assert.False(t, f.exists(t, "a.txt"), "the placement record must be gone")
}

// The tombstone is what the reaper sweep reclaims from, so it has to survive
// the delete decodable and naming exactly what the placement did.
func TestDeleteStoredObjectLeavesADecodableTombstone(t *testing.T) {
	t.Parallel()

	f := newDeleteFixture(t)
	ctx := context.Background()
	objectHash := model.ObjectHash(deleteTestBucket, "a.txt")

	place, _, err := f.write.write(ctx, objectHash, bytes.NewReader([]byte("body")), 4)
	require.NoError(t, err)
	record, err := EncodePlacement(place)
	require.NoError(t, err)
	require.NoError(t, metaPut(ctx, f.write.mc, model.TableObjects, string(objectHash[:]), record))
	require.NoError(t, metaPut(ctx, f.write.mc, model.TableObjects, objectARN(deleteTestBucket, "a.txt"), objectHash[:]))

	require.NoError(t, deleteStoredObject(ctx, f.write.mc, deleteTestBucket, "a.txt"))

	info := decodeTombstone(t, f.write.mc, deleteTestBucket, "a.txt")
	assert.Equal(t, deleteTestBucket, info.Bucket)
	assert.Equal(t, "a.txt", info.Key)
	assert.Equal(t, objectHash, info.ObjectHash)
	assert.Equal(t, place.WriteEpoch, info.WriteEpoch)
	assert.Equal(t, place.DataShardNodes, info.DataShardNodes)
	assert.Equal(t, place.ParityShardNodes, info.ParityNodes)
}

// A tombstone write that fails must fail the whole delete and leave the
// object exactly as it was: once the placement record is gone the tombstone
// is the only record of where the shards are, so losing it there would leak
// them permanently.
func TestFailedTombstoneWriteLeavesTheObjectVisible(t *testing.T) {
	t.Parallel()

	f := newDeleteFixture(t, "a.txt")
	failingMeta := &putFailingMeta{
		fakeMeta: f.write.mc,
		failOn: func(key string) bool {
			return key == TableKey(model.TableObjects, tombstoneKey(deleteTestBucket, "a.txt"))
		},
	}

	err := deleteStoredObject(context.Background(), failingMeta, deleteTestBucket, "a.txt")

	require.Error(t, err)
	assert.True(t, f.exists(t, "a.txt"), "the placement record must survive a failed tombstone write")

	_, arnErr := metaGet(context.Background(), f.write.mc, model.TableObjects, objectARN(deleteTestBucket, "a.txt"))
	assert.NoError(t, arnErr, "the listing row must survive a failed tombstone write")

	_, tombErr := metaGet(context.Background(), f.write.mc, model.TableObjects, tombstoneKey(deleteTestBucket, "a.txt"))
	assert.Error(t, tombErr, "no tombstone should exist when its own write failed")
}

// A missing key is still model.ErrNoSuchKeyError, unaffected by deferring the
// shard delete: both the single-object 404 and the batch's idempotent report
// depend on it.
func TestDeleteStoredObjectMissingKeyIsNoSuchKey(t *testing.T) {
	t.Parallel()

	f := newDeleteFixture(t)

	err := deleteStoredObject(context.Background(), f.write.mc, deleteTestBucket, "never-existed.txt")

	require.Error(t, err)
	assert.ErrorIs(t, err, model.ErrNoSuchKeyError)
}
