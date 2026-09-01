package handlers

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"testing"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// failingBlob fails every shard Delete, standing in for a fan-out that could
// not reach any node.
type failingBlob struct {
	*fakeBlob
}

func (b *failingBlob) Delete(_ context.Context, _ config.NodeID, _ blob.DeleteRequest) (*blob.DeleteResponse, error) {
	return nil, errors.New("simulated node unreachable")
}

var _ BlobClient = (*failingBlob)(nil)

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

// tombstoneExists reports whether a delete left a tombstone behind.
func tombstoneExists(mc MetaClient, bucket, key string) bool {
	_, err := metaGet(context.Background(), mc, model.TableObjects, tombstoneKey(bucket, key))
	return err == nil
}

// A fan-out that succeeds is the common case, and it must leave no tombstone
// at all: that is the whole point of only using one on the failure path.
func TestDeleteStoredObjectSuccessWritesNoTombstone(t *testing.T) {
	t.Parallel()

	f := newDeleteFixture(t, "a.txt")

	err := deleteStoredObject(context.Background(), f.write.mc, f.write.bc, deleteTestBucket, "a.txt")

	require.NoError(t, err)
	assert.False(t, f.exists(t, "a.txt"), "the placement record must be gone")
	assert.False(t, tombstoneExists(f.write.mc, deleteTestBucket, "a.txt"), "a successful fan-out must leave no tombstone")
}

// When the fan-out fails the delete still succeeds from the caller's point of
// view, and the tombstone it leaves behind is what the reaper sweep reclaims
// from, so it has to survive decodable and naming exactly what the placement
// did.
func TestDeleteStoredObjectFailedFanOutWritesTombstone(t *testing.T) {
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

	bc := &failingBlob{fakeBlob: f.write.bc}
	err = deleteStoredObject(ctx, f.write.mc, bc, deleteTestBucket, "a.txt")
	require.NoError(t, err, "a failed fan-out must not fail the delete")

	info := decodeTombstone(t, f.write.mc, deleteTestBucket, "a.txt")
	assert.Equal(t, deleteTestBucket, info.Bucket)
	assert.Equal(t, "a.txt", info.Key)
	assert.Equal(t, objectHash, info.ObjectHash)
	assert.Equal(t, place.WriteEpoch, info.WriteEpoch)
	assert.Equal(t, place.DataShardNodes, info.DataShardNodes)
	assert.Equal(t, place.ParityShardNodes, info.ParityNodes)

	assert.False(t, f.exists(t, "a.txt"), "the placement record must still be removed")
	_, arnErr := metaGet(ctx, f.write.mc, model.TableObjects, objectARN(deleteTestBucket, "a.txt"))
	assert.Error(t, arnErr, "the listing row must still be removed")
}

// A tombstone write that fails after the fan-out also failed must fail the
// whole delete and leave both index entries in place: once the placement
// record is gone the tombstone would be the only record of where the shards
// are, so losing it there would leak them permanently.
func TestFailedTombstoneWriteAfterFailedFanOutLeavesBothIndexEntries(t *testing.T) {
	t.Parallel()

	f := newDeleteFixture(t, "a.txt")
	failingMeta := &putFailingMeta{
		fakeMeta: f.write.mc,
		failOn: func(key string) bool {
			return key == TableKey(model.TableObjects, tombstoneKey(deleteTestBucket, "a.txt"))
		},
	}
	bc := &failingBlob{fakeBlob: f.write.bc}

	err := deleteStoredObject(context.Background(), failingMeta, bc, deleteTestBucket, "a.txt")

	require.Error(t, err)
	assert.True(t, f.exists(t, "a.txt"), "the placement record must survive a failed tombstone write")

	_, arnErr := metaGet(context.Background(), f.write.mc, model.TableObjects, objectARN(deleteTestBucket, "a.txt"))
	assert.NoError(t, arnErr, "the listing row must survive a failed tombstone write")

	_, tombErr := metaGet(context.Background(), f.write.mc, model.TableObjects, tombstoneKey(deleteTestBucket, "a.txt"))
	assert.Error(t, tombErr, "no tombstone should exist when its own write failed")
}

// A missing key is still model.ErrNoSuchKeyError, unaffected by whether the
// fan-out would have succeeded: both the single-object 404 and the batch's
// idempotent report depend on it.
func TestDeleteStoredObjectMissingKeyIsNoSuchKey(t *testing.T) {
	t.Parallel()

	f := newDeleteFixture(t)

	err := deleteStoredObject(context.Background(), f.write.mc, f.write.bc, deleteTestBucket, "never-existed.txt")

	require.Error(t, err)
	assert.ErrorIs(t, err, model.ErrNoSuchKeyError)
}
