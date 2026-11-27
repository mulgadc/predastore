package handlers

import (
	"bytes"
	"context"
	"encoding/gob"
	"testing"

	"github.com/mulgadc/predastore/internal/blob/blobtest"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// storedTombstones decodes every delete tombstone in the objects table,
// whatever prefix names it.
func storedTombstones(t *testing.T, mc MetaClient) []DeletedObjectInfo {
	t.Helper()
	items, err := mc.Scan(context.Background(), TableKey(model.TableObjects, "deleted"), 0)
	require.NoError(t, err)
	infos := make([]DeletedObjectInfo, 0, len(items))
	for _, item := range items {
		var info DeletedObjectInfo
		require.NoError(t, gob.NewDecoder(bytes.NewReader(item.Value)).Decode(&info))
		infos = append(infos, info)
	}
	return infos
}

// An inline delete through the real client to a node that predates scoped
// deletes must not fail the caller and must not fall back to an unscoped
// delete: the shards stay, and a tombstone owes them to the reaper.
func TestAnInlineDeleteToAPriorNodeKeepsTheShardsAndTombstonesThem(t *testing.T) {
	t.Parallel()

	bc, node, prior := blobtest.StartPriorNode(t)
	mc := newFakeMeta()
	ctx := context.Background()
	const key = "prior.txt"
	const epoch uint64 = 4242
	objectHash := model.ObjectHash(deleteTestBucket, key)
	body := []byte("a shard the prior node must keep")

	prior.Seed(t, objectHash, 0, epoch, body)
	prior.Seed(t, objectHash, 1, epoch, body)
	place := ObjectToShardNodes{
		Size:             int64(len(body)),
		WriteEpoch:       epoch,
		DataShardNodes:   []config.NodeID{node},
		ParityShardNodes: []config.NodeID{node},
	}
	record, err := EncodePlacement(place)
	require.NoError(t, err)
	require.NoError(t, metaPut(ctx, mc, model.TableObjects, string(objectHash[:]), record))
	require.NoError(t, metaPut(ctx, mc, model.TableObjects, objectARN(deleteTestBucket, key), objectHash[:]))

	require.NoError(t, deleteStoredObject(ctx, mc, bc, deleteTestBucket, key),
		"a node that rejects the scoped delete must not fail the caller")

	assert.Equal(t, int64(2), prior.RejectedDeleteEpochs(), "setup: each shard's scoped delete must reach the node")
	assert.Zero(t, prior.UnscopedDeletes(), "a rejected scoped delete must not fall back to an unscoped one")
	for index := range uint32(2) {
		held, ok := prior.Holds(t, objectHash, index)
		require.True(t, ok, "shard %d was removed by a delete the node rejected", index)
		assert.Equal(t, epoch, held)
	}

	tombstones := storedTombstones(t, mc)
	require.Len(t, tombstones, 1)
	assert.True(t, tombstones[0].Scoped)
	assert.Equal(t, epoch, tombstones[0].WriteEpoch)
	assert.Equal(t, objectHash, tombstones[0].ObjectHash)
	assert.Equal(t, []config.NodeID{node}, tombstones[0].DataShardNodes)
	assert.Equal(t, []config.NodeID{node}, tombstones[0].ParityNodes)

	_, err = metaGet(ctx, mc, model.TableObjects, string(objectHash[:]))
	assert.Error(t, err, "the placement record must go once the tombstone holds the shards")
	_, err = metaGet(ctx, mc, model.TableObjects, objectARN(deleteTestBucket, key))
	assert.Error(t, err, "the listing row must go")
}
