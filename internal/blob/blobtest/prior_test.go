package blobtest_test

import (
	"context"
	"testing"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/blob/blobtest"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Before its upgrade the node resets a scoped delete with no envelope and keeps
// the shard; after it, the same delete removes the shard.
func TestPriorNodeRejectsDeleteEpochUntilUpgraded(t *testing.T) {
	t.Parallel()

	c, node, prior := blobtest.StartPriorNode(t)
	ctx := context.Background()
	key := [32]byte{0x7a}
	prior.Seed(t, key, 0, 11, []byte("kept until the upgrade"))
	req := blob.CommitRequest{Key: key, Index: 0, Epoch: 11}

	resp, err := c.DeleteEpoch(ctx, node, req)
	require.Error(t, err)
	assert.Nil(t, resp)
	var se *transport.StreamError
	assert.ErrorAs(t, err, &se, "the stream must be reset, not answered")
	assert.Equal(t, int64(1), prior.RejectedDeleteEpochs())
	epoch, ok := prior.Holds(t, key, 0)
	require.True(t, ok, "a rejected scoped delete must leave the shard")
	assert.Equal(t, uint64(11), epoch)

	prior.Upgrade()
	resp, err = c.DeleteEpoch(ctx, node, req)
	require.NoError(t, err)
	assert.True(t, resp.Deleted)
	_, ok = prior.Holds(t, key, 0)
	assert.False(t, ok)
	assert.Equal(t, int64(1), prior.RejectedDeleteEpochs())
	assert.Zero(t, prior.UnscopedDeletes())
}

// The node counts each unscoped delete it serves, so a caller that falls back
// to one is visible.
func TestPriorNodeCountsUnscopedDeletes(t *testing.T) {
	t.Parallel()

	c, node, prior := blobtest.StartPriorNode(t)
	key := [32]byte{0x7b}
	prior.Seed(t, key, 0, 12, []byte("removed by an unscoped delete"))

	resp, err := c.Delete(context.Background(), node, blob.DeleteRequest{Key: key, Index: 0})
	require.NoError(t, err)
	assert.True(t, resp.Deleted)
	assert.Equal(t, int64(1), prior.UnscopedDeletes())
	_, ok := prior.Holds(t, key, 0)
	assert.False(t, ok)
}
