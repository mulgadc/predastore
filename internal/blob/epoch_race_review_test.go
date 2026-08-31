package blob_test

import (
	"context"
	"strings"
	"testing"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This is a review-only regression reproducer for two PUTs of the same shard.
// It models the legal ordering where the earlier PUT publishes metadata after
// the later PUT has prepared and committed its epoch.
func TestReviewConcurrentPutCanStrandPublishedEpoch(t *testing.T) {
	c := startBlobNode(t)
	earlier := []byte("earlier PUT")
	later := []byte("later PUT")

	// Both writers complete their prepare phase. The second prepare replaces
	// the node's only prepared pointer for this object and shard index.
	put(t, c, 1, earlier)
	put(t, c, 2, later)

	// Writer 2 publishes epoch 2 and commits it. Writer 1 can then publish
	// epoch 1 with the unconditional metadata Put, but can no longer commit it.
	require.NoError(t, commit(t, c, 2))
	err := commit(t, c, 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), blob.ErrNotPrepared.Error())

	got, err := get(t, c, 2)
	require.NoError(t, err)
	assert.Equal(t, later, got)

	// A GET following the final epoch-1 placement record attempts the read-side
	// commit, but epoch 1 is neither live nor prepared and remains unreadable.
	_, err = get(t, c, 1)
	require.ErrorIs(t, err, blob.ErrEpochMismatch)
	t.Logf("stored epoch 2 remains readable as %q; published epoch 1: commit=%v, get=%v", got, blob.ErrNotPrepared, err)
}

func TestReviewPrepareIsLastArrivalNotHighestEpoch(t *testing.T) {
	c := startBlobNode(t)

	put(t, c, 2, []byte("higher timestamp"))
	_, err := c.Put(context.Background(), epochServerNode, blob.PutRequest{
		Key: epochKey(), Index: epochTestShardIndex, Size: int64(len("lower timestamp")), Epoch: 1,
	}, strings.NewReader("lower timestamp"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no prepared extent")
	require.NoError(t, commit(t, c, 2))
	got, err := get(t, c, 2)
	require.NoError(t, err)
	assert.Equal(t, []byte("higher timestamp"), got)
	t.Logf("lower epoch arriving after higher epoch was rejected; stored value: %q", got)
}
