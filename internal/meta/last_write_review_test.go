package meta_test

import (
	"context"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReviewDelayedPlacementPutWins pins the fence's outcome against a real
// placement record: a delayed older write must not overwrite a newer,
// acknowledged one, even though PutMax only performs a plain Set once its
// epoch comparison lets the write through.
func TestReviewDelayedPlacementPutWins(t *testing.T) {
	cli := startStatusReplica(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.Eventually(t, func() bool {
		status, err := cli.Status(ctx, 1)
		return err == nil && status.IsLeader
	}, 5*time.Second, 20*time.Millisecond)

	newer := placementRecord(t, 2)
	older := placementRecord(t, 1)

	require.NoError(t, cli.PutMax(ctx, "objects/same-object", newer, 2))
	require.NoError(t, cli.PutMax(ctx, "objects/same-object", older, 1))

	got, err := cli.Get(ctx, "objects/same-object")
	require.NoError(t, err)
	assert.Equal(t, newer, got)
	t.Logf("final placement after epoch-2 then delayed epoch-1: %q", got)
}

// placementRecord encodes a real placement record through the same encoder
// the gate uses, so this fixture tracks the current on-disk format instead of
// hand-building bytes that can silently drift from it.
func placementRecord(t *testing.T, epoch uint64) []byte {
	t.Helper()
	b, err := handlers.EncodePlacement(handlers.ObjectToShardNodes{
		Size:           4096,
		WriteEpoch:     epoch,
		DataShardNodes: []config.NodeID{1},
	})
	require.NoError(t, err)
	return b
}
