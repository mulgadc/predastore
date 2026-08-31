package meta_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Review-only evidence that placement publication is an unconditional Put:
// a delayed earlier writer can replace a newer epoch after it has committed.
func TestReviewDelayedPlacementPutWins(t *testing.T) {
	cli := startStatusReplica(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.Eventually(t, func() bool {
		status, err := cli.Status(ctx, 1)
		return err == nil && status.IsLeader
	}, 5*time.Second, 20*time.Millisecond)

	require.NoError(t, cli.PutMax(ctx, "objects/same-object", placementRecord(2), 2))
	require.NoError(t, cli.PutMax(ctx, "objects/same-object", placementRecord(1), 1))

	got, err := cli.Get(ctx, "objects/same-object")
	require.NoError(t, err)
	assert.Equal(t, placementRecord(2), got)
	t.Logf("final placement after epoch-2 then delayed epoch-1: %q", got)
}

func placementRecord(epoch byte) []byte {
	// Minimal valid placement header: magic, version, k, size, epoch.
	b := make([]byte, 27)
	b[0], b[1], b[2] = 0, 2, 1
	b[18] = epoch
	return b
}
