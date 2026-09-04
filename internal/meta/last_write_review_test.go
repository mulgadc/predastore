package meta_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Placement publication is an unconditional Put, and a lower epoch arriving
// second is meant to win. A PUT is not acknowledged until its record reaches
// the log, so arrival order is acknowledgement order, and the write that
// finished last is the one S3 hands the key to. Ordering by the epoch instead
// picks whichever write *started* last, which is a different write whenever
// two PUTs overlap.
func TestPlacementPublicationIsLastWriteWins(t *testing.T) {
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
	assert.Equal(t, placementRecord(1), got,
		"the record that arrived second must win: its writer is the one still "+
			"waiting to be acknowledged, so it is the write that finished last")
}

func placementRecord(epoch byte) []byte {
	// Minimal valid placement header: magic, version, k, size, epoch.
	b := make([]byte, 27)
	b[0], b[1], b[2] = 0, 2, 1
	b[18] = epoch
	return b
}
