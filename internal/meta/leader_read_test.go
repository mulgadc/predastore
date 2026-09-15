package meta_test

import (
	"context"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A leader serves a leader read, including an honest not-found.
func TestLeaderReadIsServedByTheLeader(t *testing.T) {
	cli := startStatusReplica(t, true)
	require.Eventually(t, func() bool {
		st, err := cli.Status(context.Background(), 1)
		return err == nil && st.IsLeader
	}, 5*time.Second, 20*time.Millisecond, "replica never became leader")

	ctx := t.Context()
	_, err := cli.LeaderGet(ctx, "objects/missing")
	require.ErrorIs(t, err, meta.ErrNotFound)

	require.NoError(t, cli.Put(ctx, "objects/a", []byte("one")))
	require.NoError(t, cli.Put(ctx, "objects/b", []byte("two")))

	value, err := cli.LeaderGet(ctx, "objects/a")
	require.NoError(t, err)
	assert.Equal(t, []byte("one"), value)

	items, err := cli.LeaderScanFrom(ctx, "objects/", "objects/a", 10)
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.Equal(t, "objects/b", items[0].Key)
}

// A replica that is not the leader must refuse rather than answer from a local
// store that may not have applied the latest writes.
func TestLeaderReadIsRefusedWithoutALeader(t *testing.T) {
	cli := startStatusReplica(t, false)
	ctx := t.Context()

	_, err := cli.LeaderGet(ctx, "objects/a")
	require.ErrorIs(t, err, meta.ErrNoLeaderRead)
	assert.NotErrorIs(t, err, meta.ErrNotFound, "a follower's not-found must not be trusted")

	_, err = cli.LeaderScanFrom(ctx, "objects/", "", 10)
	require.ErrorIs(t, err, meta.ErrNoLeaderRead)

	// The ordinary read is unchanged: it still answers from the local store.
	_, err = cli.Get(ctx, "objects/a")
	assert.ErrorIs(t, err, meta.ErrNotFound)
}
