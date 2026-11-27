package meta_test

import (
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Through a real replica, a transaction applies once, the same transaction
// then mismatches with the value the first one wrote, and the store keeps
// the first transaction's writes only.
func TestTxnThroughTheLeader(t *testing.T) {
	cli := startStatusReplica(t, true, meta.ServeTxn)
	ctx := t.Context()
	require.Eventually(t, func() bool {
		st, err := cli.Status(ctx, 1)
		return err == nil && st.IsLeader
	}, 5*time.Second, 20*time.Millisecond, "replica never became leader")

	checks := []meta.TxnCheck{{Key: []byte("objects/k"), Absent: true}}
	first := []meta.TxnWrite{
		{Key: []byte("objects/k"), Value: []byte("gen-1")},
		{Key: []byte("intents/k"), Value: []byte("committed")},
	}
	outcome, current, err := cli.Txn(ctx, checks, first)
	require.NoError(t, err)
	assert.Equal(t, meta.TxnApplied, outcome)
	assert.Nil(t, current)

	second := []meta.TxnWrite{
		{Key: []byte("objects/k"), Value: []byte("gen-2")},
		{Key: []byte("intents/k"), Delete: true},
	}
	outcome, current, err = cli.Txn(ctx, checks, second)
	require.NoError(t, err)
	assert.Equal(t, meta.TxnMismatch, outcome)
	assert.Equal(t, []meta.TxnValue{{Key: []byte("objects/k"), Value: []byte("gen-1"), Exists: true}}, current)

	value, err := cli.LeaderGet(ctx, "objects/k")
	require.NoError(t, err)
	assert.Equal(t, []byte("gen-1"), value)
	value, err = cli.LeaderGet(ctx, "intents/k")
	require.NoError(t, err)
	assert.Equal(t, []byte("committed"), value)
}

// A replica with no leader refuses before it proposes anything.
func TestTxnIsRefusedWithoutALeader(t *testing.T) {
	cli := startStatusReplica(t, false, meta.ServeTxn)

	outcome, _, err := cli.Txn(t.Context(),
		[]meta.TxnCheck{{Key: []byte("objects/k"), Absent: true}},
		[]meta.TxnWrite{{Key: []byte("objects/k"), Value: []byte("gen-1")}})

	assert.Equal(t, meta.TxnRefused, outcome)
	require.ErrorIs(t, err, meta.ErrTxnRefused)
	require.ErrorIs(t, err, meta.ErrNotLeader)

	_, err = cli.Get(t.Context(), "objects/k")
	assert.ErrorIs(t, err, meta.ErrNotFound)
}

// A production replica does not serve OpMetaTxn, so no request can append a
// CommandTxn: the log does not grow and the key stays absent.
func TestAProductionReplicaRefusesTxn(t *testing.T) {
	cli := startStatusReplica(t, true)
	ctx := t.Context()
	require.Eventually(t, func() bool {
		st, err := cli.Status(ctx, 1)
		return err == nil && st.IsLeader
	}, 5*time.Second, 20*time.Millisecond, "replica never became leader")
	// A leader read waits out the new term's first entry, so the log is still.
	_, err := cli.LeaderGet(ctx, "objects/k")
	require.ErrorIs(t, err, meta.ErrNotFound)
	before, err := cli.Status(ctx, 1)
	require.NoError(t, err)

	outcome, _, err := cli.Txn(ctx,
		[]meta.TxnCheck{{Key: []byte("objects/k"), Absent: true}},
		[]meta.TxnWrite{{Key: []byte("objects/k"), Value: []byte("gen-1")}})

	require.Error(t, err)
	assert.NotEqual(t, meta.TxnApplied, outcome)
	assert.NotEqual(t, meta.TxnMismatch, outcome)
	after, err := cli.Status(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, before.CommitIndex, after.CommitIndex, "the refused transaction reached the log")
	_, err = cli.LeaderGet(ctx, "objects/k")
	assert.ErrorIs(t, err, meta.ErrNotFound)
}
