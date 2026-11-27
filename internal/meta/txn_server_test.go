package meta_test

import (
	"bytes"
	"encoding/json"
	"fmt"
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

func txnBody(t *testing.T, checks []meta.TxnCheck, writes []meta.TxnWrite) []byte {
	t.Helper()
	body, err := json.Marshal(meta.TxnRequest{Checks: checks, Writes: writes})
	require.NoError(t, err)
	return body
}

// paddedTxnBody encodes a valid transaction and pads it with whitespace, which
// JSON ignores, to exactly size bytes, so only its size can refuse it.
func paddedTxnBody(t *testing.T, writes []meta.TxnWrite, size int) []byte {
	t.Helper()
	body := txnBody(t, nil, writes)
	require.Less(t, len(body), size)
	return append(body, bytes.Repeat([]byte{' '}, size-len(body))...)
}

func startTxnLeader(t *testing.T) *meta.Client {
	t.Helper()
	cli := startStatusReplica(t, true, meta.ServeTxn)
	require.Eventually(t, func() bool {
		st, err := cli.Status(t.Context(), 1)
		return err == nil && st.IsLeader
	}, 5*time.Second, 20*time.Millisecond, "replica never became leader")
	return cli
}

// A placement publisher and an abandoned-extent reclaimer need to agree on
// one outcome. This exercises the existing test-only transaction endpoint:
// each operation checks the state the other would change in the same Raft log.
func TestTxnOrdersPlacementAgainstExtentReclamation(t *testing.T) {
	for _, reclaimFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("reclaim_first=%t", reclaimFirst), func(t *testing.T) {
			cli := startTxnLeader(t)
			ctx := t.Context()
			placement := []byte("objects/key")
			fence := []byte("extent-fence/key/epoch-2")
			previous := []byte("epoch-1")
			next := []byte("epoch-2")
			require.NoError(t, cli.Put(ctx, string(placement), previous))

			publish := func() meta.TxnOutcome {
				outcome, _, err := cli.Txn(ctx, []meta.TxnCheck{{Key: fence, Absent: true}},
					[]meta.TxnWrite{{Key: placement, Value: next}})
				require.NoError(t, err)
				return outcome
			}
			reclaim := func() meta.TxnOutcome {
				outcome, _, err := cli.Txn(ctx, []meta.TxnCheck{{Key: placement, Value: previous}},
					[]meta.TxnWrite{{Key: fence, Value: []byte("abandoned")}})
				require.NoError(t, err)
				return outcome
			}

			if reclaimFirst {
				assert.Equal(t, meta.TxnApplied, reclaim())
				assert.Equal(t, meta.TxnMismatch, publish())
				want, err := cli.AuthorityGet(ctx, string(placement))
				require.NoError(t, err)
				assert.Equal(t, previous, want)
				return
			}

			assert.Equal(t, meta.TxnApplied, publish())
			assert.Equal(t, meta.TxnMismatch, reclaim())
			want, err := cli.AuthorityGet(ctx, string(placement))
			require.NoError(t, err)
			assert.Equal(t, next, want)
			_, err = cli.AuthorityGet(ctx, string(fence))
			require.ErrorIs(t, err, meta.ErrNotFound)
		})
	}
}

// A body of exactly the byte limit is served, so the limit refuses only what
// exceeds it.
func TestTxnAtTheByteLimitApplies(t *testing.T) {
	cli := startTxnLeader(t)
	ctx := t.Context()

	resp, err := cli.SendTxnBody(ctx, 1, paddedTxnBody(t,
		[]meta.TxnWrite{{Key: []byte("objects/k"), Value: []byte("gen-1")}}, meta.MaxTxnBodyBytes))

	require.NoError(t, err)
	assert.Empty(t, resp.Err)
	value, err := cli.LeaderGet(ctx, "objects/k")
	require.NoError(t, err)
	assert.Equal(t, []byte("gen-1"), value)
}

// A transaction over a limit is refused before it is proposed: the replica
// answers invalid, the log does not grow, and the key it writes stays absent.
func TestTxnOverALimitIsRefusedWithoutALogAppend(t *testing.T) {
	write := []meta.TxnWrite{{Key: []byte("objects/k"), Value: []byte("gen-1")}}
	manyChecks := make([]meta.TxnCheck, meta.MaxTxnChecks+1)
	for i := range manyChecks {
		manyChecks[i] = meta.TxnCheck{Key: fmt.Appendf(nil, "objects/c%d", i), Absent: true}
	}
	manyWrites := append([]meta.TxnWrite(nil), write...)
	for i := range meta.MaxTxnWrites {
		manyWrites = append(manyWrites, meta.TxnWrite{Key: fmt.Appendf(nil, "objects/w%d", i), Value: []byte("v")})
	}
	cases := map[string][]byte{
		"oversized body":  paddedTxnBody(t, write, meta.MaxTxnBodyBytes+1),
		"too many checks": txnBody(t, manyChecks, write),
		"too many writes": txnBody(t, nil, manyWrites),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			cli := startTxnLeader(t)
			ctx := t.Context()
			// A leader read waits out the new term's first entry, so the log is still.
			_, err := cli.LeaderGet(ctx, "objects/k")
			require.ErrorIs(t, err, meta.ErrNotFound)
			before, err := cli.Status(ctx, 1)
			require.NoError(t, err)

			resp, err := cli.SendTxnBody(ctx, 1, body)

			require.NoError(t, err)
			assert.Equal(t, meta.ErrCodeInvalid, resp.Err)
			after, err := cli.Status(ctx, 1)
			require.NoError(t, err)
			assert.Equal(t, before.CommitIndex, after.CommitIndex, "the refused transaction reached the log")
			assert.Equal(t, before.AppliedIndex, after.AppliedIndex, "the refused transaction was applied")
			_, err = cli.LeaderGet(ctx, "objects/k")
			assert.ErrorIs(t, err, meta.ErrNotFound)
		})
	}
}

// The client refuses an oversized transaction itself, so no replica sees it.
func TestClientTxn_AnOversizedTransactionIsRefusedBeforeSending(t *testing.T) {
	cli := startStatusReplica(t, true, meta.ServeTxn)

	outcome, _, err := cli.Txn(t.Context(), nil, []meta.TxnWrite{
		{Key: []byte("objects/k"), Value: bytes.Repeat([]byte{'v'}, meta.MaxTxnBodyBytes)},
	})

	assert.Equal(t, meta.TxnRefused, outcome)
	require.ErrorIs(t, err, meta.ErrTxnRefused)
	require.ErrorIs(t, err, meta.ErrInvalidTxn)
}

// A mismatch reply carries no value larger than the body limit, so a check
// that names a large row cannot make the replica send it.
func TestTxnMismatchOnALargeRowCarriesNoValue(t *testing.T) {
	cli := startTxnLeader(t)
	ctx := t.Context()
	require.NoError(t, cli.Put(ctx, "objects/big", bytes.Repeat([]byte{'v'}, meta.MaxTxnBodyBytes+1)))

	resp, err := cli.SendTxnBody(ctx, 1, txnBody(t,
		[]meta.TxnCheck{{Key: []byte("objects/big"), Value: []byte("v")}},
		[]meta.TxnWrite{{Key: []byte("objects/k"), Value: []byte("gen-1")}}))

	require.NoError(t, err)
	assert.Empty(t, resp.Err)
	assert.True(t, resp.Mismatch)
	assert.Equal(t, []meta.TxnValue{{Key: []byte("objects/big"), Exists: true, Oversized: true}}, resp.Current)
}
