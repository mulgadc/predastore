package meta

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// commandLog encodes cmd as the committed entry raft hands the FSM.
func commandLog(t *testing.T, index uint64, cmd Command) *raft.Log {
	t.Helper()
	data, err := json.Marshal(cmd)
	require.NoError(t, err)
	return &raft.Log{Index: index, Term: 1, Type: raft.LogCommand, Data: data}
}

// TestFSM_ApplyBatch_RespondsAsSequentialApply pins what one batch returns and
// stores: every Swap reports the value the entries before it in the same batch
// left, deletes and overwrites inside the batch included.
func TestFSM_ApplyBatch_RespondsAsSequentialApply(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	cmds := []Command{
		{Type: CommandPut, Key: []byte("a"), Value: []byte("1")},
		{Type: CommandSwap, Key: []byte("a"), Value: []byte("2")},
		{Type: CommandDelete, Key: []byte("a")},
		{Type: CommandSwap, Key: []byte("a"), Value: []byte("3")},
		{Type: CommandPutMax, Key: []byte("b"), Value: []byte("x"), Epoch: 9},
		{Type: CommandSwap, Key: []byte("b"), Value: []byte("y")},
		{Type: CommandPut, Key: []byte("c"), Value: []byte("1")},
		{Type: CommandPut, Key: []byte("c"), Value: []byte("2")},
		{Type: CommandSwap, Key: []byte("c"), Value: []byte("3")},
	}
	logs := make([]*raft.Log, len(cmds))
	for i, cmd := range cmds {
		logs[i] = commandLog(t, uint64(i+1), cmd)
	}

	got := fsm.ApplyBatch(logs)

	want := []any{nil, []byte("1"), nil, []byte(nil), nil, []byte("x"), nil, nil, []byte("2")}
	require.Len(t, got, len(want))
	for i := range want {
		assert.Equal(t, want[i], got[i], "response %d", i)
	}
	for key, value := range map[string]string{"a": "3", "b": "y", "c": "3"} {
		stored, err := dbGet(db, []byte(key))
		require.NoError(t, err)
		assert.Equal(t, value, string(stored), "key %s", key)
	}
	assert.Equal(t, uint64(len(cmds)), fsm.applied.Load())
}

// TestFSM_ApplyBatch_AnswersEachEntryOnItsOwn covers an entry a batch can
// carry besides commands: a configuration change, which only the batch path is
// sent. It gets its own answer and does not stop the rest of the batch.
func TestFSM_ApplyBatch_AnswersEachEntryOnItsOwn(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	logs := []*raft.Log{
		commandLog(t, 1, Command{Type: CommandPut, Key: []byte("before"), Value: []byte("v")}),
		{Index: 2, Term: 1, Type: raft.LogConfiguration, Data: []byte("not a command")},
		commandLog(t, 3, Command{Type: CommandPut, Key: []byte("after"), Value: []byte("v")}),
	}

	got := fsm.ApplyBatch(logs)

	require.Len(t, got, len(logs))
	assert.Nil(t, got[0])
	assert.Nil(t, got[1], "a configuration entry is not a command")
	assert.Nil(t, got[2])
	for _, key := range []string{"before", "after"} {
		_, err := dbGet(db, []byte(key))
		assert.NoError(t, err, "key %s", key)
	}
	assert.Equal(t, uint64(3), fsm.applied.Load())
}

// TestFSM_ApplyBatch_SplitsATransactionBadgerRefuses fills a batch past what one
// badger transaction may hold, which a run of large values can do, and needs
// every entry stored anyway.
func TestFSM_ApplyBatch_SplitsATransactionBadgerRefuses(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).
		WithLoggingLevel(badger.WARNING).
		WithMemTableSize(1 << 20).
		WithValueThreshold(64 << 10))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	fsm := NewFSM(db)

	value := bytes.Repeat([]byte{0x5a}, 16<<10)
	const n = 64
	logs := make([]*raft.Log, n)
	for i := range logs {
		key := []byte{'k', byte(i)}
		logs[i] = commandLog(t, uint64(i+1), Command{Type: CommandPut, Key: key, Value: value})
	}

	// The batch must actually exceed one transaction, or this proves nothing.
	require.ErrorIs(t, db.Update(func(txn *badger.Txn) error {
		for i := range n {
			if err := txn.Set([]byte{'p', byte(i)}, value); err != nil {
				return err
			}
		}
		return nil
	}), badger.ErrTxnTooBig)

	for i, resp := range fsm.ApplyBatch(logs) {
		assert.Nil(t, resp, "response %d", i)
	}
	for i := range n {
		stored, err := dbGet(db, []byte{'k', byte(i)})
		require.NoError(t, err, "entry %d", i)
		assert.Len(t, stored, len(value))
	}
}

// TestFSM_Apply_IsABatchOfOne keeps the single-entry path answering as it did:
// raft calls it when it does not batch, and a Swap still reports what it
// replaced.
func TestFSM_Apply_IsABatchOfOne(t *testing.T) {
	fsm := NewFSM(newTestDB(t))

	assert.Equal(t, []byte(nil), fsm.Apply(commandLog(t, 1, Command{Type: CommandSwap, Key: []byte("k"), Value: []byte("1")})))
	assert.Equal(t, []byte("1"), fsm.Apply(commandLog(t, 2, Command{Type: CommandSwap, Key: []byte("k"), Value: []byte("2")})))
	assert.Equal(t, uint64(2), fsm.applied.Load())
}

// TestFSM_ApplyBatch_FailedCommitStopsTheReplica pins the fail-stop: a commit
// that fails may have reached badger in part, so the replica must not answer
// the entries or apply more. Restarting restores the snapshot and replays the
// log, which is the recovery.
func TestFSM_ApplyBatch_FailedCommitStopsTheReplica(t *testing.T) {
	fsm := NewFSM(newTestDB(t))
	fsm.commitBatch = func(txn *badger.Txn) error {
		txn.Discard()
		return errors.New("injected commit failure")
	}

	logs := []*raft.Log{
		commandLog(t, 7, Command{Type: CommandPut, Key: []byte("a"), Value: []byte("1")}),
		commandLog(t, 8, Command{Type: CommandSwap, Key: []byte("a"), Value: []byte("2")}),
	}

	assert.PanicsWithValue(t,
		"meta: committing applied entries 7-8 failed; stopping so the log rebuilds the store: injected commit failure",
		func() { fsm.ApplyBatch(logs) })
	assert.Zero(t, fsm.applied.Load(), "a failed batch is not applied")
}

// countCommits counts the transactions fsm commits, through its commit seam.
func countCommits(fsm *FSM) *int {
	n := new(int)
	fsm.commitBatch = func(txn *badger.Txn) error {
		*n++
		return txn.Commit()
	}
	return n
}

// TestFSM_ApplyBatch_CommitsOncePerBatch pins what batching is for: a batch
// that fits one transaction is one synced commit, and one too big for a
// transaction splits into a few commits rather than one per entry.
func TestFSM_ApplyBatch_CommitsOncePerBatch(t *testing.T) {
	fsm := NewFSM(newTestDB(t))
	commits := countCommits(fsm)
	logs := make([]*raft.Log, 64)
	for i := range logs {
		logs[i] = commandLog(t, uint64(i+1), Command{Type: CommandPut, Key: []byte{'k', byte(i)}, Value: []byte("v")})
	}
	fsm.ApplyBatch(logs)
	assert.Equal(t, 1, *commits, "64 small entries")

	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).
		WithLoggingLevel(badger.WARNING).
		WithMemTableSize(1 << 20).
		WithValueThreshold(64 << 10))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	big := NewFSM(db)
	commits = countCommits(big)
	value := bytes.Repeat([]byte{0x5a}, 16<<10)
	for i := range logs {
		logs[i] = commandLog(t, uint64(i+1), Command{Type: CommandPut, Key: []byte{'b', byte(i)}, Value: value})
	}
	big.ApplyBatch(logs)
	assert.Greater(t, *commits, 1, "a batch too big for one transaction splits")
	assert.Less(t, *commits, len(logs)/4, "and splits into a few commits, not one per entry")
}
