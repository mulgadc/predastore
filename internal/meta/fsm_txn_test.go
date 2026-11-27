package meta

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// oversizedKey is a key badger refuses to stage, so a write to it fails after
// the writes before it in the same transaction have been staged.
var oversizedKey = bytes.Repeat([]byte{'k'}, 70_000)

func txnCommand(checks []TxnCheck, writes ...TxnWrite) Command {
	return Command{Type: CommandTxn, Checks: checks, Writes: writes}
}

func put(key, value string) TxnWrite { return TxnWrite{Key: []byte(key), Value: []byte(value)} }

func holds(key, value string) TxnCheck { return TxnCheck{Key: []byte(key), Value: []byte(value)} }

func absent(key string) TxnCheck { return TxnCheck{Key: []byte(key), Absent: true} }

// assertStored checks every key: a nil want means the key must be absent.
func assertStored(t *testing.T, db *badger.DB, want map[string]*string) {
	t.Helper()
	for key, value := range want {
		stored, err := dbGet(db, []byte(key))
		if value == nil {
			assert.ErrorIs(t, err, badger.ErrKeyNotFound, "key %s must be absent", key)
			continue
		}
		require.NoError(t, err, "key %s", key)
		assert.Equal(t, *value, string(stored), "key %s", key)
	}
}

func str(s string) *string { return &s }

func TestFSM_Txn_AppliesEveryWriteWhenEveryCheckHolds(t *testing.T) {
	db := newTestDB(t)
	dbSet(t, db, []byte("a"), []byte("1"))
	dbSet(t, db, []byte("c"), []byte("old"))
	dbSet(t, db, []byte("e"), nil)
	fsm := NewFSM(db)

	got := fsm.Apply(commandLog(t, 1, txnCommand(
		[]TxnCheck{holds("a", "1"), absent("b"), {Key: []byte("e")}},
		put("a", "2"), put("b", "new"), TxnWrite{Key: []byte("c"), Delete: true},
	)))

	assert.Equal(t, TxnResult{Applied: true}, got)
	assertStored(t, db, map[string]*string{"a": str("2"), "b": str("new"), "c": nil})
}

func TestFSM_Txn_AFailedCheckPublishesNothingAndReportsCurrentValues(t *testing.T) {
	db := newTestDB(t)
	dbSet(t, db, []byte("a"), []byte("1"))
	dbSet(t, db, []byte("b"), []byte("taken"))
	fsm := NewFSM(db)

	// The first check holds and the next two fail, one on a present key and
	// one on an absent key, so Current must report both kinds.
	got := fsm.Apply(commandLog(t, 1, txnCommand(
		[]TxnCheck{holds("a", "1"), absent("b"), holds("missing", "x")},
		put("a", "2"), put("d", "new"),
	)))

	assert.Equal(t, TxnResult{Current: []TxnValue{
		{Key: []byte("a"), Value: []byte("1"), Exists: true},
		{Key: []byte("b"), Value: []byte("taken"), Exists: true},
		{Key: []byte("missing")},
	}}, got)
	assertStored(t, db, map[string]*string{"a": str("1"), "b": str("taken"), "d": nil})
}

func TestFSM_Txn_AStagingFailureAfterAStagedWritePublishesNothing(t *testing.T) {
	db := newTestDB(t)
	dbSet(t, db, []byte("a"), []byte("1"))
	fsm := NewFSM(db)

	got := fsm.Apply(commandLog(t, 1, txnCommand(
		[]TxnCheck{holds("a", "1")},
		put("a", "2"), put("b", "new"), TxnWrite{Key: oversizedKey, Value: []byte("v")},
	)))

	err, ok := got.(error)
	require.True(t, ok, "a staging failure answers an error, got %#v", got)
	assert.ErrorContains(t, err, "not applied")
	assertStored(t, db, map[string]*string{"a": str("1"), "b": nil})
	assert.Equal(t, uint64(1), fsm.applied.Load(), "the entry is still applied, with no effect")
}

func TestFSM_Txn_AMalformedTransactionIsRejectedWithoutEffect(t *testing.T) {
	cases := map[string]Command{
		"no writes":           txnCommand([]TxnCheck{absent("a")}),
		"check without key":   txnCommand([]TxnCheck{{Value: []byte("x")}}, put("a", "1")),
		"absent with a value": txnCommand([]TxnCheck{{Key: []byte("b"), Value: []byte("x"), Absent: true}}, put("a", "1")),
		"write without key":   txnCommand(nil, put("a", "1"), TxnWrite{Value: []byte("x")}),
		"delete with a value": txnCommand(nil, put("a", "1"), TxnWrite{Key: []byte("b"), Value: []byte("x"), Delete: true}),
		"too many checks":     txnCommand(absentChecks(MaxTxnChecks+1), put("a", "1")),
		"too many writes":     txnCommand(nil, puts(MaxTxnWrites+1)...),
	}
	for name, cmd := range cases {
		t.Run(name, func(t *testing.T) {
			db := newTestDB(t)
			fsm := NewFSM(db)

			got := fsm.Apply(commandLog(t, 1, cmd))

			err, ok := got.(error)
			require.True(t, ok, "got %#v", got)
			assert.ErrorIs(t, err, ErrInvalidTxn)
			assertStored(t, db, map[string]*string{"a": nil, "b": nil})
		})
	}
}

func absentChecks(n int) []TxnCheck {
	checks := make([]TxnCheck, n)
	for i := range checks {
		checks[i] = absent(fmt.Sprintf("check/%d", i))
	}
	return checks
}

// puts writes "a" first, so a refused transaction leaves "a" absent.
func puts(n int) []TxnWrite {
	writes := make([]TxnWrite, n)
	for i := range writes {
		writes[i] = put(fmt.Sprintf("write/%d", i), "v")
	}
	writes[0] = put("a", "1")
	return writes
}

// The limits refuse only what exceeds them: a transaction at both limits applies.
func TestFSM_Txn_ATransactionAtTheLimitsApplies(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	got := fsm.Apply(commandLog(t, 1, txnCommand(absentChecks(MaxTxnChecks), puts(MaxTxnWrites)...)))

	assert.Equal(t, TxnResult{Applied: true}, got)
	assertStored(t, db, map[string]*string{"a": str("1"), fmt.Sprintf("write/%d", MaxTxnWrites-1): str("v")})
}

// The entries before a transaction are visible to its checks, and every
// ordinary entry around a failed transaction still commits.
func TestFSM_Txn_InABatchWithOrdinaryEntries(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	cmds := []Command{
		{Type: CommandPut, Key: []byte("x"), Value: []byte("1")},
		// Holds only because the put before it in this batch is visible.
		txnCommand([]TxnCheck{holds("x", "1"), absent("y")}, put("y", "1"), put("x", "2")),
		{Type: CommandSwap, Key: []byte("x"), Value: []byte("3")},
		// Fails: the transaction before it wrote y.
		txnCommand([]TxnCheck{absent("y")}, put("w", "1")),
		{Type: CommandPut, Key: []byte("p"), Value: []byte("1")},
		// Fails in staging after it has staged q.
		txnCommand(nil, put("q", "1"), TxnWrite{Key: oversizedKey, Value: []byte("v")}),
		{Type: CommandPut, Key: []byte("z"), Value: []byte("1")},
	}
	logs := make([]*raft.Log, len(cmds))
	for i, cmd := range cmds {
		logs[i] = commandLog(t, uint64(i+1), cmd)
	}

	got := fsm.ApplyBatch(logs)

	require.Len(t, got, len(cmds))
	assert.Nil(t, got[0])
	assert.Equal(t, TxnResult{Applied: true}, got[1])
	assert.Equal(t, []byte("2"), got[2], "the swap sees the transaction's write")
	assert.Equal(t, TxnResult{Current: []TxnValue{{Key: []byte("y"), Value: []byte("1"), Exists: true}}}, got[3])
	assert.Nil(t, got[4])
	err, ok := got[5].(error)
	require.True(t, ok, "got %#v", got[5])
	assert.ErrorContains(t, err, "not applied")
	assert.Nil(t, got[6])
	assertStored(t, db, map[string]*string{
		"x": str("3"), "y": str("1"), "w": nil, "p": str("1"), "q": nil, "z": str("1"),
	})
}

// A transaction that commits alone still takes the fail-stop path when its
// commit fails, because badger may have applied part of it.
func TestFSM_Txn_AFailedCommitStopsTheReplica(t *testing.T) {
	fsm := NewFSM(newTestDB(t))
	fsm.commitBatch = func(txn *badger.Txn) error {
		txn.Discard()
		return errors.New("injected commit failure")
	}

	assert.Panics(t, func() {
		fsm.Apply(commandLog(t, 3, txnCommand([]TxnCheck{absent("a")}, put("a", "1"))))
	})
	assert.Zero(t, fsm.applied.Load())
}

// dumpStore returns every key and value in the store, in key order.
func dumpStore(t *testing.T, db *badger.DB) []string {
	t.Helper()
	var rows []string
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			value, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			rows = append(rows, fmt.Sprintf("%q=%q", it.Item().Key(), value))
		}
		return nil
	}))
	return rows
}

// Three replicas apply the same log, split into batches differently as raft
// may hand it to each, and reach the same answers and the same store.
func TestFSM_Txn_ReplicasApplyingTheSameLogAgree(t *testing.T) {
	cmds := []Command{
		{Type: CommandPut, Key: []byte("objects/a"), Value: []byte("gen-1")},
		txnCommand([]TxnCheck{absent("objects/b")}, put("objects/b", "gen-1"), put("intent/b", "committed")),
		txnCommand([]TxnCheck{absent("objects/b")}, put("objects/b", "gen-2")),
		{Type: CommandSwap, Key: []byte("objects/a"), Value: []byte("gen-2")},
		txnCommand([]TxnCheck{holds("objects/a", "gen-1")}, put("objects/a", "gen-3")),
		txnCommand([]TxnCheck{holds("objects/a", "gen-2")}, put("objects/a", "gen-3"), TxnWrite{Key: []byte("intent/b"), Delete: true}),
		txnCommand(nil, put("objects/c", "x"), TxnWrite{Key: oversizedKey, Value: []byte("v")}),
		{Type: CommandDelete, Key: []byte("objects/b")},
		txnCommand([]TxnCheck{absent("objects/b")}, put("objects/b", "gen-3")),
	}
	logs := make([]*raft.Log, len(cmds))
	for i, cmd := range cmds {
		logs[i] = commandLog(t, uint64(i+1), cmd)
	}

	type replica struct {
		answers []string
		kinds   []string
		store   []string
	}
	run := func(batch int) replica {
		db := newTestDB(t)
		fsm := NewFSM(db)
		var r replica
		for start := 0; start < len(logs); start += batch {
			end := min(start+batch, len(logs))
			for _, resp := range fsm.ApplyBatch(logs[start:end]) {
				answer, kind := fmt.Sprintf("%#v", resp), "applied"
				switch v := resp.(type) {
				case error:
					answer, kind = "error: "+v.Error(), "error"
				case TxnResult:
					if !v.Applied {
						kind = "mismatch"
					}
				}
				r.answers = append(r.answers, answer)
				r.kinds = append(r.kinds, kind)
			}
		}
		r.store = dumpStore(t, db)
		return r
	}

	one, all, threes := run(1), run(len(logs)), run(3)

	assert.Equal(t, one, all)
	assert.Equal(t, one, threes)
	assert.Equal(t, []string{"applied", "applied", "mismatch", "applied", "mismatch", "applied", "error", "applied", "applied"},
		one.kinds)
	assert.Equal(t, []string{`"objects/a"="gen-3"`, `"objects/b"="gen-3"`}, one.store)
}
