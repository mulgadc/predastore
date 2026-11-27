package meta

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// smallTxnOptions caps a badger transaction at about 150 KiB, so a test can
// fill one with a few entries.
func smallTxnOptions(dir string) badger.Options {
	return badger.DefaultOptions(dir).
		WithLoggingLevel(badger.WARNING).
		WithMemTableSize(1 << 20).
		WithValueThreshold(64 << 10)
}

// openStore opens the badger store in dir and closes it when the test ends,
// unless the test closed it first to restart on it.
func openStore(t *testing.T, opts badger.Options) *badger.DB {
	t.Helper()
	db, err := badger.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		if !db.IsClosed() {
			db.Close()
		}
	})
	return db
}

// restart closes db and opens the same directory again, as a replica that
// stops and starts on its store does.
func restart(t *testing.T, db *badger.DB, opts badger.Options) (*badger.DB, *FSM) {
	t.Helper()
	require.NoError(t, db.Close())
	db = openStore(t, opts)
	fsm, err := OpenFSM(db)
	require.NoError(t, err)
	return db, fsm
}

func storedApplied(t *testing.T, db *badger.DB) uint64 {
	t.Helper()
	index, err := readApplied(db)
	require.NoError(t, err)
	return index
}

func swapLog(t *testing.T, index uint64, key, value string) *raft.Log {
	return commandLog(t, index, Command{Type: CommandSwap, Key: []byte(key), Value: []byte(value)})
}

// A replica with no snapshot replays its whole log against the store it kept.
// The conditional create must not be evaluated again: it would find its own
// placement and fail, and the intent put before it would stay pending.
func TestFSM_Replay_AConditionalCreateKeepsItsResult(t *testing.T) {
	opts := badger.DefaultOptions(t.TempDir()).WithLoggingLevel(badger.WARNING)
	db := openStore(t, opts)
	fsm := NewFSM(db)
	logs := []*raft.Log{
		commandLog(t, 1, Command{Type: CommandPut, Key: []byte("intents/k"), Value: []byte("pending")}),
		commandLog(t, 2, txnCommand(
			[]TxnCheck{absent("objects/k"), holds("intents/k", "pending")},
			put("objects/k", "gen-1"), put("intents/k", "committed"))),
	}
	for _, log := range logs {
		fsm.Apply(log)
	}
	once := dumpStore(t, db)

	db, fsm = restart(t, db, opts)
	next := swapLog(t, 3, "objects/k", "gen-2")
	replayed := fsm.ApplyBatch(append(logs, next))

	assert.Equal(t, []any{nil, nil, []byte("gen-1")}, replayed,
		"the entries in the store are skipped, and the entry after them applies")
	assertStored(t, db, map[string]*string{"intents/k": str("committed"), "objects/k": str("gen-2")})

	// The store equals one that applied the log once.
	ref := newTestDB(t)
	refFSM := NewFSM(ref)
	for _, log := range logs {
		refFSM.Apply(log)
	}
	assert.Equal(t, once, dumpStore(t, ref))
	refFSM.Apply(next)
	assert.Equal(t, dumpStore(t, ref), dumpStore(t, db))
}

// A replayed swap is not evaluated again either, so it does not read and
// report a value that a later entry wrote.
func TestFSM_Replay_ASwapIsNotEvaluatedAgain(t *testing.T) {
	opts := badger.DefaultOptions(t.TempDir()).WithLoggingLevel(badger.WARNING)
	db := openStore(t, opts)
	fsm := NewFSM(db)
	logs := []*raft.Log{swapLog(t, 1, "objects/k", "gen-1"), swapLog(t, 2, "objects/k", "gen-2")}
	require.Equal(t, []any{[]byte(nil), []byte("gen-1")}, fsm.ApplyBatch(logs))

	db, fsm = restart(t, db, opts)
	replayed := fsm.ApplyBatch(append(logs, swapLog(t, 3, "objects/k", "gen-3")))

	assert.Equal(t, []any{nil, nil, []byte("gen-2")}, replayed)
	assertStored(t, db, map[string]*string{"objects/k": str("gen-3")})
	assert.Equal(t, uint64(3), storedApplied(t, db))
}

// A store from a release before the row exists has no row. Every entry is
// applied against it, and the first commit writes the row.
func TestFSM_Replay_AStoreWithoutTheRowAppliesEveryEntryThenHoldsIt(t *testing.T) {
	db := newTestDB(t)
	dbSet(t, db, []byte("objects/k"), []byte("old"))
	fsm, err := OpenFSM(db)
	require.NoError(t, err)

	got := fsm.ApplyBatch([]*raft.Log{swapLog(t, 1, "objects/k", "gen-1"), swapLog(t, 2, "objects/k", "gen-2")})

	assert.Equal(t, []any{[]byte("old"), []byte("gen-1")}, got)
	assert.Equal(t, uint64(2), storedApplied(t, db))
}

// The row names the last entry that wrote. Entries after it that wrote
// nothing are evaluated again on replay, against the same state.
func TestFSM_ApplyBatch_TheRowNamesTheLastEntryThatWrote(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	fsm.ApplyBatch([]*raft.Log{
		swapLog(t, 1, "objects/k", "gen-1"),
		commandLog(t, 2, txnCommand([]TxnCheck{absent("objects/k")}, put("objects/k", "gen-2"))),
		{Index: 3, Term: 1, Type: raft.LogConfiguration},
	})

	assert.Equal(t, uint64(1), storedApplied(t, db))
	assert.Equal(t, uint64(1), fsm.persisted)
	assert.Equal(t, uint64(3), fsm.applied.Load())
}

// A row that does not decode is not trusted as an index: the replica does
// not start, and so skips nothing.
func TestFSM_Open_RefusesAnUndecodableRow(t *testing.T) {
	valid := encodeApplied(7)
	cases := map[string][]byte{
		"a value written by a client": []byte("pending"),
		"empty":                       {},
		"another magic":               append([]byte("XXXX"), valid[4:]...),
		"another version":             append(append(bytes.Clone(valid[:4]), 2), valid[5:]...),
		"truncated":                   valid[:len(valid)-1],
	}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			db := newTestDB(t)
			dbSet(t, db, appliedKey, value)

			_, err := OpenFSM(db)

			require.Error(t, err)
			assert.ErrorContains(t, err, "applied index")
			assert.Panics(t, func() { NewFSM(db) })
		})
	}

	db := newTestDB(t)
	dbSet(t, db, appliedKey, valid)
	fsm, err := OpenFSM(db)
	require.NoError(t, err)
	assert.Equal(t, uint64(7), fsm.persisted)
}

// No command writes a reserved key, and no read or scan returns one.
func TestFSM_ReservedKeysAreRefusedAndHidden(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)
	fsm.Apply(swapLog(t, 1, "objects/k", "gen-1"))
	row := encodeApplied(1)

	for i, key := range [][]byte{appliedKey, []byte("\x00raft/other")} {
		base := uint64(2 + 10*i)
		for j, cmd := range []Command{
			{Type: CommandPut, Key: key, Value: []byte("v")},
			{Type: CommandPutMax, Key: key, Value: []byte("v"), Epoch: 1},
			{Type: CommandSwap, Key: key, Value: []byte("v")},
			{Type: CommandDelete, Key: key},
		} {
			got := fsm.Apply(commandLog(t, base+uint64(j), cmd))
			err, ok := got.(error)
			require.True(t, ok, "command %d on %q answered %#v", cmd.Type, key, got)
			assert.ErrorIs(t, err, ErrReservedKey)
		}
		for j, cmd := range []Command{
			txnCommand(nil, put("objects/x", "v"), TxnWrite{Key: key, Value: []byte("v")}),
			txnCommand([]TxnCheck{{Key: key, Value: row}}, put("objects/x", "v")),
		} {
			got := fsm.Apply(commandLog(t, base+4+uint64(j), cmd))
			err, ok := got.(error)
			require.True(t, ok, "transaction %d on %q answered %#v", j, key, got)
			assert.ErrorIs(t, err, ErrReservedKey)
			assert.ErrorIs(t, err, ErrInvalidTxn)
		}
	}
	stored, err := dbGet(db, appliedKey)
	require.NoError(t, err)
	assert.Equal(t, row, stored, "a refused write leaves the row as it was")
	assertStored(t, db, map[string]*string{"\x00raft/other": nil, "objects/x": nil})

	_, err = fsm.Get(string(appliedKey))
	assert.ErrorIs(t, err, badger.ErrKeyNotFound)
	for prefix, want := range map[string][]string{"": {"objects/k"}, "\x00": nil, "\x00raft/": nil} {
		var keys []string
		require.NoError(t, fsm.Scan(prefix, func(key string, _ []byte) error {
			keys = append(keys, key)
			return nil
		}))
		assert.Equal(t, want, keys, "prefix %q", prefix)
	}
}

// The shared transaction keeps room for the row. An entry that badger would
// take but that leaves no room for the row goes to the next commit, and each
// commit names its own last entry.
func TestFSM_ApplyBatch_KeepsRoomForTheAppliedRow(t *testing.T) {
	db := openStore(t, smallTxnOptions(t.TempDir()))
	maxSize := db.MaxBatchSize()

	filler := bytes.Repeat([]byte{0x5a}, 16<<10)
	fillerKey := func(i int) []byte { return fmt.Appendf(nil, "f%03d", i) }
	limits := newTxnLimits(db)
	fillerCost := limits.cost(fillerKey(0), len(filler))
	fillers := int((maxSize-txnBaseSize)/fillerCost) - 1
	remaining := maxSize - txnBaseSize - int64(fillers)*fillerCost
	lastKey := []byte("last")
	// Badger takes the last entry with 11 bytes to spare, and the row needs more.
	last := bytes.Repeat([]byte{0x6b}, int(remaining-11)-len(lastKey)-12)

	// The premise: in one transaction, badger takes every entry and then
	// refuses the row.
	premise := db.NewTransaction(true)
	for i := range fillers {
		require.NoError(t, premise.Set(fillerKey(i), filler))
	}
	require.NoError(t, premise.Set(lastKey, last))
	require.ErrorIs(t, premise.Set(appliedKey, encodeApplied(1)), badger.ErrTxnTooBig)
	premise.Discard()

	fsm := NewFSM(db)
	var rows []uint64
	fsm.commitBatch = func(txn *badger.Txn) error {
		item, err := txn.Get(appliedKey)
		require.NoError(t, err)
		value, err := item.ValueCopy(nil)
		require.NoError(t, err)
		index, err := decodeApplied(value)
		require.NoError(t, err)
		rows = append(rows, index)
		return txn.Commit()
	}
	logs := make([]*raft.Log, 0, fillers+1)
	for i := range fillers {
		logs = append(logs, commandLog(t, uint64(i+1), Command{Type: CommandPut, Key: fillerKey(i), Value: filler}))
	}
	logs = append(logs, commandLog(t, uint64(fillers+1), Command{Type: CommandPut, Key: lastKey, Value: last}))

	for i, resp := range fsm.ApplyBatch(logs) {
		assert.Nil(t, resp, "entry %d", i+1)
	}

	assert.Equal(t, []uint64{uint64(fillers), uint64(fillers + 1)}, rows)
	stored, err := dbGet(db, lastKey)
	require.NoError(t, err)
	assert.Len(t, stored, len(last))
}

// A batch too big for one transaction commits in parts. A replica that stops
// after the first commit, before the second, holds the row of the first, and
// replay applies each entry that the first commit left out exactly once.
func TestFSM_ApplyBatch_AStopBetweenSplitCommitsReplaysTheRestOnce(t *testing.T) {
	opts := smallTxnOptions(t.TempDir())
	db := openStore(t, opts)
	fsm := NewFSM(db)

	const n = 32
	value := func(i int) string { return string(bytes.Repeat([]byte{byte('a' + i%26), byte(i)}, 8<<10)) }
	logs := make([]*raft.Log, n)
	for i := range logs {
		logs[i] = swapLog(t, uint64(i+1), "objects/k", value(i+1))
	}

	commits := 0
	fsm.commitBatch = func(txn *badger.Txn) error {
		if commits++; commits == 2 {
			txn.Discard()
			return errors.New("injected stop before the second commit")
		}
		return txn.Commit()
	}
	assert.PanicsWithValue(t,
		fmt.Sprintf("meta: committing applied entries 1-%d failed; stopping so the log rebuilds the store: injected stop before the second commit", n),
		func() { fsm.ApplyBatch(logs) })

	db, fsm = restart(t, db, opts)
	first := storedApplied(t, db)
	require.Greater(t, first, uint64(1))
	require.Less(t, first, uint64(n), "the first commit holds only part of the batch")
	assertStored(t, db, map[string]*string{"objects/k": str(value(int(first)))})

	replayed := fsm.ApplyBatch(logs)

	for i, resp := range replayed {
		index := i + 1
		if uint64(index) <= first {
			assert.Nil(t, resp, "entry %d is in the store and is skipped", index)
			continue
		}
		assert.Equal(t, []byte(value(index-1)), resp, "entry %d applies once, after the one before it", index)
	}
	assertStored(t, db, map[string]*string{"objects/k": str(value(n))})
	assert.Equal(t, uint64(n), storedApplied(t, db))
}

// A restore from a snapshot older than the store moves the index back, on disk
// and in memory, so the entries after the snapshot apply again.
func TestFSM_Restore_AnOlderSnapshotMovesTheIndexBack(t *testing.T) {
	opts := badger.DefaultOptions(t.TempDir()).WithLoggingLevel(badger.WARNING)
	db := openStore(t, opts)
	fsm := NewFSM(db)
	fsm.ApplyBatch([]*raft.Log{swapLog(t, 1, "objects/k", "gen-1"), swapLog(t, 2, "objects/k", "gen-2")})
	older := snapshotStream(t, fsm)
	later := []*raft.Log{swapLog(t, 3, "objects/k", "gen-3"), swapLog(t, 4, "objects/k", "gen-4")}
	fsm.ApplyBatch(later)
	require.Equal(t, uint64(4), fsm.persisted)

	require.NoError(t, fsm.Restore(io.NopCloser(bytes.NewReader(older))))

	assert.Equal(t, uint64(2), fsm.persisted)
	assert.Equal(t, uint64(2), storedApplied(t, db))
	assertStored(t, db, map[string]*string{"objects/k": str("gen-2")})

	db, fsm = restart(t, db, opts)
	assert.Equal(t, []any{[]byte("gen-2"), []byte("gen-3")}, fsm.ApplyBatch(later))
	assertStored(t, db, map[string]*string{"objects/k": str("gen-4")})
}

// A snapshot from before the row existed holds none, so the restore removes
// the local row and replay skips nothing.
func TestFSM_Restore_ASnapshotWithoutTheRowClearsTheIndex(t *testing.T) {
	for name, stream := range map[string][]byte{
		"sorted":      snapshotOf(t, map[string][]byte{"objects/k": []byte("snap")}),
		"legacy json": []byte(`{"objects/k":"c25hcA=="}`),
	} {
		t.Run(name, func(t *testing.T) {
			db := newTestDB(t)
			fsm := NewFSM(db)
			fsm.ApplyBatch([]*raft.Log{swapLog(t, 1, "objects/k", "gen-1"), swapLog(t, 2, "objects/j", "gen-1")})
			require.Equal(t, uint64(2), fsm.persisted)

			require.NoError(t, fsm.Restore(io.NopCloser(bytes.NewReader(stream))))

			assert.Zero(t, fsm.persisted)
			_, err := dbGet(db, appliedKey)
			assert.ErrorIs(t, err, badger.ErrKeyNotFound)
			assert.Equal(t, map[string]string{"objects/k": "snap"}, contents(t, db))
			assert.Equal(t, []any{[]byte("snap")}, fsm.ApplyBatch([]*raft.Log{swapLog(t, 1, "objects/k", "gen-1")}))
		})
	}
}

// A truncated snapshot that fails before any write reaches the store is
// refused with an error and leaves the store and the row unchanged.
func TestFSM_Restore_AFailureBeforeAnyWriteLeavesTheStore(t *testing.T) {
	stream := snapshotOf(t, map[string][]byte{"objects/a": []byte("snap"), "objects/b": []byte("snap")})
	db := newTestDB(t)
	fsm := NewFSM(db)
	fsm.Apply(swapLog(t, 5, "objects/a", "local"))
	before := dumpStore(t, db)

	err := fsm.Restore(io.NopCloser(bytes.NewReader(stream[:len(stream)-2])))

	require.Error(t, err)
	assert.Equal(t, before, dumpStore(t, db))
	assert.Equal(t, uint64(5), fsm.persisted)
}

// A local read failure is this replica's alone, so it stops the replica
// before any commit names the entry. A healthy replica applies the same log,
// and the failed one, restarted, applies the entry again and agrees with it.
func TestFSM_ApplyBatch_AStoreReadFailureStopsBeforeTheRowPassesTheEntry(t *testing.T) {
	cases := map[string]Command{
		"transaction": txnCommand([]TxnCheck{holds("intents/k", "pending")},
			put("objects/k", "gen-1"), put("intents/k", "committed")),
		"swap": {Type: CommandSwap, Key: []byte("intents/k"), Value: []byte("committed")},
	}
	for name, entry := range cases {
		t.Run(name, func(t *testing.T) {
			logs := []*raft.Log{
				commandLog(t, 1, Command{Type: CommandPut, Key: []byte("intents/k"), Value: []byte("pending")}),
				commandLog(t, 2, Command{Type: CommandPut, Key: []byte("before/k"), Value: []byte("v")}),
				commandLog(t, 3, entry),
				commandLog(t, 4, Command{Type: CommandPut, Key: []byte("after/k"), Value: []byte("v")}),
			}
			ref := newTestDB(t)
			want := NewFSM(ref).ApplyBatch(logs)

			opts := badger.DefaultOptions(t.TempDir()).WithLoggingLevel(badger.WARNING)
			db := openStore(t, opts)
			fsm := NewFSM(db)
			fsm.ApplyBatch(logs[:1])
			fsm.copyValue = func(*badger.Item) ([]byte, error) { return nil, errors.New("injected read failure") }

			assert.PanicsWithValue(t,
				"meta: reading the store to apply entry 3 failed; stopping so the log applies it again: injected read failure",
				func() { fsm.ApplyBatch(logs[1:]) })
			assert.Less(t, storedApplied(t, db), uint64(3), "no commit names the failed entry")
			assertStored(t, db, map[string]*string{"intents/k": str("pending"), "after/k": nil})

			db, fsm = restart(t, db, opts)
			got := fsm.ApplyBatch(logs)

			assert.Equal(t, want[2], got[2], "the restarted replica evaluates the entry again")
			assert.Equal(t, dumpStore(t, ref), dumpStore(t, db))
		})
	}
}

// Badger keeps a value at or above its value threshold in the value log and
// charges the transaction for a pointer only, so it takes a value larger than
// a whole transaction. The release before the row took it too, and replay
// after restoring an older snapshot must apply it again.
func TestFSM_ApplyBatch_AValueInTheValueLogAppliesAndReplays(t *testing.T) {
	opts := smallTxnOptions(t.TempDir())
	db := openStore(t, opts)
	key := []byte("objects/big")
	value := bytes.Repeat([]byte{0x7a}, int(db.MaxBatchSize())+1)

	// The premise: badger alone takes the value in an empty transaction,
	// and a charge for the whole value could not fit.
	premise := db.NewTransaction(true)
	require.NoError(t, premise.Set(key, value))
	premise.Discard()
	require.Greater(t, int64(len(value)), db.MaxBatchSize())

	fsm := NewFSM(db)
	logs := []*raft.Log{
		swapLog(t, 1, "objects/small", "gen-1"),
		commandLog(t, 2, Command{Type: CommandPut, Key: key, Value: value}),
		swapLog(t, 3, string(key), "gen-2"),
	}
	require.Equal(t, []any{[]byte(nil)}, fsm.ApplyBatch(logs[:1]))
	older := snapshotStream(t, fsm)
	assert.Equal(t, []any{nil, value}, fsm.ApplyBatch(logs[1:]))
	require.NoError(t, fsm.Restore(io.NopCloser(bytes.NewReader(older))))

	db, fsm = restart(t, db, opts)
	replayed := fsm.ApplyBatch(logs)

	assert.Equal(t, []any{nil, nil, value}, replayed, "entry 2 applies again, so entry 3 replaces it")
	assertStored(t, db, map[string]*string{string(key): str("gen-2"), "objects/small": str("gen-1")})
	assert.Equal(t, uint64(3), storedApplied(t, db))
}
