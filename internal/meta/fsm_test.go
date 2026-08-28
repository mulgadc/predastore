package meta

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestDB opens a badger store the FSM can be pointed at directly, standing
// in for the one the raft node opens under its data dir.
func newTestDB(t *testing.T) *badger.DB {
	t.Helper()
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLoggingLevel(badger.WARNING))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

// dbSet writes a raw key, bypassing raft: the FSM's own read path is what is
// under test, not the consensus write path.
func dbSet(t *testing.T, db *badger.DB, key, value []byte) {
	t.Helper()
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	}))
}

// dbGet reads a raw key back.
func dbGet(db *badger.DB, key []byte) ([]byte, error) {
	var value []byte
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	})
	return value, err
}

// snapshotOf loads data into a store and persists a real snapshot of it. A
// snapshot is a transaction over badger rather than a map handed to Persist, so
// this is now the only way to obtain a stream -- which is the right way round:
// every test below goes through the code the cluster runs.
func snapshotOf(t *testing.T, data map[string][]byte) []byte {
	t.Helper()

	src := newTestDB(t)
	for k, v := range data {
		dbSet(t, src, []byte(k), v)
	}

	snap, err := NewFSM(src).Snapshot()
	require.NoError(t, err)
	defer snap.Release()

	sink := &mockSnapshotSink{}
	require.NoError(t, snap.Persist(sink))

	return sink.buf
}

func TestFSM_Snapshot(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	dbSet(t, db, []byte("t/key1"), []byte("val1"))
	dbSet(t, db, []byte("t/key2"), []byte("val2"))

	snap, err := fsm.Snapshot()
	require.NoError(t, err)
	require.NotNil(t, snap)
	defer snap.Release()

	sink := &mockSnapshotSink{}
	require.NoError(t, snap.Persist(sink))

	// Read it back into a fresh store, which is what a follower does with it.
	dst := newTestDB(t)
	require.NoError(t, NewFSM(dst).Restore(io.NopCloser(bytes.NewReader(sink.buf))))
	for k, want := range map[string][]byte{"t/key1": []byte("val1"), "t/key2": []byte("val2")} {
		got, err := dbGet(dst, []byte(k))
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}
}

func TestFSM_Snapshot_Empty(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	snap, err := fsm.Snapshot()
	require.NoError(t, err)
	defer snap.Release()

	sink := &mockSnapshotSink{}
	require.NoError(t, snap.Persist(sink))

	// An empty store persists the marker and nothing else, and restores to an
	// empty store rather than to an error.
	dst := newTestDB(t)
	dbSet(t, dst, []byte("stale"), []byte("v"))
	require.NoError(t, NewFSM(dst).Restore(io.NopCloser(bytes.NewReader(sink.buf))))
	_, err = dbGet(dst, []byte("stale"))
	assert.ErrorIs(t, err, badger.ErrKeyNotFound)
}

func TestFSM_Restore(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	// Add pre-existing data
	dbSet(t, db, []byte("old/key"), []byte("old-val"))

	// Build the snapshot stream in the on-wire frame format via Persist.
	stream := snapshotOf(t, map[string][]byte{
		"new/key1": []byte("new-val1"),
		"new/key2": []byte("new-val2"),
	})

	// Restore from snapshot
	require.NoError(t, fsm.Restore(io.NopCloser(bytes.NewReader(stream))))

	// Old data should be gone
	_, err := dbGet(db, []byte("old/key"))
	assert.Error(t, err)

	// New data should be present
	val, err := dbGet(db, []byte("new/key1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("new-val1"), val)

	val, err = dbGet(db, []byte("new/key2"))
	require.NoError(t, err)
	assert.Equal(t, []byte("new-val2"), val)
}

func TestFSMSnapshot_Persist(t *testing.T) {
	want := map[string][]byte{
		"key1": []byte("val1"),
		"key2": []byte("val2"),
	}

	src := newTestDB(t)
	for k, v := range want {
		dbSet(t, src, []byte(k), v)
	}
	snap, err := NewFSM(src).Snapshot()
	require.NoError(t, err)
	defer snap.Release()

	sink := &mockSnapshotSink{}
	require.NoError(t, snap.Persist(sink))
	assert.True(t, sink.closed)
	assert.False(t, sink.cancelled)

	// The stream must round-trip byte-exact through Restore.
	db := newTestDB(t)
	require.NoError(t, NewFSM(db).Restore(io.NopCloser(bytes.NewReader(sink.buf))))
	for k, v := range want {
		got, err := dbGet(db, []byte(k))
		require.NoError(t, err)
		assert.Equal(t, v, got)
	}
}

// TestFSM_SnapshotRestore_BinaryKeyRoundTrip guards the metadata-loss defect
// where the snapshot was JSON-encoded: object hash rows are keyed
// "objects/"+sha256, which is not valid UTF-8, and encoding/json rewrites those
// bytes to U+FFFD, destroying the row on restore. The stream must preserve keys
// byte-for-byte.
func TestFSM_SnapshotRestore_BinaryKeyRoundTrip(t *testing.T) {
	// A key whose bytes are all >= 0x80 — invalid UTF-8, the JSON failure case.
	var hash [32]byte
	for i := range hash {
		hash[i] = byte(0x80 + i)
	}
	binKey := append([]byte("objects/"), hash[:]...)
	val := []byte("shard-metadata")

	src := newTestDB(t)
	dbSet(t, src, binKey, val)

	snap, err := NewFSM(src).Snapshot()
	require.NoError(t, err)
	sink := &mockSnapshotSink{}
	require.NoError(t, snap.(*FSMSnapshot).Persist(sink))

	// The raw binary key must appear verbatim, with no U+FFFD substitution.
	assert.False(t, bytes.Contains(sink.buf, []byte{0xEF, 0xBF, 0xBD}),
		"snapshot stream must not contain the UTF-8 replacement char")
	assert.True(t, bytes.Contains(sink.buf, binKey),
		"snapshot stream must carry the binary key byte-for-byte")

	// Restore into a fresh store and confirm the exact key resolves.
	dst := newTestDB(t)
	require.NoError(t, NewFSM(dst).Restore(io.NopCloser(bytes.NewReader(sink.buf))))
	got, err := dbGet(dst, binKey)
	require.NoError(t, err)
	assert.Equal(t, val, got)
}

// TestFSM_Restore_LegacyJSON pins backward compatibility: a node upgraded on top
// of a store with pre-existing JSON-format snapshots must still start, so Restore
// has to accept the legacy map encoding as well as the new frame format.
func TestFSM_Restore_LegacyJSON(t *testing.T) {
	legacy, err := json.Marshal(map[string][]byte{
		"objects/legacy-key": []byte("legacy-val"),
	})
	require.NoError(t, err)

	db := newTestDB(t)
	require.NoError(t, NewFSM(db).Restore(io.NopCloser(bytes.NewReader(legacy))))

	got, err := dbGet(db, []byte("objects/legacy-key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("legacy-val"), got)
}

// Release discards the read transaction the snapshot holds. One that is never
// released pins every version it can see against compaction, so this is not
// merely a no-op that must not panic.
func TestFSMSnapshot_Release(t *testing.T) {
	db := newTestDB(t)
	dbSet(t, db, []byte("k"), []byte("v"))

	snap, err := NewFSM(db).Snapshot()
	require.NoError(t, err)
	snap.Release()
}

// mockSnapshotSink implements raft.SnapshotSink for testing.
type mockSnapshotSink struct {
	buf       []byte
	closed    bool
	cancelled bool
}

func (m *mockSnapshotSink) Write(p []byte) (int, error) {
	m.buf = append(m.buf, p...)
	return len(p), nil
}

func (m *mockSnapshotSink) Close() error {
	m.closed = true
	return nil
}

func (m *mockSnapshotSink) Cancel() error {
	m.cancelled = true
	return nil
}

func (m *mockSnapshotSink) ID() string {
	return "mock-snap"
}
