package s3db

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errTest = errors.New("test error")

func newTestDB(t *testing.T) *S3DB {
	t.Helper()
	tmpDir := t.TempDir()
	db, err := New(tmpDir)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestS3DB_Exists(t *testing.T) {
	db := newTestDB(t)

	t.Run("key not found", func(t *testing.T) {
		exists, err := db.Exists([]byte("nonexistent"))
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("key exists", func(t *testing.T) {
		require.NoError(t, db.Set([]byte("mykey"), []byte("myvalue")))

		exists, err := db.Exists([]byte("mykey"))
		require.NoError(t, err)
		assert.True(t, exists)
	})
}

func TestS3DB_Delete(t *testing.T) {
	db := newTestDB(t)

	require.NoError(t, db.Set([]byte("delkey"), []byte("delval")))

	// Verify it exists
	val, err := db.Get([]byte("delkey"))
	require.NoError(t, err)
	assert.Equal(t, []byte("delval"), val)

	// Delete it
	require.NoError(t, db.Delete([]byte("delkey")))

	// Verify it's gone
	_, err = db.Get([]byte("delkey"))
	assert.Error(t, err)
}

func TestS3DB_Scan(t *testing.T) {
	db := newTestDB(t)

	// Insert test data
	require.NoError(t, db.Set([]byte("prefix/a"), []byte("va")))
	require.NoError(t, db.Set([]byte("prefix/b"), []byte("vb")))
	require.NoError(t, db.Set([]byte("prefix/c"), []byte("vc")))
	require.NoError(t, db.Set([]byte("other/x"), []byte("vx")))

	t.Run("scan with prefix", func(t *testing.T) {
		var keys []string
		err := db.Scan([]byte("prefix/"), func(key, value []byte) error {
			keys = append(keys, string(key))
			return nil
		})
		require.NoError(t, err)
		assert.Len(t, keys, 3)
	})

	t.Run("scan all", func(t *testing.T) {
		var count int
		err := db.Scan(nil, func(key, value []byte) error {
			count++
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 4, count)
	})

	t.Run("scan with callback error", func(t *testing.T) {
		err := db.Scan([]byte("prefix/"), func(key, value []byte) error {
			return errTest
		})
		assert.ErrorIs(t, err, errTest)
	})
}

// FSM Snapshot/Restore tests

func TestFSM_Snapshot(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db.Badger)

	// Add some data via FSM
	require.NoError(t, db.Set([]byte("t/key1"), []byte("val1")))
	require.NoError(t, db.Set([]byte("t/key2"), []byte("val2")))

	snap, err := fsm.Snapshot()
	require.NoError(t, err)
	require.NotNil(t, snap)

	fsmSnap, ok := snap.(*FSMSnapshot)
	require.True(t, ok)
	assert.Len(t, fsmSnap.data, 2)
	assert.Equal(t, []byte("val1"), fsmSnap.data["t/key1"])
	assert.Equal(t, []byte("val2"), fsmSnap.data["t/key2"])
}

func TestFSM_Snapshot_Empty(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db.Badger)

	snap, err := fsm.Snapshot()
	require.NoError(t, err)

	fsmSnap, ok := snap.(*FSMSnapshot)
	require.True(t, ok)
	assert.Empty(t, fsmSnap.data)
}

func TestFSM_Restore(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db.Badger)

	// Add pre-existing data
	require.NoError(t, db.Set([]byte("old/key"), []byte("old-val")))

	// Build the snapshot stream in the on-wire frame format via Persist.
	snap := &FSMSnapshot{data: map[string][]byte{
		"new/key1": []byte("new-val1"),
		"new/key2": []byte("new-val2"),
	}}
	sink := &mockSnapshotSink{}
	require.NoError(t, snap.Persist(sink))

	// Restore from snapshot
	require.NoError(t, fsm.Restore(io.NopCloser(bytes.NewReader(sink.buf))))

	// Old data should be gone
	_, err := db.Get([]byte("old/key"))
	assert.Error(t, err)

	// New data should be present
	val, err := db.Get([]byte("new/key1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("new-val1"), val)

	val, err = db.Get([]byte("new/key2"))
	require.NoError(t, err)
	assert.Equal(t, []byte("new-val2"), val)
}

func TestFSMSnapshot_Persist(t *testing.T) {
	snap := &FSMSnapshot{
		data: map[string][]byte{
			"key1": []byte("val1"),
			"key2": []byte("val2"),
		},
	}

	sink := &mockSnapshotSink{}
	err := snap.Persist(sink)
	require.NoError(t, err)
	assert.True(t, sink.closed)
	assert.False(t, sink.cancelled)

	// The stream must round-trip byte-exact through Restore.
	db := newTestDB(t)
	require.NoError(t, NewFSM(db.Badger).Restore(io.NopCloser(bytes.NewReader(sink.buf))))
	for k, v := range snap.data {
		got, err := db.Get([]byte(k))
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
	require.NoError(t, src.Set(binKey, val))

	snap, err := NewFSM(src.Badger).Snapshot()
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
	require.NoError(t, NewFSM(dst.Badger).Restore(io.NopCloser(bytes.NewReader(sink.buf))))
	got, err := dst.Get(binKey)
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
	require.NoError(t, NewFSM(db.Badger).Restore(io.NopCloser(bytes.NewReader(legacy))))

	got, err := db.Get([]byte("objects/legacy-key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("legacy-val"), got)
}

func TestFSMSnapshot_Release(t *testing.T) {
	snap := &FSMSnapshot{data: map[string][]byte{"k": []byte("v")}}
	snap.Release() // Should not panic
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

func TestS3DB_Close(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := New(tmpDir)
	require.NoError(t, err)

	err = db.Close()
	assert.NoError(t, err)
}

// s3db.New error path.
func TestS3DB_New_BadDir(t *testing.T) {
	db, err := New("/nonexistent/path/that/should/fail")
	if err == nil {
		db.Close()
		os.RemoveAll("/nonexistent/path/that/should/fail")
	}
	// BadgerDB may or may not fail on this path depending on permissions
	// Just verify it doesn't panic
}
