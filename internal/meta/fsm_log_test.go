package meta

import (
	"bytes"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureLogs redirects the default logger for the duration of one test. The
// snapshot lifecycle is reported nowhere else, so asserting on it means
// asserting on what it logged.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()

	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	return &buf
}

// snapshotStream persists a snapshot of db and hands back the bytes, which is
// what raft sends a follower that has fallen behind.
func snapshotStream(t *testing.T, f *FSM) []byte {
	t.Helper()

	snap, err := f.Snapshot()
	require.NoError(t, err)
	sink := &mockSnapshotSink{}
	require.NoError(t, snap.(*FSMSnapshot).Persist(sink))

	return sink.buf
}

// The capture is what raft's contract says must be cheap, and it is the half
// that stalls writes when it is not. Reporting the index it captured is what
// makes a slow one attributable to a position in the log rather than to a
// wall-clock time.
func TestASnapshotReportsTheIndexItCaptured(t *testing.T) {
	logs := captureLogs(t)

	db := newTestDB(t)
	dbSet(t, db, []byte("objects/a"), []byte("v"))

	f := NewFSM(db)
	f.applied.Store(4242)
	snapshotStream(t, f)

	assert.Contains(t, logs.String(), "meta: snapshot captured")
	assert.Contains(t, logs.String(), "index=4242")
	assert.Contains(t, logs.String(), "duration_us=")
	assert.Contains(t, logs.String(), "meta: snapshot persisted")
}

// The distinction that matters to an operator: raft restores a local snapshot
// while it is starting, every time, and that is routine. A restore after the
// replica is serving means the leader sent one because this node fell outside
// the log it retains, which is the event worth an alarm and the event a test
// uses to prove which catch-up path ran.
func TestOnlyARestoreAfterTheReplicaServesIsAnInstall(t *testing.T) {
	src := newTestDB(t)
	dbSet(t, src, []byte("objects/a"), []byte("v"))
	stream := snapshotStream(t, NewFSM(src))

	t.Run("at boot", func(t *testing.T) {
		logs := captureLogs(t)
		f := NewFSM(newTestDB(t))
		require.NoError(t, f.Restore(io.NopCloser(bytes.NewReader(stream))))

		assert.Contains(t, logs.String(), "install=false")
		assert.NotContains(t, logs.String(), "catching up by snapshot install")
		assert.Contains(t, logs.String(), "meta: snapshot restored")
	})

	t.Run("while serving", func(t *testing.T) {
		logs := captureLogs(t)
		f := NewFSM(newTestDB(t))
		f.serving.Store(true)
		require.NoError(t, f.Restore(io.NopCloser(bytes.NewReader(stream))))

		assert.Contains(t, logs.String(), "catching up by snapshot install")
		assert.Contains(t, logs.String(), "install=true")
	})
}
