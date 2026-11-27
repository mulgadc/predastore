package meta

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// raftDisk is what a single-voter replica keeps across restarts besides its
// badger store: its log, its stable state and its snapshots.
type raftDisk struct {
	logs      *raft.InmemStore
	snapshots *raft.FileSnapshotStore
	addr      raft.ServerAddress
	conf      raft.Configuration
}

func newRaftDisk(t *testing.T) *raftDisk {
	t.Helper()
	snapshots, err := raft.NewFileSnapshotStore(t.TempDir(), 3, io.Discard)
	require.NoError(t, err)
	addr := raft.ServerAddress("startup-test-1")
	return &raftDisk{
		logs:      raft.NewInmemStore(),
		snapshots: snapshots,
		addr:      addr,
		conf:      raft.Configuration{Servers: []raft.Server{{Suffrage: raft.Voter, ID: "1", Address: addr}}},
	}
}

// putSnapshot stores stream as the snapshot at index, as raft does after a
// snapshot persists.
func (d *raftDisk) putSnapshot(t *testing.T, index uint64, stream []byte) {
	t.Helper()
	_, trans := raft.NewInmemTransport(d.addr)
	sink, err := d.snapshots.Create(raft.SnapshotVersionMax, index, 1, d.conf, 1, trans)
	require.NoError(t, err)
	_, err = sink.Write(stream)
	require.NoError(t, err)
	require.NoError(t, sink.Close())
}

// start constructs raft over fsm as a replica does at startup, which
// restores the newest snapshot that restores before it returns.
func (d *raftDisk) start(t *testing.T, fsm raft.FSM) (*raft.Raft, error) {
	t.Helper()
	cfg := raft.DefaultConfig()
	cfg.LocalID = "1"
	cfg.LogOutput = io.Discard
	cfg.HeartbeatTimeout = 50 * time.Millisecond
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.LeaderLeaseTimeout = 50 * time.Millisecond
	cfg.CommitTimeout = 5 * time.Millisecond
	_, trans := raft.NewInmemTransport(d.addr)
	r, err := raft.NewRaft(cfg, fsm, d.logs, d.logs, d.snapshots, trans)
	if r != nil {
		t.Cleanup(func() { _ = r.Shutdown().Error() })
	}
	return r, err
}

// failingRestore fails its restore stream after limit bytes, as a replica
// that stops part way through a restore does.
type failingRestore struct {
	*FSM
	limit int64
}

func (f *failingRestore) Restore(rc io.ReadCloser) error {
	return f.FSM.Restore(struct {
		io.Reader
		io.Closer
	}{io.MultiReader(io.LimitReader(rc, f.limit), failReader{}), rc})
}

type failReader struct{}

func (failReader) Read([]byte) (int, error) { return 0, errors.New("injected stop during restore") }

// recordingFSM keeps the answer to every command entry the FSM was given.
type recordingFSM struct {
	*FSM
	mu   sync.Mutex
	seen map[uint64]any
}

func (r *recordingFSM) ApplyBatch(logs []*raft.Log) []any {
	resps := r.FSM.ApplyBatch(logs)
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, log := range logs {
		if log.Type == raft.LogCommand {
			r.seen[log.Index] = resps[i]
		}
	}
	return resps
}

func (r *recordingFSM) answers() map[uint64]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seen
}

var (
	_ raft.BatchingFSM = (*recordingFSM)(nil)
	_ raft.FSM         = (*failingRestore)(nil)
)

// A replica that stops part way through restoring its snapshot at startup
// recovers on its next start: raft restores the whole snapshot, and the log
// after it applies each entry once.
func TestStartup_AnInterruptedSnapshotRestoreRecoversOnTheNextStart(t *testing.T) {
	src := NewFSM(openStore(t, smallTxnOptions(t.TempDir())))
	const n = 40
	big := func(i int) string { return string(bytes.Repeat([]byte{byte(i)}, 8<<10)) }
	for i := 1; i <= n; i++ {
		src.Apply(swapLog(t, uint64(i), fmt.Sprintf("objects/%02d", i), big(i)))
	}
	stream := snapshotStream(t, src)

	disk := newRaftDisk(t)
	disk.putSnapshot(t, n, stream)
	require.NoError(t, disk.logs.StoreLogs([]*raft.Log{
		swapLog(t, n+1, fmt.Sprintf("objects/%02d", n), "gen-2"),
		swapLog(t, n+2, "objects/zz", "gen-1"),
	}))
	require.NoError(t, disk.logs.SetUint64([]byte("CurrentTerm"), 1))

	opts := smallTxnOptions(t.TempDir())
	db := openStore(t, opts)
	fsm := NewFSM(db)
	fsm.ApplyBatch([]*raft.Log{swapLog(t, 1, "objects/01", "local"), swapLog(t, 2, "objects/zz", "local")})

	assert.Panics(t, func() { _, _ = disk.start(t, &failingRestore{FSM: fsm, limit: int64(len(stream) - 8)}) })
	require.Equal(t, uint64(2), storedApplied(t, db), "the row is written only after every data row")
	assertStored(t, db, map[string]*string{"objects/01": str(big(1)), "objects/zz": str("local")})

	db, fsm = restart(t, db, opts)
	rec := &recordingFSM{FSM: fsm, seen: map[uint64]any{}}
	r, err := disk.start(t, rec)
	require.NoError(t, err)
	assert.Equal(t, uint64(n), fsm.persisted, "startup restored the whole snapshot")
	require.Eventually(t, func() bool { return r.State() == raft.Leader }, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, r.Barrier(5*time.Second).Error())

	assert.Equal(t, map[uint64]any{n + 1: []byte(big(n)), n + 2: []byte(nil)}, rec.answers(),
		"each entry after the snapshot is evaluated once, against the restored store")
	assertStored(t, db, map[string]*string{
		"objects/01": str(big(1)), fmt.Sprintf("objects/%02d", n): str("gen-2"), "objects/zz": str("gen-1"),
	})
	assert.Equal(t, uint64(n+2), storedApplied(t, db))
}
