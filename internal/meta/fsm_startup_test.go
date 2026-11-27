package meta

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
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

// rawSnapshot persists data as a snapshot without an FSM, so the stream can
// hold an applied row that no FSM would accept.
func rawSnapshot(t *testing.T, opts badger.Options, data map[string][]byte) []byte {
	t.Helper()
	src := openStore(t, opts)
	for k, v := range data {
		dbSet(t, src, []byte(k), v)
	}
	snap := &FSMSnapshot{txn: src.NewTransaction(false)}
	defer snap.Release()
	sink := &mockSnapshotSink{}
	require.NoError(t, snap.Persist(sink))
	return sink.buf
}

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

// The newest snapshot holds an applied row that does not decode. Its restore
// fails before it writes, so startup falls back to the older snapshot.
func TestStartup_ABadRowInTheNewestSnapshotFallsBackToAnOlderOne(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true).WithLoggingLevel(badger.WARNING)
	disk := newRaftDisk(t)
	disk.putSnapshot(t, 2, rawSnapshot(t, opts, map[string][]byte{
		"objects/k": []byte("gen-2"), string(appliedKey): encodeApplied(2),
	}))
	disk.putSnapshot(t, 4, rawSnapshot(t, opts, map[string][]byte{
		"objects/k": []byte("gen-4"), "objects/j": []byte("gen-4"), string(appliedKey): []byte("garbage"),
	}))

	db := newTestDB(t)
	fsm := NewFSM(db)
	_, err := disk.start(t, fsm)

	require.NoError(t, err)
	assert.Equal(t, uint64(2), fsm.persisted)
	assert.Equal(t, map[string]string{"objects/k": "gen-2", string(appliedKey): string(encodeApplied(2))}, contents(t, db))
}

// A snapshot whose data the store already holds sends no data write. A failed
// commit of the applied row can still have landed and named the newer index,
// so it stops the replica rather than let raft fall back to an older snapshot.
func TestFSM_Restore_AFailedAppliedRowCommitStopsTheReplica(t *testing.T) {
	dir := t.TempDir()
	opts := badger.DefaultOptions(dir).WithLoggingLevel(badger.WARNING)
	db := openStore(t, opts)
	fsm := NewFSM(db)
	fsm.Apply(swapLog(t, 5, "objects/k", "gen-1"))
	stream := snapshotStream(t, fsm)
	require.NoError(t, db.Close())

	// A read-only store refuses the applied row, the only write this
	// restore makes.
	db = openStore(t, opts.WithReadOnly(true))
	fsm = NewFSM(db)

	var stop any
	func() {
		defer func() { stop = recover() }()
		_ = fsm.Restore(io.NopCloser(bytes.NewReader(stream)))
	}()
	require.NotNil(t, stop, "the restore went on after a failed applied-row commit")
	assert.Contains(t, fmt.Sprint(stop), "write the applied index")
}

// A bad applied row that the stream reaches after a write may have reached
// the store still stops the replica.
func TestFSM_Restore_ABadRowAfterAWriteStopsTheReplica(t *testing.T) {
	opts := smallTxnOptions(t.TempDir())
	data := map[string][]byte{string(appliedKey): []byte("garbage")}
	// These keys sort before the row, so their writes come first, and
	// together they fill more than one badger transaction.
	for i := range 40 {
		data[fmt.Sprintf("\x00a%02d", i)] = bytes.Repeat([]byte{byte(i)}, 8<<10)
	}
	stream := rawSnapshot(t, smallTxnOptions(t.TempDir()), data)

	fsm := NewFSM(openStore(t, opts))
	fsm.Apply(swapLog(t, 1, "objects/k", "local"))

	assert.Panics(t, func() { _ = fsm.Restore(io.NopCloser(bytes.NewReader(stream))) })
}
