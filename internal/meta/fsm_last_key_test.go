package meta

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

const testHeadPrefix = "objects/arn:aws:s3:::predastore/vol/map/head."

func TestLastKeyUsesExactPrefix(t *testing.T) {
	db := newTestDB(t)
	for _, suffix := range []string{"1", "9", "1/map/head.3"} {
		dbSet(t, db, []byte(testHeadPrefix+suffix), []byte("value"))
	}
	dbSet(t, db, []byte("objects/arn:aws:s3:::predastore/vol/map/head/zz"), []byte("outside"))
	fsm := NewFSM(db)
	_, err := fsm.LastKey(testHeadPrefix)
	require.ErrorIs(t, err, errMapHeadIndexUnavailable)
	require.NoError(t, fsm.rebuildMapHeads())

	for _, test := range []struct{ prefix, want string }{
		{testHeadPrefix, testHeadPrefix + "9"},
		{testHeadPrefix + "1/map/head.", testHeadPrefix + "1/map/head.3"},
		{"objects/arn:aws:s3:::predastore/empty/map/head.", ""},
	} {
		key, err := fsm.LastKey(test.prefix)
		require.NoError(t, err)
		require.Equal(t, test.want, key)
	}
}

func TestLastKeyRebuildIgnoresPrunedRange(t *testing.T) {
	db := newTestDB(t)
	live := testHeadPrefix + "0000"
	for n := range 130 {
		dbSet(t, db, fmt.Appendf(nil, "%s%04d", testHeadPrefix, n), []byte("head"))
	}
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		for n := 1; n < 130; n++ {
			if err := txn.Delete(fmt.Appendf(nil, "%s%04d", testHeadPrefix, n)); err != nil {
				return err
			}
		}
		return nil
	}))
	fsm := NewFSM(db)
	require.NoError(t, fsm.rebuildMapHeads())
	key, err := fsm.LastKey(testHeadPrefix)
	require.NoError(t, err)
	require.Equal(t, live, key)
}

func TestLastKeyTracksCommittedBatch(t *testing.T) {
	fsm := NewFSM(newTestDB(t))
	require.NoError(t, fsm.rebuildMapHeads())
	commands := []Command{
		{Type: CommandPut, Key: []byte(testHeadPrefix + "1"), Value: []byte("first")},
		{Type: CommandPut, Key: []byte(testHeadPrefix + "2"), Value: []byte("second")},
		{Type: CommandSwap, Key: []byte(testHeadPrefix + "3"), Value: []byte("third")},
		{Type: CommandSwap, Key: []byte(testHeadPrefix + "3"), Value: []byte("third updated")},
	}
	batch := make([]*raft.Log, 0, len(commands))
	for i, cmd := range commands {
		batch = append(batch, commandLog(t, uint64(i+1), cmd))
	}
	got := fsm.ApplyBatch(batch)
	require.Nil(t, got[0])
	require.Nil(t, got[1])
	require.Equal(t, []byte("third"), got[3])
	key, err := fsm.LastKey(testHeadPrefix)
	require.NoError(t, err)
	require.Equal(t, testHeadPrefix+"3", key)
	require.Nil(t, fsm.Apply(commandLog(t, 5, Command{Type: CommandDelete, Key: []byte(key)})))
	key, err = fsm.LastKey(testHeadPrefix)
	require.NoError(t, err)
	require.Equal(t, testHeadPrefix+"2", key)
	require.Nil(t, fsm.Apply(commandLog(t, 6, Command{Type: CommandDelete, Key: []byte(key)})))
	key, err = fsm.LastKey(testHeadPrefix)
	require.NoError(t, err)
	require.Equal(t, testHeadPrefix+"1", key)
}

func TestLastKeyRebuildAfterSnapshotRestore(t *testing.T) {
	key := testHeadPrefix + "2"
	src := newTestDB(t)
	dbSet(t, src, []byte(key), []byte("new snapshot"))
	sorted := snapshotStream(t, NewFSM(src))
	legacy, err := json.Marshal(map[string][]byte{key: []byte("new snapshot")})
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		data []byte
	}{{"sorted", sorted}, {"legacy", legacy}} {
		t.Run(tc.name, func(t *testing.T) {
			dst := newTestDB(t)
			dbSet(t, dst, []byte(testHeadPrefix+"3"), []byte("old view"))
			fsm := NewFSM(dst)
			require.NoError(t, fsm.rebuildMapHeads())
			require.NoError(t, fsm.Restore(io.NopCloser(bytes.NewReader(tc.data))))
			got, err := fsm.LastKey(testHeadPrefix)
			require.NoError(t, err)
			require.Equal(t, key, got)
			require.Error(t, fsm.Restore(io.NopCloser(bytes.NewReader([]byte("{")))))
			got, err = fsm.LastKey(testHeadPrefix)
			require.ErrorIs(t, err, errMapHeadIndexUnavailable)
			require.Empty(t, got)
		})
	}
}

func TestFailedRestoreCannotBecomeANewSnapshot(t *testing.T) {
	key := testHeadPrefix + "1"
	fsm := NewFSM(newTestDB(t))
	require.NoError(t, fsm.rebuildMapHeads())
	require.Nil(t, fsm.Apply(commandLog(t, 1, Command{
		Type: CommandPut, Key: []byte(key), Value: []byte("acknowledged"),
	})))
	prior := snapshotStream(t, fsm)
	require.Error(t, fsm.Restore(io.NopCloser(bytes.NewReader([]byte("{")))))

	snapshot, err := fsm.Snapshot()
	if snapshot != nil {
		defer snapshot.Release()
	}
	require.Error(t, err, "failed restore must not replace the previous recovery point")
	require.Nil(t, snapshot)
	_, err = fsm.LastKey(testHeadPrefix)
	require.ErrorIs(t, err, errMapHeadIndexUnavailable)

	require.NoError(t, fsm.Restore(io.NopCloser(bytes.NewReader(prior))))
	got, err := fsm.LastKey(testHeadPrefix)
	require.NoError(t, err)
	require.Equal(t, key, got)
	recovered, err := fsm.Snapshot()
	require.NoError(t, err)
	recovered.Release()
}

// On the second user snapshot, pause Raft between capturing the FSM and
// opening its real file sink. A restore at that point can invalidate the capture.
type snapshotStoreCaptureGate struct {
	*raft.FileSnapshotStore
	armed   bool
	entered chan struct{}
	release chan struct{}
}

func (s *snapshotStoreCaptureGate) Create(version raft.SnapshotVersion, index, term uint64,
	configuration raft.Configuration, configurationIndex uint64, transport raft.Transport,
) (raft.SnapshotSink, error) {
	if s.armed {
		close(s.entered)
		<-s.release
	}
	return s.FileSnapshotStore.Create(version, index, term, configuration, configurationIndex, transport)
}

func TestRestoreRejectsAnEarlierLocalSnapshotCapture(t *testing.T) {
	key := testHeadPrefix + "1"
	fsm := NewFSM(newTestDB(t))
	files, err := raft.NewFileSnapshotStore(t.TempDir(), 1, io.Discard)
	require.NoError(t, err)
	store := &snapshotStoreCaptureGate{
		FileSnapshotStore: files, entered: make(chan struct{}), release: make(chan struct{}),
	}
	logs := raft.NewInmemStore()
	addr, transport := raft.NewInmemTransport("")
	t.Cleanup(func() { require.NoError(t, transport.Close()) })
	cfg := raft.DefaultConfig()
	cfg.LocalID = "local"
	cfg.HeartbeatTimeout = 50 * time.Millisecond
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.LeaderLeaseTimeout = 50 * time.Millisecond
	cfg.SnapshotInterval = time.Hour
	cfg.TrailingLogs = 0
	cfg.LogOutput = io.Discard
	members := raft.Configuration{Servers: []raft.Server{{ID: cfg.LocalID, Address: addr}}}
	require.NoError(t, raft.BootstrapCluster(cfg, logs, logs, store, transport, members))
	node, err := raft.NewRaft(cfg, fsm, logs, logs, store, transport)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, node.Shutdown().Error()) })
	require.Eventually(t, func() bool { return node.State() == raft.Leader }, 3*time.Second, 5*time.Millisecond)

	put := func(value string) {
		t.Helper()
		cmd := commandLog(t, 0, Command{Type: CommandPut, Key: []byte(key), Value: []byte(value)})
		applied := node.Apply(cmd.Data, time.Second)
		require.NoError(t, applied.Error())
		require.Nil(t, applied.Response())
	}
	put("acknowledged")
	require.NoError(t, node.Snapshot().Error())
	prior, err := store.List()
	require.NoError(t, err)
	require.Len(t, prior, 1)

	put("acknowledged later")
	first, err := logs.FirstIndex()
	require.NoError(t, err)
	require.Greater(t, first, prior[0].Index, "initial snapshot should already have compacted old logs")

	release := sync.OnceFunc(func() { close(store.release) })
	defer release()
	store.armed = true
	future := node.Snapshot()
	select {
	case <-store.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("Raft did not reach persistence after capturing the snapshot")
	}
	require.Error(t, fsm.Restore(io.NopCloser(bytes.NewReader([]byte("{")))))
	_, err = fsm.Get(key)
	require.ErrorIs(t, err, badger.ErrKeyNotFound, "failed legacy restore must have changed the store")
	release()
	snapshotErr := future.Error()

	snapshots, err := store.List()
	require.NoError(t, err)
	require.Len(t, snapshots, 1)
	require.Equal(t, prior[0].ID, snapshots[0].ID,
		"a stale capture must not publish and reap the previous recovery point")
	require.Error(t, snapshotErr)
	firstAfter, err := logs.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, first, firstAfter, "a stale snapshot cannot compact acknowledged log entries")
}

type blockingMetaSnapshotSink struct {
	mockSnapshotSink

	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingMetaSnapshotSink) Write(data []byte) (int, error) {
	s.once.Do(func() { close(s.started) })
	<-s.release
	return s.mockSnapshotSink.Write(data)
}

func TestLegacyRestoreRefusesWhileSnapshotPersists(t *testing.T) {
	fsm := NewFSM(newTestDB(t))
	key := testHeadPrefix + "1"
	require.Nil(t, fsm.Apply(commandLog(t, 1, Command{
		Type: CommandPut, Key: []byte(key), Value: []byte("before"),
	})))
	captured, err := fsm.Snapshot()
	require.NoError(t, err)
	defer captured.Release()

	sink := &blockingMetaSnapshotSink{started: make(chan struct{}), release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(sink.release) })
	defer release()
	persisted := make(chan error, 1)
	go func() { persisted <- captured.Persist(sink) }()
	select {
	case <-sink.started:
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot did not begin persistence")
	}

	write := commandLog(t, 2, Command{Type: CommandPut, Key: []byte(key), Value: []byte("after")})
	applied := make(chan any, 1)
	go func() { applied <- fsm.Apply(write) }()
	select {
	case result := <-applied:
		require.Nil(t, result)
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot persistence blocked an ordinary metadata apply")
	}

	input := []byte("{")
	restored := make(chan error, 1)
	go func() { restored <- fsm.Restore(io.NopCloser(bytes.NewReader(input))) }()

	var restoreErr error
	refusedBeforeRelease := false
	select {
	case restoreErr = <-restored:
		refusedBeforeRelease = true
	case <-time.After(2 * time.Second):
	}
	value, getErr := fsm.Get(key)
	release()
	require.NoError(t, <-persisted)
	if !refusedBeforeRelease {
		<-restored
	}
	require.True(t, refusedBeforeRelease, "legacy restore must return before the sink is released")
	require.Error(t, restoreErr)
	require.NoError(t, getErr, "legacy restore must leave Badger untouched while persistence is active")
	require.Equal(t, []byte("after"), value)
	require.True(t, sink.closed)

	require.Error(t, fsm.Restore(io.NopCloser(bytes.NewReader(input))))
	_, err = fsm.Get(key)
	require.ErrorIs(t, err, badger.ErrKeyNotFound, "retry must reach legacy replacement after persistence")
}
