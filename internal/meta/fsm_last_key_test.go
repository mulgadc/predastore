package meta

import (
	"bytes"
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

	dst := newTestDB(t)
	dbSet(t, dst, []byte(testHeadPrefix+"3"), []byte("old view"))
	fsm := NewFSM(dst)
	require.NoError(t, fsm.rebuildMapHeads())
	require.NoError(t, fsm.Restore(io.NopCloser(bytes.NewReader(snapshotStream(t, NewFSM(src))))))
	got, err := fsm.LastKey(testHeadPrefix)
	require.NoError(t, err)
	require.Equal(t, key, got)
}

func TestFailedRestoreCannotBecomeANewSnapshot(t *testing.T) {
	key := testHeadPrefix + "1"
	fsm := NewFSM(newTestDB(t))
	require.NoError(t, fsm.rebuildMapHeads())
	require.Nil(t, fsm.Apply(commandLog(t, 1, Command{
		Type: CommandPut, Key: []byte(key), Value: []byte("acknowledged"),
	})))
	prior := snapshotStream(t, fsm)
	require.Error(t, fsm.Restore(io.NopCloser(bytes.NewReader(prior[:len(prior)-1]))))

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
// opening its real file sink.
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

func TestUnsupportedRestorePreservesCapturedSnapshot(t *testing.T) {
	key := testHeadPrefix + "1"
	fsm := NewFSM(newTestDB(t))
	require.NoError(t, fsm.rebuildMapHeads())
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
	oldFormats := [][]byte{
		[]byte(`{"old":"dmFsdWU="}`),
		unsortedFrames(t, []string{"b", "a"}, map[string][]byte{"a": []byte("1"), "b": []byte("2")}),
	}
	for _, input := range oldFormats {
		require.ErrorContains(t, fsm.Restore(io.NopCloser(bytes.NewReader(input))), "unsupported metadata snapshot format")
		value, err := fsm.Get(key)
		require.NoError(t, err)
		require.Equal(t, []byte("acknowledged later"), value)
		head, err := fsm.LastKey(testHeadPrefix)
		require.NoError(t, err)
		require.Equal(t, key, head)
	}
	release()
	require.NoError(t, future.Error())

	snapshots, err := store.List()
	require.NoError(t, err)
	require.Len(t, snapshots, 1)
	require.Greater(t, snapshots[0].Index, prior[0].Index)
	require.NoError(t, node.Shutdown().Error())

	recovered := NewFSM(newTestDB(t))
	reopened, err := raft.NewRaft(cfg, recovered, logs, logs, files, transport)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Shutdown().Error()) })
	value, err := recovered.Get(key)
	require.NoError(t, err)
	require.Equal(t, []byte("acknowledged later"), value)
}
