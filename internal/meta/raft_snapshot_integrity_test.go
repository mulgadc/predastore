package meta

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestRaftRefusesSnapshotTruncatedAtFrameBoundary(t *testing.T) {
	const acknowledged = "acknowledged"
	snapshot := snapshotOf(t, map[string][]byte{
		"a": []byte("included"),
		"b": []byte(acknowledged),
	})
	cut := len(snapshotMagic) + 4 + len("a") + 4 + len("included")

	t.Run("restart detects invalid checksum", func(t *testing.T) {
		dir := t.TempDir()
		store, err := raft.NewFileSnapshotStore(dir, 2, io.Discard)
		require.NoError(t, err)
		addr, peer := raft.NewInmemTransport("")
		t.Cleanup(func() { require.NoError(t, peer.Close()) })
		cfg := raft.DefaultConfig()
		cfg.LocalID = "local"
		cfg.LogOutput = io.Discard
		members := raft.Configuration{Servers: []raft.Server{{ID: cfg.LocalID, Address: addr}}}

		sink, err := store.Create(raft.SnapshotVersionMax, 1, 1, members, 1, peer)
		require.NoError(t, err)
		_, err = sink.Write(snapshot)
		require.NoError(t, err)
		require.NoError(t, sink.Close())
		statePath := filepath.Join(dir, "snapshots", sink.ID(), "state.bin")
		require.NoError(t, os.Truncate(statePath, int64(cut)))

		fsm := NewFSM(newTestDB(t))
		dbSet(t, fsm.db, []byte("b"), []byte(acknowledged))
		logs := raft.NewInmemStore()
		_, err = raft.NewRaft(cfg, fsm, logs, logs, store, peer)
		require.ErrorContains(t, err, "failed to load any existing snapshots")
		got, err := fsm.Get("b")
		require.NoError(t, err)
		require.Equal(t, []byte(acknowledged), got)
	})

	t.Run("install detects a short transfer", func(t *testing.T) {
		store, err := raft.NewFileSnapshotStore(t.TempDir(), 2, io.Discard)
		require.NoError(t, err)
		addr, receiver := raft.NewInmemTransport("")
		t.Cleanup(func() { require.NoError(t, receiver.Close()) })
		senderAddr, sender := raft.NewInmemTransport("")
		t.Cleanup(func() { require.NoError(t, sender.Close()) })
		sender.Connect(addr, receiver)

		cfg := raft.DefaultConfig()
		cfg.LocalID = "receiver"
		cfg.LogOutput = io.Discard
		fsm := NewFSM(newTestDB(t))
		dbSet(t, fsm.db, []byte("b"), []byte(acknowledged))
		logs := raft.NewInmemStore()
		node, err := raft.NewRaft(cfg, fsm, logs, logs, store, receiver)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, node.Shutdown().Error()) })

		members := raft.Configuration{Servers: []raft.Server{{ID: cfg.LocalID, Address: addr}}}
		request := &raft.InstallSnapshotRequest{
			RPCHeader: raft.RPCHeader{
				ProtocolVersion: raft.ProtocolVersionMax,
				ID:              []byte("sender"),
				Addr:            []byte(senderAddr),
			},
			SnapshotVersion:    raft.SnapshotVersionMax,
			Term:               1,
			LastLogIndex:       1,
			LastLogTerm:        1,
			Configuration:      raft.EncodeConfiguration(members),
			ConfigurationIndex: 1,
			Size:               int64(len(snapshot)),
		}
		var reply raft.InstallSnapshotResponse
		require.ErrorContains(t, sender.InstallSnapshot(cfg.LocalID, addr, request, &reply, bytes.NewReader(snapshot[:cut])), "short read")
		require.False(t, reply.Success)
		snapshots, err := store.List()
		require.NoError(t, err)
		require.Empty(t, snapshots)
		got, err := fsm.Get("b")
		require.NoError(t, err)
		require.Equal(t, []byte(acknowledged), got)
	})
}

func TestRaftFailedInstallsRetainUsableSnapshot(t *testing.T) {
	dir := t.TempDir()
	store, err := newSnapshotStore(dir, 2, io.Discard)
	require.NoError(t, err)
	addr, receiver := raft.NewInmemTransport("")
	t.Cleanup(func() { require.NoError(t, receiver.Close()) })
	senderAddr, sender := raft.NewInmemTransport("")
	t.Cleanup(func() { require.NoError(t, sender.Close()) })
	sender.Connect(addr, receiver)

	cfg := raft.DefaultConfig()
	cfg.LocalID = "receiver"
	cfg.LogOutput = io.Discard
	members := raft.Configuration{Servers: []raft.Server{{ID: cfg.LocalID, Address: addr}}}
	prior, err := store.active.Create(raft.SnapshotVersionMax, 1, 1, members, 1, receiver)
	require.NoError(t, err)
	snapshot := snapshotOf(t, map[string][]byte{"key": []byte("acknowledged")})
	_, err = prior.Write(snapshot)
	require.NoError(t, err)
	require.NoError(t, prior.Close())

	fsm := NewFSM(newTestDB(t))
	logs := raft.NewInmemStore()
	node, err := raft.NewRaft(cfg, fsm, logs, logs, store, receiver)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, node.Shutdown().Error()) })
	require.Eventually(t, func() bool { return node.State() == raft.Leader },
		5*time.Second, 10*time.Millisecond)
	log := commandLog(t, 0, Command{Type: CommandPut, Key: []byte("suffix"), Value: []byte("acknowledged after snapshot")})
	require.NoError(t, node.Apply(log.Data, time.Second).Error())
	lastApplied := node.AppliedIndex()
	got, err := fsm.Get("key")
	require.NoError(t, err)
	require.Equal(t, []byte("acknowledged"), got)

	install := func(n *raft.Raft, index uint64, data []byte) (raft.InstallSnapshotResponse, error) {
		t.Helper()
		request := &raft.InstallSnapshotRequest{
			RPCHeader: raft.RPCHeader{
				ProtocolVersion: raft.ProtocolVersionMax,
				ID:              []byte("sender"),
				Addr:            []byte(senderAddr),
			},
			SnapshotVersion:    raft.SnapshotVersionMax,
			Term:               n.CurrentTerm() + 1,
			LastLogIndex:       index,
			LastLogTerm:        1,
			Configuration:      raft.EncodeConfiguration(members),
			ConfigurationIndex: 1,
			Size:               int64(len(data)),
		}
		var reply raft.InstallSnapshotResponse
		err := sender.InstallSnapshot(cfg.LocalID, addr, request, &reply, bytes.NewReader(data))
		return reply, err
	}
	damaged := append(snapshotMagic[:len(snapshotMagic):len(snapshotMagic)], 0, 0, 0, 1)
	for index := lastApplied + 1; index <= lastApplied+2; index++ {
		reply, err := install(node, index, damaged)
		require.ErrorContains(t, err, "read key")
		require.False(t, reply.Success)
	}

	snapshots, err := store.List()
	require.NoError(t, err)
	var retained []string
	for _, candidate := range snapshots {
		retained = append(retained, candidate.ID)
	}
	require.Equal(t, []string{prior.ID()}, retained, "failed incoming installs must preserve the usable recovery point")
	staged, err := os.ReadDir(store.stage)
	require.NoError(t, err)
	require.Empty(t, staged)

	// Leave a complete transfer staged as if the process stopped before
	// it could call Restore. It cannot participate in startup selection.
	pending, err := store.Create(raft.SnapshotVersionMax, lastApplied+3, 1, members, 1, receiver)
	require.NoError(t, err)
	_, err = pending.Write(snapshot)
	require.NoError(t, err)
	require.NoError(t, pending.Close())
	require.NoError(t, node.Shutdown().Error())
	store, err = newSnapshotStore(dir, 2, io.Discard)
	require.NoError(t, err)
	staged, err = os.ReadDir(store.stage)
	require.NoError(t, err)
	require.Empty(t, staged)

	// Use a fresh Badger store and no repair peers. Snapshot and replay
	// alone must recover both acknowledgements.
	restartedFSM := NewFSM(newTestDB(t))
	restarted, err := raft.NewRaft(cfg, restartedFSM, logs, logs, store, receiver)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, restarted.Shutdown().Error()) })
	require.Eventually(t, func() bool {
		got, err := restartedFSM.Get("suffix")
		return err == nil && string(got) == "acknowledged after snapshot"
	}, 5*time.Second, 10*time.Millisecond)
	got, err = restartedFSM.Get("key")
	require.NoError(t, err)
	require.Equal(t, []byte("acknowledged"), got)

	validIndex := restarted.AppliedIndex() + 1
	valid := snapshotOf(t, map[string][]byte{"key": []byte("installed")})
	reply, err := install(restarted, validIndex, valid)
	require.NoError(t, err)
	require.True(t, reply.Success)
	published, err := store.List()
	require.NoError(t, err)
	require.Len(t, published, 2)
	require.Equal(t, validIndex, published[0].Index)
	got, err = restartedFSM.Get("key")
	require.NoError(t, err)
	require.Equal(t, []byte("installed"), got)
}
