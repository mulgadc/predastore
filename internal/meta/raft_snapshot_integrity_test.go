package meta

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

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
	members := raft.Configuration{Servers: []raft.Server{{ID: cfg.LocalID, Address: addr}}}
	prior, err := store.Create(raft.SnapshotVersionMax, 1, 1, members, 1, receiver)
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
	got, err := fsm.Get("key")
	require.NoError(t, err)
	require.Equal(t, []byte("acknowledged"), got)

	damaged := append(snapshotMagic[:len(snapshotMagic):len(snapshotMagic)], 0, 0, 0, 1)
	for index := uint64(2); index <= 3; index++ {
		request := &raft.InstallSnapshotRequest{
			RPCHeader: raft.RPCHeader{
				ProtocolVersion: raft.ProtocolVersionMax,
				ID:              []byte("sender"),
				Addr:            []byte(senderAddr),
			},
			SnapshotVersion:    raft.SnapshotVersionMax,
			Term:               node.CurrentTerm() + 1,
			LastLogIndex:       index,
			LastLogTerm:        1,
			Configuration:      raft.EncodeConfiguration(members),
			ConfigurationIndex: 1,
			Size:               int64(len(damaged)),
		}
		var reply raft.InstallSnapshotResponse
		err := sender.InstallSnapshot(cfg.LocalID, addr, request, &reply, bytes.NewReader(damaged))
		require.ErrorContains(t, err, "read key")
		require.False(t, reply.Success)
	}

	snapshots, err := store.List()
	require.NoError(t, err)
	var retained []string
	for _, candidate := range snapshots {
		retained = append(retained, candidate.ID)
	}
	require.Contains(t, retained, prior.ID(), "failed incoming installs must preserve the usable recovery point")
}
