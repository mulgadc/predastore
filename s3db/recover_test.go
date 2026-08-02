package s3db_test

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/mulgadc/predastore/s3db"
)

// startSoloReplica runs one raft node over the rpc stream layer with no peers
// but itself, so it elects immediately and needs no dialable siblings. The
// data directory is the caller's, so a node can be stopped and started again
// over the same state.
func startSoloReplica(t *testing.T, id uint64, dataDir, pipeName string) (*s3db.RaftNode, func()) {
	t.Helper()

	pipeTr := transport.NewPipeTransport()

	// A solo member never dials anyone; a call here means the recovered
	// configuration still names a peer that should have been dropped.
	dial := func(context.Context, raft.ServerAddress) (transport.Stream, error) {
		return nil, fmt.Errorf("unexpected raft dial from solo replica %d", id)
	}

	layer := s3db.NewRPCStreamLayer(fmt.Sprintf("node-%d", id), dial)

	srvAddr, err := transport.ResolveAddr(string(transport.NetworkPipe), pipeName)
	if err != nil {
		t.Fatalf("ResolveAddr: %v", err)
	}
	srv, err := rpc.NewServer(rpc.ServerConfig{
		Mux:          rpc.NewMux(),
		Addrs:        []net.Addr{srvAddr},
		Transports:   []transport.Transport{pipeTr},
		DrainTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srvCtx, srvCancel := context.WithCancel(context.Background())
	srvDone := make(chan struct{})
	go func() {
		defer close(srvDone)
		srv.Run(srvCtx)
	}()

	cfg := s3db.DefaultClusterConfig()
	cfg.NodeID = id
	cfg.DataDir = dataDir
	cfg.Bootstrap = true
	cfg.StreamLayer = layer
	cfg.Peers = []s3db.RaftPeer{{ID: id, Address: fmt.Sprintf("node-%d", id)}}

	node, err := s3db.NewRaftNode(cfg)
	if err != nil {
		srvCancel()
		<-srvDone
		layer.Close()
		t.Fatalf("NewRaftNode(%d): %v", id, err)
	}

	stop := func() {
		if err := node.Close(); err != nil {
			t.Logf("close raft node %d: %v", id, err)
		}
		srvCancel()
		<-srvDone
		layer.Close()
	}
	return node, stop
}

// TestRecoverConfigurationRenumbersMember starts a replica as node 1, then
// recovers its data directory onto node 4 and asserts the state survives the
// renumbering. Without recovery the restarted node is absent from its own
// persisted configuration and never elects.
func TestRecoverConfigurationRenumbersMember(t *testing.T) {
	dataDir := t.TempDir()

	node, stop := startSoloReplica(t, 1, dataDir, "recover-proc-old")
	if err := node.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	if err := node.Put("buckets", "example", []byte("payload")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	stop()

	recovered, err := s3db.RecoverConfiguration(dataDir, 4, []s3db.RaftPeer{{ID: 4, Address: "node-4"}})
	if err != nil {
		t.Fatalf("RecoverConfiguration: %v", err)
	}
	if !recovered {
		t.Fatal("RecoverConfiguration reported no state to recover")
	}

	node, stop = startSoloReplica(t, 4, dataDir, "recover-proc-new")
	defer stop()

	if err := node.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("WaitForLeader after recovery: %v", err)
	}
	if got := node.LeaderID(); got != "4" {
		t.Fatalf("leader after recovery = %q, want \"4\"", got)
	}
	val, err := node.Get("buckets", "example")
	if err != nil {
		t.Fatalf("Get after recovery: %v", err)
	}
	if string(val) != "payload" {
		t.Fatalf("value after recovery = %q, want %q", val, "payload")
	}
}

// TestRecoverConfigurationFreshDirectory reports nothing to recover rather
// than failing, so a migration can call it for every node it relocates without
// knowing which ones were ever written to.
func TestRecoverConfigurationFreshDirectory(t *testing.T) {
	dir := t.TempDir()

	recovered, err := s3db.RecoverConfiguration(dir, 4, []s3db.RaftPeer{{ID: 4, Address: "node-4"}})
	if err != nil {
		t.Fatalf("RecoverConfiguration on empty dir: %v", err)
	}
	if recovered {
		t.Fatal("RecoverConfiguration reported state in an empty directory")
	}

	// A store file that exists but was never written to is still fresh.
	if err := writeEmptyRaftStore(filepath.Join(dir, "raft.db")); err != nil {
		t.Fatalf("create empty raft store: %v", err)
	}
	recovered, err = s3db.RecoverConfiguration(dir, 4, []s3db.RaftPeer{{ID: 4, Address: "node-4"}})
	if err != nil {
		t.Fatalf("RecoverConfiguration on empty store: %v", err)
	}
	if recovered {
		t.Fatal("RecoverConfiguration reported state in an unwritten store")
	}
}

// writeEmptyRaftStore creates the bolt store a node opens on first start,
// before anything has been appended to it.
func writeEmptyRaftStore(path string) error {
	store, err := raftboltdb.NewBoltStore(path)
	if err != nil {
		return err
	}
	return store.Close()
}
