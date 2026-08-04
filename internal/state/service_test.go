package state_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/state"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/mulgadc/predastore/s3db"
)

// startStateProc simulates one process hosting a state replica behind the
// production wire protocol: raft dial and state ops on one mux.
func startStateProc(t *testing.T, id uint64, pipeNames map[uint64]string, peers []s3db.RaftPeer) *s3db.RaftNode {
	t.Helper()

	pipeTr := transport.NewPipeTransport()
	client := rpc.NewClient(rpc.ClientConfig{
		Transports: []transport.Transport{pipeTr},
	})

	dial := func(ctx context.Context, address raft.ServerAddress) (transport.Stream, error) {
		target, err := state.ParseRaftAddress(string(address))
		if err != nil {
			return nil, err
		}
		addr, err := transport.ResolveAddr(string(transport.NetworkPipe), pipeNames[target])
		if err != nil {
			return nil, err
		}
		return rpc.OpenStream(ctx, client, addr, state.OpRaftDial, &state.RaftDial{})
	}

	layer := s3db.NewRPCStreamLayer(state.RaftAddress(id), dial)
	t.Cleanup(func() { layer.Close() })

	cfg := s3db.DefaultClusterConfig()
	cfg.NodeID = id
	cfg.DataDir = t.TempDir()
	cfg.Bootstrap = true
	cfg.StreamLayer = layer
	cfg.Peers = peers

	node, err := s3db.NewRaftNode(cfg)
	if err != nil {
		t.Fatalf("NewRaftNode(%d): %v", id, err)
	}
	t.Cleanup(func() { node.Close() })

	svc := state.NewService(id, node, layer)
	mux := rpc.NewMux()
	svc.Register(mux)

	srvAddr, err := transport.ResolveAddr(string(transport.NetworkPipe), pipeNames[id])
	if err != nil {
		t.Fatalf("ResolveAddr: %v", err)
	}
	srv, err := rpc.NewServer(rpc.ServerConfig{
		Mux:          mux,
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
	t.Cleanup(func() {
		srvCancel()
		<-srvDone
	})

	return node
}

// TestStateServiceOverRPC drives the full state path — client, wire
// protocol, service, raft — across three replicas over pipe streams.
func TestStateServiceOverRPC(t *testing.T) {
	pipeNames := map[uint64]string{
		1: "state-svc-proc1",
		2: "state-svc-proc2",
		3: "state-svc-proc3",
	}
	peers := []s3db.RaftPeer{
		{ID: 1, Address: state.RaftAddress(1)},
		{ID: 2, Address: state.RaftAddress(2)},
		{ID: 3, Address: state.RaftAddress(3)},
	}

	nodes := make(map[uint64]*s3db.RaftNode, len(pipeNames))
	for id := range pipeNames {
		nodes[id] = startStateProc(t, id, pipeNames, peers)
	}
	if err := nodes[1].WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}

	// The client is its own "process": it reaches every replica over rpc.
	rpcClient := rpc.NewClient(rpc.ClientConfig{
		Transports: []transport.Transport{transport.NewPipeTransport()},
	})
	cli, err := state.NewClient(state.ClientConfig{
		Client: rpcClient,
		Resolve: func(nodeID uint64) (net.Addr, error) {
			name, ok := pipeNames[nodeID]
			if !ok {
				return nil, fmt.Errorf("unknown node %d", nodeID)
			}
			return transport.ResolveAddr(string(transport.NetworkPipe), name)
		},
		Replicas: []uint64{1, 2, 3},
	})
	if err != nil {
		t.Fatalf("NewRPCClient: %v", err)
	}

	// Writes land regardless of which replica is dialed first: the client
	// follows not-leader redirects.
	for i := range 5 {
		key := fmt.Sprintf("obj/%d", i)
		if err := cli.Put("objects", key, fmt.Appendf(nil, "v%d", i)); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}

	v, err := cli.Get("objects", "obj/3")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(v) != "v3" {
		t.Fatalf("Get = %q, want v3", v)
	}

	if _, err := cli.Get("objects", "missing"); !errors.Is(err, s3db.ErrKeyNotFound) {
		t.Fatalf("Get missing: got %v, want ErrKeyNotFound", err)
	}

	items, err := cli.Scan("objects", "obj/", 3)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("Scan returned %d items, want 3 (limit)", len(items))
	}

	if err := cli.Delete("objects", "obj/0"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// Deletion is applied per replica; poll until no replica still serves
	// the key.
	deadline := time.Now().Add(15 * time.Second)
	for {
		_, err := cli.Get("objects", "obj/0")
		if errors.Is(err, s3db.ErrKeyNotFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("deleted key still readable: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
