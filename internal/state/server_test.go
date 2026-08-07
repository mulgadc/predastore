package state_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/state"
	"github.com/mulgadc/predastore/internal/transport"
)

// procTopo maps node ids to the pipe endpoint their process listens on. It
// stands in for the cluster topology the rpc layer resolves node ids through.
type procTopo map[int]string

func (p procTopo) NodeAddr(nodeID int) (net.Addr, error) {
	name, ok := p[nodeID]
	if !ok {
		return nil, fmt.Errorf("unknown node %d", nodeID)
	}
	return transport.ResolveAddr(string(transport.NetworkPipe), name)
}

func (p procTopo) ListenAddrs(nodeID int) ([]net.Addr, error) {
	addr, err := p.NodeAddr(nodeID)
	if err != nil {
		return nil, err
	}
	return []net.Addr{addr}, nil
}

// startStateProc simulates one process hosting a state replica behind the
// production wire protocol: raft dial and state ops on one mux.
func startStateProc(t *testing.T, id uint64, topo procTopo, peers []state.RaftPeer) *state.Server {
	t.Helper()

	pipeTr := transport.NewPipeTransport()
	client := rpc.NewClient(rpc.ClientConfig{
		Transports: []transport.Transport{pipeTr},
		Topology:   topo,
	})

	dial := func(ctx context.Context, address raft.ServerAddress) (transport.Stream, error) {
		target, err := state.ParseRaftAddress(string(address))
		if err != nil {
			return nil, err
		}
		return rpc.OpenStream(ctx, client, int(target), state.OpRaftDial, &state.RaftDial{})
	}

	layer := state.NewRPCStreamLayer(state.RaftAddress(id), dial)
	t.Cleanup(func() { layer.Close() })

	cfg := state.DefaultClusterConfig()
	cfg.NodeID = id
	cfg.DataDir = t.TempDir()
	cfg.Bootstrap = true
	cfg.StreamLayer = layer
	cfg.Peers = peers

	node, err := state.NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer(%d): %v", id, err)
	}
	t.Cleanup(func() { node.Close() })

	mux := rpc.NewMux()
	node.Register(mux)

	srv, err := rpc.NewServer(rpc.ServerConfig{
		Mux:          mux,
		NodeID:       int(id),
		Topology:     topo,
		Transports:   []transport.Transport{pipeTr},
		DrainTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("rpc.NewServer: %v", err)
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

// startStateCluster brings up a three-replica cluster over pipe streams and
// returns a client that reaches every replica.
func startStateCluster(t *testing.T, prefix string) *state.Client {
	t.Helper()

	topo := procTopo{
		1: prefix + "-proc1",
		2: prefix + "-proc2",
		3: prefix + "-proc3",
	}
	peers := []state.RaftPeer{
		{ID: 1, Address: state.RaftAddress(1)},
		{ID: 2, Address: state.RaftAddress(2)},
		{ID: 3, Address: state.RaftAddress(3)},
	}

	nodes := make(map[int]*state.Server, len(topo))
	for id := range topo {
		nodes[id] = startStateProc(t, uint64(id), topo, peers)
	}
	if err := nodes[1].WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}

	// The client is its own "process": it reaches every replica over rpc.
	rpcClient := rpc.NewClient(rpc.ClientConfig{
		Transports: []transport.Transport{transport.NewPipeTransport()},
		Topology:   topo,
	})
	cli, err := state.NewClient(state.ClientConfig{
		Client:   rpcClient,
		Replicas: []uint64{1, 2, 3},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return cli
}

// TestStateServiceOverRPC drives the full state path — client, wire
// protocol, service, raft — across three replicas over pipe streams.
func TestStateServiceOverRPC(t *testing.T) {
	cli := startStateCluster(t, "state-svc")

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

	if _, err := cli.Get("objects", "missing"); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("Get missing: got %v, want ErrNotFound", err)
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
		if errors.Is(err, state.ErrNotFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("deleted key still readable: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestStateBinaryKeysRoundTrip proves a key of arbitrary bytes survives put,
// get and scan byte-identically. Encoding keys as JSON strings replaced every
// byte that is not valid UTF-8 with U+FFFD, which silently collapsed the
// entropy of the sha256 keys object metadata is stored under.
func TestStateBinaryKeysRoundTrip(t *testing.T) {
	cli := startStateCluster(t, "state-binkey")

	// A raw sha256 stands in for the object metadata keys the backend writes.
	hash := sha256.Sum256([]byte("bucket/object"))

	prefix := "bin\x00\xff/"
	want := map[string][]byte{
		prefix + string([]byte{0x00, 0xff, 0x41, 0x80, 0xfe, 0x42}) + "é世🙂": []byte("mixed"),
		prefix + string(hash[:]): []byte("object-metadata"),
		// These two differ only in a byte that a JSON string collapses to
		// U+FFFD, so they must not land on one stored key.
		prefix + "\x80": []byte("first"),
		prefix + "\xfe": []byte("second"),
	}

	for k, v := range want {
		if err := cli.Put("objects", k, v); err != nil {
			t.Fatalf("Put %q: %v", k, err)
		}
	}

	for k, v := range want {
		got, err := cli.Get("objects", k)
		if err != nil {
			t.Fatalf("Get %q: %v", k, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("Get %q = %q, want %q", k, got, v)
		}
	}

	items, err := cli.Scan("objects", prefix, 0)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(items) != len(want) {
		t.Fatalf("Scan returned %d items, want %d", len(items), len(want))
	}
	for _, item := range items {
		v, ok := want[item.Key]
		if !ok {
			t.Fatalf("Scan returned key %q, which was never written", item.Key)
		}
		if !bytes.Equal(item.Value, v) {
			t.Fatalf("Scan key %q = %q, want %q", item.Key, item.Value, v)
		}
	}
}
