package state_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/state"
	"github.com/mulgadc/predastore/internal/topology"
	"github.com/mulgadc/predastore/internal/transport"
)

const opRaftDialTest rpc.Opcode = 100

// raftDialHeader routes an inbound raft stream to the target node.
type raftDialHeader struct {
	Target topology.NodeID `json:"target"`
}

func (h *raftDialHeader) Append(buf []byte) ([]byte, error) {
	b, err := json.Marshal(h)
	if err != nil {
		return nil, err
	}
	return append(buf, b...), nil
}

func (h *raftDialHeader) Unmarshal(b []byte) error {
	return json.Unmarshal(b, h)
}

// raftTestProc simulates one process hosting one raft node: an rpc server on
// its own pipe endpoint routing raft dials to the node's stream layer.
type raftTestProc struct {
	node  *state.Server
	layer *state.RPCStreamLayer
}

// startRaftProc wires a raft node over the rpc stream layer. topo maps every
// node id to its process's pipe endpoint.
func startRaftProc(t *testing.T, id topology.NodeID, topo procTopo, peers []state.RaftPeer) *raftTestProc {
	t.Helper()

	pipeTr := transport.NewPipeTransport()
	client := rpc.NewClient(rpc.ClientConfig{
		Transports: []transport.Transport{pipeTr},
		Resolver:   topo,
	})

	// Dialing a peer parses the node id out of the node-identifying raft
	// address and frames the target in the header.
	dial := func(ctx context.Context, address raft.ServerAddress) (transport.Stream, error) {
		parsed, err := strconv.ParseUint(strings.TrimPrefix(string(address), "node-"), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bad raft address %q: %w", address, err)
		}
		target := topology.NodeID(parsed)
		return rpc.OpenStream(ctx, client, target, opRaftDialTest, &raftDialHeader{Target: target})
	}

	layer := state.NewRPCStreamLayer(fmt.Sprintf("node-%d", id), dial)
	t.Cleanup(func() { layer.Close() })

	mux := rpc.NewMux()
	rpc.RegisterHandler(mux, opRaftDialTest, func(ctx context.Context, h raftDialHeader, stream transport.Stream) error {
		if h.Target != id {
			return fmt.Errorf("raft dial for node %d landed on node %d", h.Target, id)
		}
		return layer.Deliver(ctx, stream)
	})

	srv, err := rpc.NewServer(rpc.ServerConfig{
		Mux:        mux,
		NodeID:     id,
		Resolver:   topo,
		Transports: []transport.Transport{pipeTr},
		// Raft connections are long-lived; don't stall shutdown on them.
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

	return &raftTestProc{node: node, layer: layer}
}

// TestRaftClusterOverRPCStreamLayer elects a leader and replicates a write
// across three raft nodes that talk exclusively over rpc pipe streams.
func TestRaftClusterOverRPCStreamLayer(t *testing.T) {
	topo := procTopo{
		1: "raft-sl-proc1",
		2: "raft-sl-proc2",
		3: "raft-sl-proc3",
	}
	peers := []state.RaftPeer{
		{ID: 1, Address: "node-1"},
		{ID: 2, Address: "node-2"},
		{ID: 3, Address: "node-3"},
	}

	procs := make(map[topology.NodeID]*raftTestProc, len(topo))
	for id := range topo {
		procs[id] = startRaftProc(t, id, topo, peers)
	}

	// A leader must emerge over the pipe transport.
	if err := procs[1].node.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	var leader *state.Server
	for _, p := range procs {
		if p.node.IsLeader() {
			leader = p.node
			break
		}
	}
	if leader == nil {
		t.Fatal("no proc reports leadership")
	}

	if err := leader.Put("objects/k1", []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// The write must replicate to every follower's local FSM.
	for id, p := range procs {
		deadline := time.Now().Add(15 * time.Second)
		for {
			v, err := p.node.Get("objects/k1")
			if err == nil && string(v) == "v1" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %d never saw the replicated key: v=%q err=%v", id, v, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}
