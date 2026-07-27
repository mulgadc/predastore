package s3db_test

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
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/mulgadc/predastore/s3db"
)

const opRaftDialTest rpc.Opcode = 100

// raftDialHeader routes an inbound raft stream to the target node.
type raftDialHeader struct {
	Target uint64 `json:"target"`
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
	node  *s3db.RaftNode
	layer *s3db.RPCStreamLayer
}

// startRaftProc wires a raft node over the rpc stream layer. pipeNames maps
// every node id to its process's pipe endpoint.
func startRaftProc(t *testing.T, id uint64, pipeNames map[uint64]string, peers []s3db.RaftPeer) *raftTestProc {
	t.Helper()

	client := rpc.NewClient(rpc.ClientConfig{
		Transports: []transport.Transport{
			transport.NewPipeTransport(pipeNames[id] + "-client"),
		},
	})

	// Dialing a peer resolves the node-identifying raft address to the
	// peer process's pipe endpoint and frames the target in the header.
	dial := func(ctx context.Context, address raft.ServerAddress) (transport.Stream, error) {
		target, err := strconv.ParseUint(strings.TrimPrefix(string(address), "node-"), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bad raft address %q: %w", address, err)
		}
		addr, err := transport.ResolveAddr(string(transport.NetworkPipe), pipeNames[target])
		if err != nil {
			return nil, err
		}
		return rpc.OpenStream(ctx, client, addr, opRaftDialTest, &raftDialHeader{Target: target})
	}

	layer := s3db.NewRPCStreamLayer(fmt.Sprintf("node-%d", id), dial)
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
		Transports: []transport.Transport{transport.NewPipeTransport(pipeNames[id])},
		// Raft connections are long-lived; don't stall shutdown on them.
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

	return &raftTestProc{node: node, layer: layer}
}

// TestRaftClusterOverRPCStreamLayer elects a leader and replicates a write
// across three raft nodes that talk exclusively over rpc pipe streams.
func TestRaftClusterOverRPCStreamLayer(t *testing.T) {
	pipeNames := map[uint64]string{
		1: "raft-sl-proc1",
		2: "raft-sl-proc2",
		3: "raft-sl-proc3",
	}
	peers := []s3db.RaftPeer{
		{ID: 1, Address: "node-1"},
		{ID: 2, Address: "node-2"},
		{ID: 3, Address: "node-3"},
	}

	procs := make(map[uint64]*raftTestProc, len(pipeNames))
	for id := range pipeNames {
		procs[id] = startRaftProc(t, id, pipeNames, peers)
	}

	// A leader must emerge over the pipe transport.
	if err := procs[1].node.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	var leader *s3db.RaftNode
	for _, p := range procs {
		if p.node.IsLeader() {
			leader = p.node
			break
		}
	}
	if leader == nil {
		t.Fatal("no proc reports leadership")
	}

	if err := leader.Put("objects", "k1", []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// The write must replicate to every follower's local FSM.
	for id, p := range procs {
		deadline := time.Now().Add(15 * time.Second)
		for {
			v, err := p.node.Get("objects", "k1")
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
