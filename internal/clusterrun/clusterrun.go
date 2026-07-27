// Package clusterrun assembles the per-process cluster runtime: transports,
// the rpc server multiplexing every local node, and each node's raft replica
// or shard store. cmd/s3d builds one and hands its backend to the S3 server.
package clusterrun

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/backend/distributed"
	"github.com/mulgadc/predastore/internal/cluster"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/storage"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/mulgadc/predastore/internal/wire"
	"github.com/mulgadc/predastore/pkg/masterkey"
	"github.com/mulgadc/predastore/s3"
	"github.com/mulgadc/predastore/s3db"
	"github.com/mulgadc/predastore/store"
)

// pipeEndpoint names the process's in-process transport endpoint; every
// colocated node is reachable there.
const pipeEndpoint = "s3d"

// Runtime is everything a process runs besides the S3 frontend.
type Runtime struct {
	// Backend is the fully wired storage backend for the S3 frontend.
	Backend backend.Backend

	rpcCancel context.CancelFunc
	rpcDone   chan struct{}
	raftNodes []*s3db.RaftNode
	stores    []*store.Store
}

// Build assembles the process for the selected nodes: local nodes talk over
// the pipe, remote nodes over quic, and rpc clients pick the transport per
// address.
func Build(cfg *s3.Config, localIDs []int, tlsCert, tlsKey string, key *masterkey.Key) (*Runtime, error) {
	topo, err := cluster.NewTopology(cfg.Hosts, cfg.ClusterNodes, localIDs, pipeEndpoint)
	if err != nil {
		return nil, err
	}

	// The pipe transport serves and dials in-process traffic. The network
	// only comes up when some node runs outside this process: one quic
	// listener per local bind address, plus a dial-only transport for
	// clients.
	pipeTr := transport.NewPipeTransport(topo.PipeName())
	serverTrs := []transport.Transport{pipeTr}
	clientTrs := []transport.Transport{pipeTr}
	if topo.NeedsNetwork() {
		if tlsCert == "" || tlsKey == "" {
			return nil, fmt.Errorf("cluster has remote nodes: -tls-cert and -tls-key are required")
		}
		for _, bind := range topo.LocalBindAddrs() {
			serverTrs = append(serverTrs, transport.NewQUICTransport(transport.QUICTransportConfig{
				BindAddr: bind,
				TLSCert:  tlsCert,
				TLSKey:   tlsKey,
			}))
		}
		clientTrs = append(clientTrs, transport.NewQUICTransport(transport.QUICTransportConfig{}))
	}

	rpcClient := rpc.NewClient(rpc.ClientConfig{Transports: clientTrs})

	// Services register before the server starts; replicas and stores
	// attach as they come up, and peers retry until they do.
	stateSvc := s3db.NewStateService()
	storageSvc := storage.NewService()
	mux := rpc.NewMux()
	stateSvc.Register(mux)
	storageSvc.Register(mux)

	rpcSrv, err := rpc.NewServer(rpc.ServerConfig{Mux: mux, Transports: serverTrs})
	if err != nil {
		return nil, fmt.Errorf("start rpc server: %w", err)
	}
	rpcCtx, rpcCancel := context.WithCancel(context.Background())
	rpcDone := make(chan struct{})
	go func() {
		defer close(rpcDone)
		if err := rpcSrv.Run(rpcCtx); err != nil {
			slog.Error("rpc server exited", "error", err)
		}
	}()

	rt := &Runtime{rpcCancel: rpcCancel, rpcDone: rpcDone}

	// Raft traffic dials peers through the same client, resolving the
	// node-identifying advertise address to a pipe or quic endpoint.
	raftDial := func(ctx context.Context, address raft.ServerAddress) (transport.Stream, error) {
		target, err := wire.ParseRaftAddress(string(address))
		if err != nil {
			return nil, err
		}
		addr, err := topo.NodeAddr(int(target)) //nolint:gosec // G115: node ids are small positives from validated topology.
		if err != nil {
			return nil, err
		}
		return rpc.OpenStream(ctx, rpcClient, addr, wire.OpRaftDial, &wire.RaftDial{Target: target})
	}

	replicas := topo.NodesByRole(cluster.RoleStateReplica)
	peers := make([]s3db.RaftPeer, len(replicas))
	replicaIDs := make([]uint64, len(replicas))
	for i, n := range replicas {
		id := uint64(n.ID) //nolint:gosec // G115: validated positive node ids.
		peers[i] = s3db.RaftPeer{ID: id, Address: wire.RaftAddress(id)}
		replicaIDs[i] = id
	}

	// Compaction tuning is optional; the store applies its own default
	// interval when enabled without one.
	storeOpts := []store.Option{store.WithAEAD(key.AEAD)}
	if cfg.Compaction.IntervalSeconds > 0 {
		storeOpts = append(storeOpts, store.WithCompaction(time.Duration(cfg.Compaction.IntervalSeconds)*time.Second))
	}

	for _, n := range topo.LocalNodes() {
		host, _ := topo.Host(n.HostID)
		id := uint64(n.ID) //nolint:gosec // G115: validated positive node ids.
		dataDir := filepath.Join(host.DataDir, fmt.Sprintf("node-%d", n.ID), string(n.Role))

		switch n.Role {
		case cluster.RoleStateReplica:
			layer := s3db.NewRPCStreamLayer(wire.RaftAddress(id), raftDial)
			ccfg := s3db.DefaultClusterConfig()
			ccfg.NodeID = id
			ccfg.DataDir = dataDir
			// Bootstrapping with an identical peer set is idempotent
			// across replicas, so every replica may attempt it.
			ccfg.Bootstrap = true
			ccfg.StreamLayer = layer
			ccfg.Peers = peers
			node, err := s3db.NewRaftNode(ccfg)
			if err != nil {
				rt.Close()
				return nil, fmt.Errorf("start state replica %d: %w", n.ID, err)
			}
			rt.raftNodes = append(rt.raftNodes, node)
			stateSvc.AddReplica(id, node, layer)

		case cluster.RoleShardStorage:
			// The store expects its directory to exist.
			if err := os.MkdirAll(dataDir, 0750); err != nil {
				rt.Close()
				return nil, fmt.Errorf("create shard store directory %s: %w", dataDir, err)
			}
			st, err := store.Open(dataDir, storeOpts...)
			if err != nil {
				rt.Close()
				return nil, fmt.Errorf("open shard store for node %d: %w", n.ID, err)
			}
			rt.stores = append(rt.stores, st)
			storageSvc.AddNode(id, st)
		}
	}

	// Give consensus a chance to settle before serving S3 traffic; the
	// state client retries, so a slow election degrades rather than fails.
	if len(rt.raftNodes) > 0 {
		if err := rt.raftNodes[0].WaitForLeader(30 * time.Second); err != nil {
			slog.Warn("No leader elected within timeout, continuing anyway", "error", err)
		}
	}

	stateClient, err := s3db.NewRPCClient(s3db.RPCClientConfig{
		Client: rpcClient,
		Resolve: func(nodeID uint64) (net.Addr, error) {
			return topo.NodeAddr(int(nodeID)) //nolint:gosec // G115: validated positive node ids.
		},
		Replicas: replicaIDs,
	})
	if err != nil {
		rt.Close()
		return nil, err
	}
	shardClient, err := storage.NewClient(storage.ClientConfig{
		Client:  rpcClient,
		Resolve: topo.NodeAddr,
	})
	if err != nil {
		rt.Close()
		return nil, err
	}

	// The hash ring places shards on the shard-storage nodes by id; the
	// injected clients make addressing the transports' concern.
	storageNodes := topo.NodesByRole(cluster.RoleShardStorage)
	beNodes := make([]distributed.NodeConfig, len(storageNodes))
	for i, n := range storageNodes {
		beNodes[i] = distributed.NodeConfig{ID: n.ID}
	}
	beBuckets := make([]distributed.BucketConfig, len(cfg.Buckets))
	for i, b := range cfg.Buckets {
		beBuckets[i] = distributed.BucketConfig{
			Name:      b.Name,
			Region:    b.Region,
			Type:      b.Type,
			Public:    b.Public,
			AccountID: b.AccountID,
		}
	}

	be, err := distributed.New(&distributed.Config{
		DataShards:   cfg.RS.Data,
		ParityShards: cfg.RS.Parity,
		Nodes:        beNodes,
		Buckets:      beBuckets,
		StateClient:  stateClient,
		ShardClient:  shardClient,
	})
	if err != nil {
		rt.Close()
		return nil, fmt.Errorf("create distributed backend: %w", err)
	}
	rt.Backend = be

	return rt, nil
}

// Close tears the runtime down: stop accepting rpc streams, then the raft
// replicas, then the shard stores.
func (rt *Runtime) Close() {
	rt.rpcCancel()
	select {
	case <-rt.rpcDone:
	case <-time.After(35 * time.Second):
		slog.Warn("rpc server did not drain in time")
	}
	for _, node := range rt.raftNodes {
		if err := node.Close(); err != nil {
			slog.Warn("closing raft node", "error", err)
		}
	}
	for _, st := range rt.stores {
		if err := st.Close(); err != nil {
			slog.Warn("closing shard store", "error", err)
		}
	}
}
