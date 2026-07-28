// Package clusterrun assembles the per-process cluster runtime: one transport
// per network, and for every node this process runs, a service and the rpc
// server carrying it. cmd/s3d is a thin entrypoint over Build and Run.
//
// It lives outside package main because the entrypoint's FIPS boot guard
// panics under a plain `go test`, which would make this untestable there.
package clusterrun

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/hashicorp/raft"
	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/backend/distributed"
	"github.com/mulgadc/predastore/internal/cluster"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/state"
	"github.com/mulgadc/predastore/internal/storage"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/mulgadc/predastore/internal/wire"
	"github.com/mulgadc/predastore/pkg/masterkey"
	"github.com/mulgadc/predastore/s3"
	"github.com/mulgadc/predastore/s3db"
	"github.com/mulgadc/predastore/store"
	"golang.org/x/sync/errgroup"
)

// node is one storage or state replica running in this process: a service
// serving its rpc endpoint, and the server carrying it.
type node struct {
	id  int
	svc interface{ Run(context.Context) error }
	srv *rpc.Server
}

// Runtime is everything this process runs besides the S3 frontend.
type Runtime struct {
	// Backend is the fully wired storage backend for the S3 frontend.
	Backend backend.Backend

	nodes  []node
	client *rpc.Client
	trs    []transport.Transport
	// raftNodes back WaitReady; consensus is what the S3 frontend waits on.
	raftNodes []*s3db.RaftNode
}

// Build assembles the process for the selected nodes. Every node gets its own
// service and rpc server; whether a peer is reached over the pipe or the
// network follows from its address, so nothing below here branches on it.
func Build(cfg *s3.Config, localIDs []int, tlsCert, tlsKey string, key *masterkey.Key) (*Runtime, error) {
	topo, err := cluster.NewTopology(cfg.Hosts, cfg.ClusterNodes, localIDs)
	if err != nil {
		return nil, err
	}

	// One transport per network for the whole process. The pipe carries
	// in-process traffic; the QUIC socket only comes up when some node runs
	// elsewhere, so a single-process cluster needs no certificates.
	trs := []transport.Transport{transport.NewPipeTransport()}
	if topo.NeedsNetwork() {
		if tlsCert == "" || tlsKey == "" {
			return nil, fmt.Errorf("cluster has remote nodes: -tls-cert and -tls-key are required")
		}
		trs = append(trs, transport.NewQUICTransport(transport.QUICTransportConfig{
			TLSCert: tlsCert,
			TLSKey:  tlsKey,
		}))
	}

	rt := &Runtime{
		trs:    trs,
		client: rpc.NewClient(rpc.ClientConfig{Transports: trs}),
	}

	// Raft dials peers through the same client; its advertise addresses are
	// node keys, which the topology resolves to whichever transport applies.
	raftDial := func(ctx context.Context, address raft.ServerAddress) (transport.Stream, error) {
		target, err := wire.ParseRaftAddress(string(address))
		if err != nil {
			return nil, err
		}
		addr, err := topo.NodeAddr(int(target)) //nolint:gosec // G115: node ids are small positives from validated topology.
		if err != nil {
			return nil, err
		}
		return rpc.OpenStream(ctx, rt.client, addr, wire.OpRaftDial, &wire.RaftDial{})
	}

	replicas := topo.NodesByRole(cluster.RoleStateReplica)
	peers := make([]s3db.RaftPeer, len(replicas))
	replicaIDs := make([]uint64, len(replicas))
	for i, n := range replicas {
		id := uint64(n.ID) //nolint:gosec // G115: validated positive node ids.
		peers[i] = s3db.RaftPeer{ID: id, Address: wire.RaftAddress(id)}
		replicaIDs[i] = id
	}

	// Compaction is always enabled: without it, overwrite and delete churn
	// never reclaims dead shards and the store fills. A zero interval falls
	// back to the store's default.
	storeOpts := []store.Option{
		store.WithAEAD(key.AEAD),
		store.WithCompaction(time.Duration(cfg.Compaction.IntervalSeconds) * time.Second),
	}

	for _, n := range topo.LocalNodes() {
		if err := rt.addNode(topo, n, raftDial, peers, storeOpts); err != nil {
			rt.Close()
			return nil, err
		}
	}

	stateClient, err := state.NewClient(state.ClientConfig{
		Client: rt.client,
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
		Client:  rt.client,
		Resolve: topo.NodeAddr,
	})
	if err != nil {
		rt.Close()
		return nil, err
	}

	be, err := rt.buildBackend(cfg, topo, stateClient, shardClient)
	if err != nil {
		rt.Close()
		return nil, err
	}
	rt.Backend = be

	return rt, nil
}

// addNode builds one node's service and the rpc server that carries it. The
// server is constructed but not started: handlers must be registered on every
// node before any of them accepts, or early peer traffic finds no handler.
func (rt *Runtime) addNode(
	topo *cluster.Topology,
	n cluster.Node,
	raftDial func(context.Context, raft.ServerAddress) (transport.Stream, error),
	peers []s3db.RaftPeer,
	storeOpts []store.Option,
) error {
	id := uint64(n.ID) //nolint:gosec // G115: validated positive node ids.
	dataDir := topo.DataDir(n.ID)
	addrs, err := topo.ListenAddrs(n.ID)
	if err != nil {
		return err
	}

	mux := rpc.NewMux()
	var svc interface{ Run(context.Context) error }

	switch n.Role {
	case cluster.RoleStateReplica:
		layer := s3db.NewRPCStreamLayer(wire.RaftAddress(id), raftDial)
		ccfg := s3db.DefaultClusterConfig()
		ccfg.NodeID = id
		ccfg.DataDir = dataDir
		// Bootstrapping with an identical peer set is idempotent across
		// replicas, so every replica may attempt it.
		ccfg.Bootstrap = true
		ccfg.StreamLayer = layer
		ccfg.Peers = peers
		raftNode, err := s3db.NewRaftNode(ccfg)
		if err != nil {
			return fmt.Errorf("start state replica %d: %w", n.ID, err)
		}
		stateSvc := state.NewService(id, raftNode, layer)
		stateSvc.Register(mux)
		svc = stateSvc
		rt.raftNodes = append(rt.raftNodes, raftNode)

	case cluster.RoleShardStorage:
		// The store expects its directory to exist.
		if err := os.MkdirAll(dataDir, 0750); err != nil {
			return fmt.Errorf("create shard store directory %s: %w", dataDir, err)
		}
		st, err := store.Open(dataDir, storeOpts...)
		if err != nil {
			return fmt.Errorf("open shard store for node %d: %w", n.ID, err)
		}
		storageSvc := storage.NewService(id, st)
		storageSvc.Register(mux)
		svc = storageSvc

	default:
		return fmt.Errorf("node %d has unknown role %q", n.ID, n.Role)
	}

	srv, err := rpc.NewServer(rpc.ServerConfig{
		Mux:        mux,
		Addrs:      addrs,
		Transports: rt.trs,
	})
	if err != nil {
		return fmt.Errorf("serve node %d: %w", n.ID, err)
	}

	rt.nodes = append(rt.nodes, node{id: n.ID, svc: svc, srv: srv})
	return nil
}

// buildBackend wires the hash ring over the cluster's storage nodes; the
// injected clients make addressing the transports' concern.
func (rt *Runtime) buildBackend(
	cfg *s3.Config,
	topo *cluster.Topology,
	stateClient *state.Client,
	shardClient *storage.Client,
) (backend.Backend, error) {
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
		return nil, fmt.Errorf("create distributed backend: %w", err)
	}
	return be, nil
}

// Run serves every node until ctx is cancelled, then drains. Each node's rpc
// server and service share the process context, so one signal stops them all.
func (rt *Runtime) Run(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)
	for _, n := range rt.nodes {
		g.Go(func() error {
			if err := n.srv.Run(gctx); err != nil {
				return fmt.Errorf("node %d rpc server: %w", n.id, err)
			}
			return nil
		})
		g.Go(func() error {
			if err := n.svc.Run(gctx); err != nil {
				return fmt.Errorf("node %d service: %w", n.id, err)
			}
			return nil
		})
	}

	err := g.Wait()
	rt.Close()
	return err
}

// WaitReady blocks until consensus has a leader, so callers do not serve S3
// traffic into a cluster that cannot commit yet. Election needs the rpc
// servers running, so this is only meaningful once Run has started.
//
// A cluster with no replicas is ready immediately. A slow election degrades
// rather than fails: the state client retries, so callers may proceed on a
// timeout with a warning.
func (rt *Runtime) WaitReady(timeout time.Duration) error {
	if len(rt.raftNodes) == 0 {
		return nil
	}
	return rt.raftNodes[0].WaitForLeader(timeout)
}

// Close releases the process-wide resources the nodes share. Node state is
// closed by each service's Run as it returns.
func (rt *Runtime) Close() {
	if rt.client != nil {
		rt.client.Close()
	}
	for _, tr := range rt.trs {
		if c, ok := tr.(interface{ Close() error }); ok {
			c.Close()
		}
	}
}
