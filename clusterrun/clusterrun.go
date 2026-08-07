// Package clusterrun assembles the per-process cluster runtime: one transport
// per network, and for every node this process runs, a service and the rpc
// server carrying it. cmd/s3d is a thin entrypoint over Build and Run.
//
// It is public because s3d is not the only entrypoint: embedders that host
// predastore in their own process (spinifex's service supervisor) need the
// same assembly before handing the backend to s3.WithPreparedBackend.
package clusterrun

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/backend/distributed"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/state"
	"github.com/mulgadc/predastore/internal/storage"
	"github.com/mulgadc/predastore/internal/topology"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/mulgadc/predastore/pkg/masterkey"
	"github.com/mulgadc/predastore/s3"
	"github.com/mulgadc/predastore/s3db"
	"github.com/mulgadc/predastore/store"
	"golang.org/x/sync/errgroup"
)

// The topology is what rpc translates node ids through. The assertion lives
// here, where the two are wired together, so internal/topology stays free of
// any dependency on rpc.
var _ rpc.Topology = (*topology.Topology)(nil)

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

	nodes    []node
	client   *rpc.Client
	trs      []transport.Transport
	basePath string
	// raftNodes back WaitReady; consensus is what the S3 frontend waits on.
	raftNodes []*s3db.RaftNode
}

// raftPeers maps state replica node ids to the raft members they become. The
// address is the node key the stream layer resolves through the topology, so
// it stays valid however the replica is reached.
func raftPeers(replicaIDs []int) []s3db.RaftPeer {
	peers := make([]s3db.RaftPeer, len(replicaIDs))
	for i, id := range replicaIDs {
		u := uint64(id) //nolint:gosec // G115: node ids are small positives from a validated topology.
		peers[i] = s3db.RaftPeer{ID: u, Address: state.RaftAddress(u)}
	}
	return peers
}

// AllNodeIDs returns every node id in the topology: the selection for a
// process that runs the whole cluster over the in-process pipe.
func AllNodeIDs(cfg *s3.Config) []int {
	ids := make([]int, len(cfg.ClusterNodes))
	for i, n := range cfg.ClusterNodes {
		ids[i] = n.ID
	}
	return ids
}

// NodeIDsForHost returns the ids of the nodes pinned to hostID: the selection
// for the process that owns that host's socket and data directory. Callers
// select nodes by host rather than by id because the host is the unit an
// operator places, and everything per-node derives from it.
func NodeIDsForHost(cfg *s3.Config, hostID int) []int {
	var ids []int
	for _, n := range cfg.ClusterNodes {
		if n.HostID == hostID {
			ids = append(ids, n.ID)
		}
	}
	return ids
}

// Build assembles the process for the selected nodes. Every node gets its own
// service and rpc server; whether a peer is reached over the pipe or the
// network follows from its address, so nothing below here branches on it.
func Build(cfg *s3.Config, localIDs []int, tlsCert, tlsKey string, key *masterkey.Key) (*Runtime, error) {
	topo, err := topology.NewTopology(cfg.Hosts, cfg.ClusterNodes, localIDs)
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
		trs:      trs,
		client:   rpc.NewClient(rpc.ClientConfig{Transports: trs, Topology: topo}),
		basePath: cfg.BasePath,
	}

	// Raft dials peers through the same client; its advertise addresses are
	// node keys, so dialing one is parsing out the node id and opening a
	// stream to it.
	raftDial := func(ctx context.Context, address raft.ServerAddress) (transport.Stream, error) {
		target, err := state.ParseRaftAddress(string(address))
		if err != nil {
			return nil, err
		}
		nodeID := int(target) //nolint:gosec // G115: node ids are small positives from a validated topology.
		return rpc.OpenStream(ctx, rt.client, nodeID, state.OpRaftDial, &state.RaftDial{})
	}

	replicas := topo.NodesByRole(topology.RoleStateReplica)
	replicaIDs := make([]int, len(replicas))
	for i, n := range replicas {
		replicaIDs[i] = n.ID
	}
	peers := raftPeers(replicaIDs)
	replicaNodeIDs := make([]uint64, len(peers))
	for i, p := range peers {
		replicaNodeIDs[i] = p.ID
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
		Client:   rt.client,
		Replicas: replicaNodeIDs,
	})
	if err != nil {
		rt.Close()
		return nil, err
	}
	shardClient, err := storage.NewClient(storage.ClientConfig{Client: rt.client})
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
	topo *topology.Topology,
	n topology.Node,
	raftDial func(context.Context, raft.ServerAddress) (transport.Stream, error),
	peers []s3db.RaftPeer,
	storeOpts []store.Option,
) error {
	id := uint64(n.ID) //nolint:gosec // G115: validated positive node ids.

	// A relative data_dir is resolved against -base-path, so a config can be
	// shared across machines and the launcher decides where state lands.
	dataDir := topo.DataDir(n.ID)
	if !filepath.IsAbs(dataDir) && rt.basePath != "" {
		dataDir = filepath.Join(rt.basePath, dataDir)
	}

	mux := rpc.NewMux()
	var svc interface{ Run(context.Context) error }

	switch n.Role {
	case topology.RoleStateReplica:
		layer := s3db.NewRPCStreamLayer(state.RaftAddress(id), raftDial)
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

	case topology.RoleShardStorage:
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
		NodeID:     n.ID,
		Topology:   topo,
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
	topo *topology.Topology,
	stateClient *state.Client,
	shardClient *storage.Client,
) (backend.Backend, error) {
	storageNodes := topo.NodesByRole(topology.RoleShardStorage)
	beNodes := make([]int, len(storageNodes))
	for i, n := range storageNodes {
		beNodes[i] = n.ID
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
		StorageNodes: beNodes,
		Buckets:      beBuckets,
		State:        stateClient,
		Storage:      shardClient,
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
