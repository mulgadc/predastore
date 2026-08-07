// Package predastore is an S3-compatible object store: a Raft-replicated
// metadata plane, erasure-coded shard storage, and an S3 gateway in front of
// both. It is the module's whole public surface — Config and LoadConfig for
// the on-disk configuration, Options and New to build a process, and Run to
// serve it until the context is cancelled.
//
// One process runs any subset of the cluster's nodes. Nodes selected here talk
// over an in-process pipe; nodes running elsewhere are reached over QUIC. The
// selection is the only difference between a single-process deployment and a
// node of a distributed one.
package predastore

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	"github.com/mulgadc/predastore/internal/gateway"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/state"
	"github.com/mulgadc/predastore/internal/storage"
	"github.com/mulgadc/predastore/internal/storage/engine"
	"github.com/mulgadc/predastore/internal/topology"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/mulgadc/predastore/pkg/masterkey"
	"golang.org/x/sync/errgroup"
)

// rpc declares the two-method view of the topology it needs rather than
// importing topology, which keeps topology free of any dependency on rpc. This
// package is where the two are wired together, so it is where the
// implementation can be checked against the interface.
var _ rpc.Topology = (*topology.Topology)(nil)

// leaderWait bounds how long the gateway holds off serving while consensus
// settles. Exceeding it is a warning, not a failure: the state client retries.
const leaderWait = 30 * time.Second

// Options is everything a predastore process needs that does not come from the
// configuration file: which nodes to run, where to serve S3, and the key that
// protects data at rest.
type Options struct {
	// Config is the parsed configuration file. Required.
	Config *Config

	// LocalNodeIDs are the cluster nodes this process runs. Empty selects
	// every node in the topology, which runs the whole cluster in one process
	// over the pipe transport and opens no network socket.
	LocalNodeIDs []int

	// Host and Port are the S3 listen address. Zero values default to
	// 0.0.0.0:8443.
	Host string
	Port int

	// TLSCert and TLSKey serve the S3 gateway, and the inter-node QUIC socket
	// when some node runs elsewhere. Required in both those cases.
	TLSCert string
	TLSKey  string

	// MasterKey is the AES-256 key shards are encrypted with at rest.
	// Required: predastore never writes plaintext shards.
	MasterKey *masterkey.Key

	// Debug forces debug logging on regardless of the config file.
	Debug bool

	// Pprof writes a CPU profile for the lifetime of Run, saved to PprofPath.
	Pprof     bool
	PprofPath string
}

// localNode is one storage or state replica running in this process: a service
// serving its rpc endpoint, and the server carrying it.
type localNode struct {
	id  int
	svc interface{ Run(context.Context) error }
	srv *rpc.Server
}

// Node is a predastore process: the cluster nodes it runs and the S3 gateway
// in front of them. New builds it, Run serves it, and cancelling Run's context
// is the only way to stop it.
type Node struct {
	nodes   []localNode
	gateway *gateway.Server

	client *rpc.Client
	trs    []transport.Transport
	// replicas back the leader wait; consensus is what the gateway waits on.
	replicas []*state.Server
}

// New assembles the process for the selected nodes. Every node gets its own
// service and rpc server; whether a peer is reached over the pipe or the
// network follows from its address, so nothing below here branches on it.
//
// Nothing listens until Run is called.
func New(opts Options) (*Node, error) {
	if opts.Config == nil {
		return nil, fmt.Errorf("predastore: Options.Config is required")
	}
	if opts.MasterKey == nil {
		return nil, fmt.Errorf("predastore: Options.MasterKey is required")
	}

	cfg := opts.Config
	localIDs := opts.LocalNodeIDs
	if len(localIDs) == 0 {
		localIDs = cfg.AllNodeIDs()
	}

	basePath, err := cfg.basePath()
	if err != nil {
		return nil, err
	}

	topo, err := topology.NewTopology(cfg.topologyHosts(), cfg.topologyNodes(), localIDs)
	if err != nil {
		return nil, err
	}

	// One transport per network for the whole process. The pipe carries
	// in-process traffic; the QUIC socket only comes up when some node runs
	// elsewhere, so a single-process cluster needs no certificates.
	trs := []transport.Transport{transport.NewPipeTransport()}
	if topo.NeedsNetwork() {
		if opts.TLSCert == "" || opts.TLSKey == "" {
			return nil, fmt.Errorf("cluster has remote nodes: TLSCert and TLSKey are required")
		}
		trs = append(trs, transport.NewQUICTransport(transport.QUICTransportConfig{
			TLSCert: opts.TLSCert,
			TLSKey:  opts.TLSKey,
		}))
	}

	n := &Node{
		trs:    trs,
		client: rpc.NewClient(rpc.ClientConfig{Transports: trs, Topology: topo}),
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
		return rpc.OpenStream(ctx, n.client, nodeID, state.OpRaftDial, &state.RaftDial{})
	}

	peers := raftPeers(topo.NodesByRole(topology.RoleStateReplica))
	replicaIDs := make([]uint64, len(peers))
	for i, p := range peers {
		replicaIDs[i] = p.ID
	}

	// Compaction is always enabled: without it, overwrite and delete churn
	// never reclaims dead shards and the store fills. A zero interval falls
	// back to the store's default.
	storeOpts := []engine.Option{
		engine.WithAEAD(opts.MasterKey.AEAD),
		engine.WithCompaction(time.Duration(cfg.Compaction.IntervalSeconds) * time.Second),
	}

	for _, local := range topo.LocalNodes() {
		if err := n.addNode(topo, local, basePath, raftDial, peers, storeOpts); err != nil {
			n.close()
			return nil, err
		}
	}

	stateClient, err := state.NewClient(state.ClientConfig{Client: n.client, Replicas: replicaIDs})
	if err != nil {
		n.close()
		return nil, err
	}
	shardClient, err := storage.NewClient(storage.ClientConfig{Client: n.client})
	if err != nil {
		n.close()
		return nil, err
	}

	n.gateway, err = gateway.NewServer(gateway.ServerConfig{
		Config:          cfg.gatewayConfig(basePath, opts.Debug),
		Host:            opts.Host,
		Port:            opts.Port,
		TLSCert:         opts.TLSCert,
		TLSKey:          opts.TLSKey,
		MasterKey:       opts.MasterKey,
		Clients:         gateway.Clients{State: stateClient, Storage: shardClient},
		PprofEnabled:    opts.Pprof,
		PprofOutputPath: opts.PprofPath,
	})
	if err != nil {
		n.close()
		return nil, fmt.Errorf("create s3 gateway: %w", err)
	}

	return n, nil
}

// raftPeers maps state replicas to the raft members they become. The address
// is the node key the stream layer resolves through the topology, so it stays
// valid however the replica is reached.
func raftPeers(replicas []topology.Node) []state.RaftPeer {
	peers := make([]state.RaftPeer, len(replicas))
	for i, n := range replicas {
		id := uint64(n.ID) //nolint:gosec // G115: node ids are small positives from a validated topology.
		peers[i] = state.RaftPeer{ID: id, Address: state.RaftAddress(id)}
	}
	return peers
}

// addNode builds one node's service and the rpc server that carries it. The
// server is constructed but not started: handlers must be registered on every
// node before any of them accepts, or early peer traffic finds no handler.
func (n *Node) addNode(
	topo *topology.Topology,
	local topology.Node,
	basePath string,
	raftDial func(context.Context, raft.ServerAddress) (transport.Stream, error),
	peers []state.RaftPeer,
	storeOpts []engine.Option,
) error {
	id := uint64(local.ID) //nolint:gosec // G115: validated positive node ids.

	// A relative data_dir is resolved against the base path, so a config can
	// be shared across machines and the launcher decides where state lands.
	dataDir := topo.DataDir(local.ID)
	if !filepath.IsAbs(dataDir) {
		dataDir = filepath.Join(basePath, dataDir)
	}

	mux := rpc.NewMux()
	var svc interface{ Run(context.Context) error }

	switch local.Role {
	case topology.RoleStateReplica:
		ccfg := state.DefaultClusterConfig()
		ccfg.NodeID = id
		ccfg.DataDir = dataDir
		// Bootstrapping with an identical peer set is idempotent across
		// replicas, so every replica may attempt it.
		ccfg.Bootstrap = true
		ccfg.StreamLayer = state.NewRPCStreamLayer(state.RaftAddress(id), raftDial)
		ccfg.Peers = peers
		replica, err := state.NewServer(ccfg)
		if err != nil {
			return fmt.Errorf("start state replica %d: %w", local.ID, err)
		}
		replica.Register(mux)
		svc = replica
		n.replicas = append(n.replicas, replica)

	case topology.RoleShardStorage:
		// The store expects its directory to exist.
		if err := os.MkdirAll(dataDir, 0750); err != nil {
			return fmt.Errorf("create shard store directory %s: %w", dataDir, err)
		}
		st, err := engine.Open(dataDir, storeOpts...)
		if err != nil {
			return fmt.Errorf("open shard store for node %d: %w", local.ID, err)
		}
		storageSvc := storage.NewServer(id, st)
		storageSvc.Register(mux)
		svc = storageSvc

	default:
		return fmt.Errorf("node %d has unknown role %q", local.ID, local.Role)
	}

	srv, err := rpc.NewServer(rpc.ServerConfig{
		Mux:        mux,
		NodeID:     local.ID,
		Topology:   topo,
		Transports: n.trs,
	})
	if err != nil {
		return fmt.Errorf("serve node %d: %w", local.ID, err)
	}

	n.nodes = append(n.nodes, localNode{id: local.ID, svc: svc, srv: srv})
	return nil
}

// Run serves the process until ctx is cancelled or one of its parts fails,
// then drains everything it started. Every node's rpc server, every node's
// service and the S3 gateway share one context, so a single signal stops the
// lot; there is nothing else to stop it with.
func (n *Node) Run(ctx context.Context) error {
	defer n.close()

	g, gctx := errgroup.WithContext(ctx)
	for _, local := range n.nodes {
		g.Go(func() error {
			if err := local.srv.Run(gctx); err != nil {
				return fmt.Errorf("node %d rpc server: %w", local.id, err)
			}
			return nil
		})
		g.Go(func() error {
			if err := local.svc.Run(gctx); err != nil {
				return fmt.Errorf("node %d service: %w", local.id, err)
			}
			return nil
		})
	}

	g.Go(func() error {
		// Serving before consensus settles would fail writes that would have
		// succeeded a moment later. The wait is bounded and advisory: a slow
		// election degrades rather than aborts.
		if err := n.waitForLeader(gctx); err != nil {
			slog.Warn("No leader elected within timeout, serving anyway", "error", err)
		}
		return n.gateway.Run(gctx)
	})

	return g.Wait()
}

// waitForLeader blocks until consensus has a leader, the timeout expires, or
// ctx is cancelled. Election needs the rpc servers running, so this is only
// meaningful once Run has started them. A cluster with no local replica has
// nothing to wait on here and proceeds immediately.
func (n *Node) waitForLeader(ctx context.Context) error {
	if len(n.replicas) == 0 {
		return nil
	}

	done := make(chan error, 1)
	go func() { done <- n.replicas[0].WaitForLeader(leaderWait) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// close releases the process-wide resources the nodes share. Node state is
// closed by each service's Run as it returns.
func (n *Node) close() {
	if n.client != nil {
		n.client.Close()
	}
	for _, tr := range n.trs {
		if c, ok := tr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}
}
