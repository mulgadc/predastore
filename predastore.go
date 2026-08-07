// Package predastore is an S3-compatible object store: a Raft-replicated
// metadata plane, erasure-coded shard storage, and an S3 gateway in front of
// both. It is the module's whole public surface — Config and LoadConfig for
// the on-disk configuration, Options and New to build a process, and Run to
// serve it until the context is cancelled.
//
// One process runs one host: the cluster nodes pinned to it in the
// configuration. Those nodes talk over an in-process pipe; nodes on other
// hosts are reached over QUIC. A cluster whose nodes all sit on one host is
// therefore a single-process deployment, with no code path of its own.
package predastore

import (
	"context"
	"fmt"
	"log/slog"
	"net"
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
// configuration file: which host to run, where to serve S3, and the key that
// protects data at rest.
type Options struct {
	// Config is the parsed configuration file. Required.
	Config *Config

	// HostID is the [[host]] this process runs. It selects the nodes pinned to
	// that host and the address they are reached on. Required.
	HostID int

	// Port is the S3 listen port; zero defaults to 8443. The address it binds
	// is the host's own, so a process serves S3 where it serves everything
	// else.
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

// node is one storage or state replica running in this process: a service
// serving its rpc endpoint, and the server carrying it.
type node struct {
	id  int
	svc interface{ Run(context.Context) error }
	srv *rpc.Server
}

// Host is a predastore process: the cluster nodes pinned to it and the S3
// gateway in front of them. New builds it, Run serves it, and cancelling Run's
// context is the only way to stop it.
type Host struct {
	nodes   []node
	gateway *gateway.Server

	client *rpc.Client
	trs    []transport.Transport
	// replicas back the leader wait; consensus is what the gateway waits on.
	replicas []*state.Server
}

// New assembles the process for one host. Every node pinned to it gets its own
// service and rpc server; whether a peer is reached over the pipe or the
// network follows from its address, so nothing below here branches on it.
//
// Nothing listens until Run is called.
func New(opts Options) (*Host, error) {
	if opts.Config == nil {
		return nil, fmt.Errorf("predastore: Options.Config is required")
	}
	if opts.MasterKey == nil {
		return nil, fmt.Errorf("predastore: Options.MasterKey is required")
	}

	cfg := opts.Config

	basePath, err := cfg.basePath()
	if err != nil {
		return nil, err
	}

	topo, err := topology.NewTopology(cfg.Hosts, cfg.Nodes, opts.HostID)
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

	h := &Host{
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
		return rpc.OpenStream(ctx, h.client, nodeID, state.OpRaftDial, &state.RaftDial{})
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
		if err := h.addNode(topo, local, basePath, raftDial, peers, storeOpts); err != nil {
			h.close()
			return nil, err
		}
	}

	stateClient, err := state.NewClient(state.ClientConfig{Client: h.client, Replicas: replicaIDs})
	if err != nil {
		h.close()
		return nil, err
	}
	shardClient, err := storage.NewClient(storage.ClientConfig{Client: h.client})
	if err != nil {
		h.close()
		return nil, err
	}

	h.gateway, err = gateway.NewServer(gateway.ServerConfig{
		Config:          cfg.gatewayConfig(basePath, opts.Debug),
		Host:            gatewayHost(topo.LocalHost()),
		Port:            opts.Port,
		TLSCert:         opts.TLSCert,
		TLSKey:          opts.TLSKey,
		MasterKey:       opts.MasterKey,
		Clients:         gateway.Clients{State: stateClient, Storage: shardClient},
		PprofEnabled:    opts.Pprof,
		PprofOutputPath: opts.PprofPath,
	})
	if err != nil {
		h.close()
		return nil, fmt.Errorf("create s3 gateway: %w", err)
	}

	return h, nil
}

// gatewayHost is the address the S3 frontend binds: the host's own, so one
// process serves S3 and inter-node traffic on the same interface. BindAddr
// carries the inter-node port, which the gateway does not share.
func gatewayHost(local topology.Host) string {
	if addr, _, err := net.SplitHostPort(local.BindAddr); err == nil {
		return addr
	}
	return local.BindAddr
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
func (h *Host) addNode(
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
		h.replicas = append(h.replicas, replica)

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
		Transports: h.trs,
	})
	if err != nil {
		return fmt.Errorf("serve node %d: %w", local.ID, err)
	}

	h.nodes = append(h.nodes, node{id: local.ID, svc: svc, srv: srv})
	return nil
}

// Run serves the process until ctx is cancelled or one of its parts fails,
// then drains everything it started. Every node's rpc server, every node's
// service and the S3 gateway share one context, so a single signal stops the
// lot; there is nothing else to stop it with.
func (h *Host) Run(ctx context.Context) error {
	defer h.close()

	g, gctx := errgroup.WithContext(ctx)
	for _, local := range h.nodes {
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
		if err := h.waitForLeader(gctx); err != nil {
			slog.Warn("No leader elected within timeout, serving anyway", "error", err)
		}
		return h.gateway.Run(gctx)
	})

	return g.Wait()
}

// waitForLeader blocks until consensus has a leader, the timeout expires, or
// ctx is cancelled. Election needs the rpc servers running, so this is only
// meaningful once Run has started them. A cluster with no local replica has
// nothing to wait on here and proceeds immediately.
func (h *Host) waitForLeader(ctx context.Context) error {
	if len(h.replicas) == 0 {
		return nil
	}

	done := make(chan error, 1)
	go func() { done <- h.replicas[0].WaitForLeader(leaderWait) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// close releases the process-wide resources the nodes share. Per-node state is
// closed by each service's Run as it returns.
func (h *Host) close() {
	if h.client != nil {
		h.client.Close()
	}
	for _, tr := range h.trs {
		if c, ok := tr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}
}
