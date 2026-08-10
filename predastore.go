// Package predastore is an S3-compatible object store: a Raft-replicated
// metadata plane, erasure-coded shard storage, and an S3 gate in front of
// both. It is the module's whole public surface — Config and LoadConfig for
// the on-disk configuration, Options and Run to serve a host until the context
// is cancelled.
//
// One process runs one host: the cluster nodes pinned to it in the
// configuration, the S3 gate among them. Those nodes talk over an
// in-process pipe; nodes on other hosts are reached over QUIC. A cluster whose
// nodes all sit on one host is therefore a single-process deployment, with no
// code path of its own.
package predastore

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/blob/engine"
	"github.com/mulgadc/predastore/internal/gate"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/mulgadc/predastore/pkg/masterkey"
	"golang.org/x/sync/errgroup"
)

// leaderWait bounds how long the gate holds off serving while consensus
// settles. Exceeding it is a warning, not a failure: the state client retries.
const leaderWait = 30 * time.Second

// Options is everything a predastore process needs that does not come from the
// configuration file: which host to run and the key that protects data at
// rest.
type Options struct {
	// Config is the parsed configuration file. Required.
	Config *Config

	// HostID is the [[host]] this process runs. It selects the nodes pinned to
	// that host and the addresses they are reached on. Required.
	HostID HostID

	// MasterKey is the AES-256 key shards are encrypted with at rest.
	// Required: predastore never writes plaintext shards.
	MasterKey *masterkey.Key

	// Debug forces debug logging on regardless of the config file.
	Debug bool

	// Pprof writes a CPU profile for the lifetime of Run, saved to PprofPath.
	Pprof     bool
	PprofPath string
}

// leaderBarrier holds the gate off until local consensus settles: serving
// before it does would fail writes that would have succeeded a moment later.
// Local meta replicas open it, and a host running none starts it open.
type leaderBarrier struct {
	open func()
	wait <-chan struct{}
}

func newLeaderBarrier() leaderBarrier {
	open := make(chan struct{})
	return leaderBarrier{open: sync.OnceFunc(func() { close(open) }), wait: open}
}

// Run serves one host until ctx is cancelled or one of its nodes fails, then
// drains everything it started. Every node shares the one context, so a single
// signal stops the lot; there is nothing else to stop it with.
func Run(ctx context.Context, opts Options) error {
	if opts.Config == nil {
		return fmt.Errorf("predastore: Options.Config is required")
	}
	if opts.MasterKey == nil {
		return fmt.Errorf("predastore: Options.MasterKey is required")
	}

	cfg := opts.Config
	local := localNodes(cfg, opts.HostID)
	if len(local) == 0 {
		return fmt.Errorf("predastore: host %d runs no nodes", opts.HostID)
	}

	// A host with no replica of its own has no local consensus to wait on, so
	// its gate serves immediately.
	barrier := newLeaderBarrier()
	if !slices.ContainsFunc(local, func(n NodeConfig) bool { return n.Role == RoleMeta }) {
		barrier.open()
	}

	// Every node is built before any of them starts. A node that dialed a
	// colocated peer first would otherwise find no listener registered for it.
	runs := make([]func(context.Context) error, 0, len(local))
	cleanups := make([]func(), 0, len(local))
	defer func() {
		for _, cleanup := range cleanups {
			cleanup()
		}
	}()
	for _, n := range local {
		run, cleanup, err := buildNode(cfg, n, opts, barrier)
		if err != nil {
			return err
		}
		runs = append(runs, run)
		cleanups = append(cleanups, cleanup)
	}

	g, gctx := errgroup.WithContext(ctx)
	for _, run := range runs {
		g.Go(func() error { return run(gctx) })
	}
	return g.Wait()
}

// buildNode builds one node of this host: the transports it is reached over,
// the rpc plumbing around them and the service it runs. Nothing listens or
// dials until run is called, and cleanup releases what run does not.
func buildNode(cfg *Config, n NodeConfig, opts Options, barrier leaderBarrier) (
	run func(context.Context) error, cleanup func(), err error,
) {
	host, ok := hostOf(cfg, n.HostID)
	if !ok {
		return nil, nil, fmt.Errorf("node %d references unknown host %d", n.ID, n.HostID)
	}

	// A gate's port is its S3 port, so its rpc sockets bind ephemerally.
	port := n.Port
	if n.Role == RoleGate {
		port = 0
	}

	// The pipe carries traffic between colocated nodes and the QUIC socket
	// traffic to other hosts, so each exists only when the cluster puts a peer
	// on the other end of it.
	var trs []transport.Transport
	if len(localNodes(cfg, host.ID)) > 1 {
		trs = append(trs, transport.NewPipeTransport(host.PublicAddr, port))
	}
	if hasRemoteNodes(cfg, host.ID) {
		quic, qerr := transport.NewQUICTransport(host.BindAddr, port, host.TLSCert, host.TLSKey)
		if qerr != nil {
			return nil, nil, fmt.Errorf("node %d quic transport: %w", n.ID, qerr)
		}
		trs = append(trs, quic)
	}

	// pool is set by the roles that dial; closing it and the transports is all
	// a node leaves behind, since each service closes its own meta.
	var pool *rpc.ConnPool
	cleanup = func() {
		if pool != nil {
			_ = pool.Close()
		}
		for _, tr := range trs {
			_ = tr.Close()
		}
	}

	res, err := rpc.NewResolver(cfg, n.ID, trs...)
	if err != nil {
		cleanup()
		return nil, nil, err
	}

	// Nothing dials a gate, so it listens on nothing: the S3 frontend is
	// its only listener.
	var lns []transport.Listener
	if n.Role != RoleGate {
		for _, tr := range trs {
			ln, lerr := tr.Listen()
			if lerr != nil {
				cleanup()
				return nil, nil, fmt.Errorf("node %d listen on %s: %w", n.ID, tr.Network(), lerr)
			}
			lns = append(lns, ln)
		}
	}

	mux := rpc.NewMux()
	dir := dataDir(cfg, n.ID)
	var serve func(context.Context) error

	switch n.Role {
	case RoleGate:
		// The pool is private: a gate dials but is never dialed, so it has
		// nothing to share one with.
		pool = rpc.NewConnPool(n.ID, res)
		gw, gerr := gateServer(cfg, n, host, opts, rpc.NewClient(pool))
		if gerr != nil {
			cleanup()
			return nil, nil, fmt.Errorf("create s3 gate: %w", gerr)
		}
		serve = func(ctx context.Context) error {
			select {
			case <-barrier.wait:
			case <-ctx.Done():
			}
			return gw.Run(ctx)
		}

	case RoleBlob:
		// The store expects its directory to exist.
		if mkErr := os.MkdirAll(dir, 0750); mkErr != nil {
			cleanup()
			return nil, nil, fmt.Errorf("create shard store directory %s: %w", dir, mkErr)
		}
		// Compaction is always enabled: without it, overwrite and delete churn
		// never reclaims dead shards and the store fills. A zero interval falls
		// back to the store's default.
		st, oerr := engine.Open(dir,
			engine.WithAEAD(opts.MasterKey.AEAD),
			engine.WithCompaction(time.Duration(cfg.Compaction.IntervalSeconds)*time.Second))
		if oerr != nil {
			cleanup()
			return nil, nil, fmt.Errorf("open shard store for node %d: %w", n.ID, oerr)
		}
		svc := blob.NewServer(n.ID, st)
		svc.Register(mux)
		serve = svc.Run

	case RoleMeta:
		// One pool between the client and the server: a replica both dials its
		// peers and is dialed by them, so a connection serves either direction.
		pool = rpc.NewConnPool(n.ID, res)
		ccfg := meta.DefaultClusterConfig()
		ccfg.NodeID = n.ID
		ccfg.DataDir = dir
		// Bootstrapping with an identical peer set is idempotent across
		// replicas, so every replica may attempt it.
		ccfg.Bootstrap = true
		ccfg.StreamLayer = meta.NewRPCStreamLayer(meta.RaftAddress(n.ID), raftDial(rpc.NewClient(pool)))
		ccfg.Peers = raftPeers(nodesByRole(cfg, RoleMeta))
		replica, rerr := meta.NewServer(ccfg)
		if rerr != nil {
			cleanup()
			return nil, nil, fmt.Errorf("start meta replica %d: %w", n.ID, rerr)
		}
		replica.Register(mux)
		serve = func(ctx context.Context) error {
			// The barrier opens however the election goes: a slow one warns and
			// the gate serves anyway rather than never serving.
			go func() {
				if werr := replica.WaitForLeader(leaderWait); werr != nil {
					slog.Warn("No leader elected within timeout, serving anyway", "error", werr)
				}
				barrier.open()
			}()
			return replica.Run(ctx)
		}

	default:
		cleanup()
		return nil, nil, fmt.Errorf("node %d has unknown role %q", n.ID, n.Role)
	}

	// A blob node never dials, so it donates to no pool; a replica donates
	// to the one it dials from. Both are the same call with a different pool.
	var srv *rpc.Server
	if n.Role != RoleGate {
		srv, err = rpc.NewServer(mux, lns, pool)
		if err != nil {
			cleanup()
			return nil, nil, fmt.Errorf("serve node %d: %w", n.ID, err)
		}
	}

	run = func(ctx context.Context) error {
		g, gctx := errgroup.WithContext(ctx)
		if srv != nil {
			g.Go(func() error { return srv.Run(gctx) })
		}
		g.Go(func() error { return serve(gctx) })
		if err := g.Wait(); err != nil {
			return fmt.Errorf("node %d: %w", n.ID, err)
		}
		return nil
	}
	return run, cleanup, nil
}

// raftDial opens a raft connection through the replica's own client. Raft
// advertises node keys as addresses, so dialing one is parsing out the node id
// and opening a stream to it.
func raftDial(cli *rpc.Client) func(context.Context, raft.ServerAddress) (transport.Stream, error) {
	return func(ctx context.Context, address raft.ServerAddress) (transport.Stream, error) {
		target, err := meta.ParseRaftAddress(string(address))
		if err != nil {
			return nil, err
		}
		return rpc.OpenStream(ctx, cli, target, meta.OpRaftDial, &meta.RaftDial{})
	}
}

// gateServer builds the S3 frontend a gate node runs: the file's gate
// slice, its host's listen address and TLS identity, and the cluster clients
// it works through.
func gateServer(
	cfg *Config, n NodeConfig, host HostConfig, opts Options, cli *rpc.Client,
) (*gate.Server, error) {
	metaClient, err := meta.NewClient(meta.ClientConfig{
		Client:   cli,
		Replicas: nodeIDs(nodesByRole(cfg, RoleMeta)),
	})
	if err != nil {
		return nil, err
	}
	blobClient, err := blob.NewClient(blob.ClientConfig{Client: cli})
	if err != nil {
		return nil, err
	}
	return gate.NewServer(gate.ServerConfig{
		Config:          gateConfig(cfg, opts.Debug),
		Host:            host.BindAddr,
		Port:            n.Port,
		TLSCert:         host.TLSCert,
		TLSKey:          host.TLSKey,
		MasterKey:       opts.MasterKey,
		Clients:         gate.Clients{Meta: metaClient, Blob: blobClient},
		PprofEnabled:    opts.Pprof,
		PprofOutputPath: opts.PprofPath,
	})
}

// raftPeers maps meta replicas to the raft members they become. The address
// is the node key the stream layer resolves, so it stays valid however the
// replica is reached.
func raftPeers(replicas []NodeConfig) []meta.RaftPeer {
	peers := make([]meta.RaftPeer, len(replicas))
	for i, n := range replicas {
		peers[i] = meta.RaftPeer{ID: n.ID, Address: meta.RaftAddress(n.ID)}
	}
	return peers
}
