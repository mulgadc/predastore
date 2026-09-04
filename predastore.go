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
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
	"github.com/mulgadc/predastore/internal/admin"
	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
	"golang.org/x/sync/errgroup"
)

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
		return fmt.Errorf("Options.Config is required")
	}
	if opts.MasterKey == nil {
		return fmt.Errorf("Options.MasterKey is required")
	}

	cfg := opts.Config
	host, ok := hostOf(cfg, opts.HostID)
	if !ok {
		return fmt.Errorf("host %d is not in the configuration", opts.HostID)
	}
	if len(host.Nodes) == 0 {
		return fmt.Errorf("host %d runs no nodes", opts.HostID)
	}
	// A node that names no directory of its own derives one under the host
	// root, and an empty root derives a relative path under whatever directory
	// the process was started in.
	if host.DataDir == "" && slices.ContainsFunc(host.Nodes, func(n NodeConfig) bool {
		return n.Role != RoleGate && n.DataDir == ""
	}) {
		return fmt.Errorf("host %d has no data directory", opts.HostID)
	}

	// A host with no replica of its own has no local consensus to wait on, so
	// its gate serves immediately.
	barrier := newLeaderBarrier()
	if !slices.ContainsFunc(host.Nodes, func(n NodeConfig) bool { return n.Role == RoleMeta }) {
		barrier.open()
	}

	// Every node is built before any of them starts. A node that dialed a
	// colocated peer first would otherwise find no listener registered for it.
	runs := make([]func(context.Context) error, 0, len(host.Nodes)+1)
	cleanups := make([]func(), 0, len(host.Nodes))
	var checks []admin.Check
	defer func() {
		for _, cleanup := range cleanups {
			cleanup()
		}
	}()
	for _, n := range host.Nodes {
		built, err := buildNode(cfg, host, n, opts, barrier)
		if err != nil {
			return err
		}
		runs = append(runs, built.run)
		cleanups = append(cleanups, built.cleanup)
		checks = append(checks, built.checks...)
	}

	// The admin listener binds the cluster plane, never the gate's S3 address:
	// health is operator traffic and that address is public by design.
	if host.AdminPort != 0 {
		addr := net.JoinHostPort(config.HostBindAddr(host), strconv.Itoa(host.AdminPort))
		runs = append(runs, admin.New(addr, checks).Run)
	}

	g, gctx := errgroup.WithContext(ctx)
	for _, run := range runs {
		g.Go(func() error { return run(gctx) })
	}
	return g.Wait()
}

// probeKey is a key nothing writes. A probe wants an answer rather than data,
// and "no such object" is an answer only a working node can give.
const probeKey = "predastore/readyz-probe"

var probeShardKey [32]byte

// writeShardFloor is how many shards a write has to place before it is
// acknowledged, which is every shard unless degraded writes are on.
func writeShardFloor(cfg *Config) int {
	if cfg.RS.DegradedWritesEnabled() {
		return cfg.RS.Data
	}
	return cfg.RS.Data + cfg.RS.Parity
}

// probeReader is the meta read a readiness probe needs, and shardProber the
// blob one. Both are narrowed to what the check calls so a probe can be tested
// without a cluster behind it.
type probeReader interface {
	Get(ctx context.Context, key string) ([]byte, error)
}

type leaderReporter interface {
	LeaderKnown() bool
}

type shardProber interface {
	Get(ctx context.Context, node NodeID, req blob.GetRequest) (io.ReadCloser, error)
}

// leaderObserved fails while this replica sees no leader. Writes cannot commit
// without one, so a gate in front of it would accept requests it cannot serve.
func leaderObserved(svc leaderReporter) admin.Check {
	return admin.Check{
		Name: "meta_leader",
		Probe: func(context.Context) error {
			if !svc.LeaderKnown() {
				return errors.New("no raft leader observed")
			}
			return nil
		},
	}
}

// metaReachable asks the meta plane for a key that is not there. A missing key
// is the healthy answer: the read reached a replica and came back.
func metaReachable(mc probeReader) admin.Check {
	return admin.Check{
		Name: "meta_reachable",
		Probe: func(ctx context.Context) error {
			if _, err := mc.Get(ctx, probeKey); err != nil && !errors.Is(err, meta.ErrNotFound) {
				return err
			}
			return nil
		},
	}
}

// blobNodesReachable fails when fewer blob nodes answer than a write needs
// shards durable.
//
// The write floor, not the read one. Reads reconstruct from DataShards and so
// survive losing parity, which is why a gate that cannot write at all still
// answers every read it is asked for. Gating readiness on the read floor made
// that gate report ready, and the only cluster-wide symptom of a store that
// had stopped accepting writes was in its log.
func blobNodesReachable(bc shardProber, nodes []NodeID, need int) admin.Check {
	return admin.Check{
		Name: "blob_nodes",
		Probe: func(ctx context.Context) error {
			var reached atomic.Int64
			var wg sync.WaitGroup
			for _, id := range nodes {
				wg.Go(func() {
					if blobAnswers(ctx, bc, id) {
						reached.Add(1)
					}
				})
			}
			wg.Wait()

			if got := int(reached.Load()); got < need {
				return fmt.Errorf("%d of %d blob nodes answered, need %d", got, len(nodes), need)
			}
			return nil
		},
	}
}

// blobNodeReachable names one blob node in the probe response. Advisory: the
// cluster tolerates losing a node, so one being gone is not unreadiness, but
// which one it is has to be answerable without reading a gate's log.
func blobNodeReachable(bc shardProber, id NodeID) admin.Check {
	return admin.Check{
		Name:     fmt.Sprintf("blob_node_%d", id),
		Advisory: true,
		Probe: func(ctx context.Context) error {
			if !blobAnswers(ctx, bc, id) {
				return fmt.Errorf("blob node %d did not answer", id)
			}
			return nil
		},
	}
}

// blobAnswers reports whether a blob node served a read at all. A node that
// says the shard is missing has opened its store and answered, which is the
// whole question.
func blobAnswers(ctx context.Context, bc shardProber, id NodeID) bool {
	body, err := bc.Get(ctx, id, blob.GetRequest{Key: probeShardKey, RangeStart: -1, RangeEnd: -1})
	if err == nil {
		_ = body.Close()
		return true
	}
	return errors.Is(err, blob.ErrNotFound)
}

// builtNode is one node's lifecycle and the readiness questions it can answer.
// A node contributes only what it holds a client or a replica for, so a probe
// describes the process it reached rather than the cluster as a whole.
type builtNode struct {
	run     func(context.Context) error
	cleanup func()
	checks  []admin.Check
}

// buildNode builds one node of this host: the transports it is reached over,
// the rpc plumbing around them and the service it runs. Nothing listens or
// dials until run is called, and cleanup releases what run does not.
func buildNode(cfg *Config, host HostConfig, n NodeConfig, opts Options, barrier leaderBarrier) (
	built builtNode, err error,
) {
	// A gate's port is its S3 port, so its rpc sockets bind ephemerally.
	port := n.Port
	if n.Role == RoleGate {
		port = 0
	}

	// The pipe carries traffic between colocated nodes and the QUIC socket
	// traffic to other hosts, so each exists only when the cluster puts a peer
	// on the other end of it. Both are the cluster plane and bind the host's
	// address; only the gate's S3 listener may be given a public one.
	var trs []transport.Transport
	if len(host.Nodes) > 1 {
		trs = append(trs, transport.NewPipeTransport(host.Addr, port))
	}
	if hasRemoteNodes(cfg, host.ID) {
		quic, qerr := transport.NewQUICTransport(config.HostBindAddr(host), port, host.TLSCert, host.TLSKey)
		if qerr != nil {
			return builtNode{}, fmt.Errorf("node %d quic transport: %w", n.ID, qerr)
		}
		trs = append(trs, quic)
	}

	// pool is set by the gate, the one role that dials without running a
	// server of its own; closing it and the transports is all a node leaves
	// behind, since every service releases what it opened.
	var pool *rpc.ConnPool
	cleanup := func() {
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
		return builtNode{}, err
	}

	// Nothing dials a gate, so it listens on nothing: the S3 frontend is
	// its only listener.
	var lns []transport.Listener
	if n.Role != RoleGate {
		for _, tr := range trs {
			ln, lerr := tr.Listen()
			if lerr != nil {
				cleanup()
				return builtNode{}, fmt.Errorf("node %d listen on %s: %w", n.ID, tr.Network(), lerr)
			}
			lns = append(lns, ln)
		}
	}

	dir := config.NodeDataDir(host, n)
	var serve func(context.Context) error
	var checks []admin.Check

	switch n.Role {
	case RoleGate:
		// The pool is private: a gate dials but is never dialed, so it has
		// nothing to share one with.
		pool = rpc.NewConnPool(n.ID, res)
		cli := rpc.NewClient(pool)
		metaClient, cerr := meta.NewClient(meta.ClientConfig{
			Client:   cli,
			Replicas: nodeIDs(nodesByRole(cfg, RoleMeta)),
		})
		if cerr != nil {
			cleanup()
			return builtNode{}, fmt.Errorf("create s3 gate: %w", cerr)
		}
		blobClient, cerr := blob.NewClient(blob.ClientConfig{Client: cli})
		if cerr != nil {
			cleanup()
			return builtNode{}, fmt.Errorf("create s3 gate: %w", cerr)
		}
		gw, gerr := gate.New(gateConfig(cfg, host, n, metaClient, blobClient))
		if gerr != nil {
			cleanup()
			return builtNode{}, fmt.Errorf("create s3 gate: %w", gerr)
		}
		serve = func(ctx context.Context) error {
			select {
			case <-barrier.wait:
			case <-ctx.Done():
			}
			return gw.Run(ctx)
		}
		// The gate is the one role holding a client for each plane, so it is
		// where reaching them can be asked about at all.
		blobIDs := nodeIDs(nodesByRole(cfg, RoleBlob))
		checks = append(checks, metaReachable(metaClient),
			blobNodesReachable(blobClient, blobIDs, writeShardFloor(cfg)))
		for _, id := range blobIDs {
			checks = append(checks, blobNodeReachable(blobClient, id))
		}

	case RoleBlob:
		// The node owns its store: it creates the directory, opens the engine
		// and closes it again, so no failure here can leak one.
		svc, berr := blob.New(blob.Config{
			NodeID:     n.ID,
			DataDir:    dir,
			AEAD:       opts.MasterKey.AEAD,
			Compaction: time.Duration(cfg.Compaction.IntervalSeconds) * time.Second,
			Listeners:  lns,
		})
		if berr != nil {
			cleanup()
			return builtNode{}, fmt.Errorf("create blob node %d: %w", n.ID, berr)
		}
		serve = svc.Run

	case RoleMeta:
		// The replica owns its raft node and the pool it is both dialed
		// through and dials its peers with, so no failure here can leak one.
		// The barrier opens however the election goes: a slow one still
		// releases the gate rather than never serving.
		svc, merr := meta.New(meta.Config{
			NodeID:    n.ID,
			DataDir:   dir,
			Peers:     nodeIDs(nodesByRole(cfg, RoleMeta)),
			Bootstrap: true,
			Listeners: lns,
			Resolver:  res,
			OnLeader:  barrier.open,

			SnapshotInterval:  time.Duration(cfg.Meta.SnapshotIntervalSeconds) * time.Second,
			SnapshotThreshold: cfg.Meta.SnapshotThreshold,
			TrailingLogs:      cfg.Meta.TrailingLogs,
		})
		if merr != nil {
			cleanup()
			return builtNode{}, fmt.Errorf("create meta replica %d: %w", n.ID, merr)
		}
		serve = svc.Run
		checks = append(checks, leaderObserved(svc))

	default:
		cleanup()
		return builtNode{}, fmt.Errorf("node %d has unknown role %q", n.ID, n.Role)
	}

	run := func(ctx context.Context) error {
		if err := serve(ctx); err != nil {
			return fmt.Errorf("node %d: %w", n.ID, err)
		}
		return nil
	}
	return builtNode{run: run, cleanup: cleanup, checks: checks}, nil
}
