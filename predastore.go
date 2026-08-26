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
	"database/sql/driver"
	"fmt"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
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
	runs := make([]func(context.Context) error, 0, len(host.Nodes))
	cleanups := make([]func(), 0, len(host.Nodes))
	defer func() {
		for _, cleanup := range cleanups {
			cleanup()
		}
	}()
	for _, n := range host.Nodes {
		run, cleanup, err := buildNode(cfg, host, n, opts, barrier)
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

type node interface {
	Run(ctx context.Context) error
	Close() error
}

func buildNode(cfg *Config, hCfg HostConfig, nCfg NodeConfig, barrier leaderBarrier) (node, error) {
	transports := make([]rpc.Transport, 0, 2)
	if len(hCfg.Nodes) > 1 {
		tr, err := rpc.NewPipeTransport(hCfg.BindAddr, nCfg.Port)
		if err != nil {
			return nil, err
		}
		transports = append(transports, tr)
	}
	if len(cfg.Hosts) > 1 {
		tr, err := rpc.NewQUICTransport(
			hCfg.BindAddr, nCfg.Port,
			hCfg.TLSCert, hCfg.TLSKey,
		)
		if err != nil {
			return nil, err
		}
		transports = append(transports, tr)
	}

	resolver := rpc.NewResolver(hCfg.ID, nCfg.ID)

	for _, host := range cfg.Hosts {
		for _, node := range host.Nodes {
			if node.ID != nCfg.ID {
				var dialer rpc.Dialer
				if host.ID == hCfg.ID {
					// Pipe for same host.
					dialer = transports[0]
				} else {
					// QUIC for cross host.
					dialer = transports[1]
				}
				resolver.RegisterPeer(host.ID, node.ID, dialer)
			}
		}
	}

	switch nCfg.Role {
	case RoleGate:
		gate, err := gate.New(resolver, transports)
		if err != nil {
			return nil, err
		}
		return gate, nil

	case RoleMeta:
		meta, err := meta.New(resolver, transports)
		if err != nil {
			return nil, err
		}
		return meta, nil

	case RoleBlob:
		blob, err := blob.New(resolver, transports)
		if err != nil {
			return nil, err
		}
		return blob, nil
	}

	return nil, nil
}
