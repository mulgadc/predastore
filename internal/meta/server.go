package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
)

const (
	// leaderPoll is how often the leader watch samples raft while it waits.
	leaderPoll = 100 * time.Millisecond

	// raftShutdownWait bounds the graceful raft shutdown: without quorum the
	// future never resolves, and the stores must still be released.
	raftShutdownWait = 5 * time.Second

	// applyTimeout bounds one consensus write.
	applyTimeout = 10 * time.Second
)

// Server is one meta replica: the raft node itself, its FSM and badger store,
// and the rpc handlers fronting them (see handlers.go). A process running
// several replicas builds one Server per node, each with its own rpc server
// and connection pool, so a Server never learns that it has siblings.
type Server struct {
	cfg Config

	pool      *rpc.ConnPool
	layer     *RPCStreamLayer
	transport *raft.NetworkTransport
	raft      *raft.Raft
	fsm       *FSM
	badgerDB  *badger.DB
	bolt      *raftboltdb.BoltStore
}

// New validates cfg and applies its defaults. It creates no directory, opens
// no database, starts no raft node and binds nothing: everything the replica
// holds is acquired by Run and released by it.
func New(cfg Config) (*Server, error) {
	if cfg.NodeID == 0 {
		return nil, errors.New("node id is required")
	}
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("node %d has no data directory", cfg.NodeID)
	}
	if cfg.Resolver == nil {
		return nil, fmt.Errorf("node %d has no resolver", cfg.NodeID)
	}
	// A replica no peer can dial cannot replicate. Failing here says so, rather
	// than leaving a raft node campaigning alone behind an open store.
	if len(cfg.Listeners) == 0 {
		return nil, fmt.Errorf("node %d has no listeners", cfg.NodeID)
	}

	cfg.LeaderTimeout = orDefault(cfg.LeaderTimeout, 30*time.Second)
	cfg.HeartbeatTimeout = orDefault(cfg.HeartbeatTimeout, 1000*time.Millisecond)
	cfg.ElectionTimeout = orDefault(cfg.ElectionTimeout, 1000*time.Millisecond)
	cfg.CommitTimeout = orDefault(cfg.CommitTimeout, 50*time.Millisecond)
	cfg.SnapshotInterval = orDefault(cfg.SnapshotInterval, 120*time.Second)
	cfg.SnapshotThreshold = orDefault(cfg.SnapshotThreshold, uint64(8192))
	cfg.TrailingLogs = orDefault(cfg.TrailingLogs, uint64(10240))
	cfg.LeaderLeaseTimeout = orDefault(cfg.LeaderLeaseTimeout, 500*time.Millisecond)

	return &Server{cfg: cfg}, nil
}

// orDefault replaces an unset value with the default for its field.
func orDefault[T comparable](v, def T) T {
	var unset T
	if v == unset {
		return def
	}
	return v
}

// Run opens the replica's stores, starts raft, answers rpc on the configured
// listeners until ctx is cancelled, and then shuts the lot down. The leader
// watch runs alongside the rpc server and stops with it.
func (s *Server) Run(ctx context.Context) error {
	srv, err := s.open()
	if err != nil {
		return errors.Join(err, s.shutdown())
	}

	watching := make(chan struct{})
	go func() {
		defer close(watching)
		s.watchLeader(ctx)
	}()

	serveErr := srv.Run(ctx)
	<-watching
	return errors.Join(serveErr, s.shutdown())
}

// open acquires everything the replica runs on, in dependency order: the rpc
// plumbing it is reached through, then its stores, then raft itself. Whatever
// it took before a failure is released by the shutdown Run runs after it.
func (s *Server) open() (*rpc.Server, error) {
	// One pool serves both directions: the client dials peers from it and the
	// rpc server donates the connections it accepts back to it, so a
	// connection carries raft traffic whichever end opened it.
	s.pool = rpc.NewConnPool(s.cfg.NodeID, s.cfg.Resolver)
	s.layer = NewRPCStreamLayer(RaftAddress(s.cfg.NodeID), raftDial(rpc.NewClient(s.pool)))

	mux := rpc.NewMux()
	rpc.RegisterHandler(mux, OpRaftDial, s.handleRaftDial)
	rpc.RegisterHandler(mux, OpMetaGet, s.handleGet)
	rpc.RegisterHandler(mux, OpMetaPut, s.handlePut)
	rpc.RegisterHandler(mux, OpMetaDelete, s.handleDelete)
	rpc.RegisterHandler(mux, OpMetaScan, s.handleScan)
	srv, err := rpc.NewServer(mux, s.cfg.Listeners, s.pool)
	if err != nil {
		return nil, err
	}

	// Creating the badger directory creates the data directory with it; the
	// bolt store and the snapshots land beside it.
	badgerDir := filepath.Join(s.cfg.DataDir, "badger")
	if err := os.MkdirAll(badgerDir, 0750); err != nil {
		return nil, fmt.Errorf("create data directory %s: %w", s.cfg.DataDir, err)
	}

	s.badgerDB, err = badger.Open(badger.DefaultOptions(badgerDir).
		WithLoggingLevel(badger.WARNING).
		WithSyncWrites(true)) // Ensure durability.
	if err != nil {
		return nil, fmt.Errorf("open badger: %w", err)
	}
	s.fsm = NewFSM(s.badgerDB)

	s.bolt, err = raftboltdb.NewBoltStore(filepath.Join(s.cfg.DataDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("create bolt store: %w", err)
	}
	snapshots, err := raft.NewFileSnapshotStore(filepath.Join(s.cfg.DataDir, "snapshots"), 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("create snapshot store: %w", err)
	}

	slog.Info("setting up raft transport over rpc stream layer", "advertiseAddr", s.layer.Addr())
	s.transport = raft.NewNetworkTransport(s.layer, 3, 10*time.Second, os.Stderr)

	s.raft, err = raft.NewRaft(s.raftConfig(), s.fsm, s.bolt, s.bolt, snapshots, s.transport)
	if err != nil {
		return nil, fmt.Errorf("create raft: %w", err)
	}
	if s.cfg.Bootstrap {
		if err := s.bootstrap(); err != nil {
			return nil, fmt.Errorf("bootstrap cluster: %w", err)
		}
	}
	return srv, nil
}

// raftConfig applies this replica's identity and tuning over raft's own
// defaults. New guarantees every field it sets is non-zero.
func (s *Server) raftConfig() *raft.Config {
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(strconv.FormatUint(uint64(s.cfg.NodeID), 10))
	cfg.HeartbeatTimeout = s.cfg.HeartbeatTimeout
	cfg.ElectionTimeout = s.cfg.ElectionTimeout
	cfg.CommitTimeout = s.cfg.CommitTimeout
	cfg.SnapshotInterval = s.cfg.SnapshotInterval
	cfg.SnapshotThreshold = s.cfg.SnapshotThreshold
	cfg.TrailingLogs = s.cfg.TrailingLogs
	cfg.LeaderLeaseTimeout = s.cfg.LeaderLeaseTimeout
	return cfg
}

// bootstrap initializes the cluster with all configured peers. Every replica
// may attempt it: the peer set is identical across them, so the attempt is
// idempotent and a bootstrapped cluster reports ErrCantBootstrap.
func (s *Server) bootstrap() error {
	servers := make([]raft.Server, 0, len(s.cfg.Peers))
	for _, peer := range s.cfg.Peers {
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(strconv.FormatUint(uint64(peer), 10)),
			Address: raft.ServerAddress(RaftAddress(peer)),
		})
	}

	future := s.raft.BootstrapCluster(raft.Configuration{Servers: servers})
	if err := future.Error(); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
		return err
	}
	return nil
}

// raftDial opens a raft connection through this replica's own client. Raft
// advertises node keys as addresses, so dialing one is parsing out the node id
// and opening a stream to it.
func raftDial(cli *rpc.Client) RaftDialFunc {
	return func(ctx context.Context, address raft.ServerAddress) (transport.Stream, error) {
		target, err := ParseRaftAddress(string(address))
		if err != nil {
			return nil, err
		}
		return rpc.OpenStream(ctx, cli, target, OpRaftDial, &RaftDial{})
	}
}

// watchLeader fires OnLeader once a leader is observed, or once LeaderTimeout
// expires without one: whoever waits on it would otherwise wait forever on a
// cluster that never elects. A cancelled run fires nothing, having nothing
// left to release.
func (s *Server) watchLeader(ctx context.Context) {
	if s.cfg.OnLeader == nil {
		return
	}

	ticker := time.NewTicker(leaderPoll)
	defer ticker.Stop()
	timeout := time.NewTimer(s.cfg.LeaderTimeout)
	defer timeout.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if s.leaderAddr() == "" {
				continue
			}
		case <-timeout.C:
			slog.Warn("no leader elected within timeout", "timeout", s.cfg.LeaderTimeout)
		}
		s.cfg.OnLeader()
		return
	}
}

// shutdown releases what open acquired, in reverse: the network first, so a
// replica that has lost quorum fails fast rather than blocking on elections,
// then raft, then the stores it was writing to.
func (s *Server) shutdown() error {
	slog.Info("starting shutdown")
	var errs []error

	// Closing the transport first stops all network activity: it prevents
	// election loops when peers have already stopped, and turns their calls
	// into immediate errors instead of timeouts.
	if s.transport != nil {
		if err := s.transport.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close transport: %w", err))
		}
	}
	if s.raft != nil {
		errs = append(errs, s.shutdownRaft())
	}
	if s.bolt != nil {
		if err := s.bolt.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close bolt store: %w", err))
		}
	}
	if s.badgerDB != nil {
		if err := s.badgerDB.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close badger: %w", err))
		}
	}
	if s.pool != nil {
		if err := s.pool.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close connection pool: %w", err))
		}
	}

	slog.Info("shutdown complete")
	return errors.Join(errs...)
}

// shutdownRaft waits out the graceful shutdown, giving up after
// raftShutdownWait so an unreachable quorum cannot hold the stores open.
func (s *Server) shutdownRaft() error {
	done := make(chan error, 1)
	future := s.raft.Shutdown()
	go func() { done <- future.Error() }()

	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("shutdown raft: %w", err)
		}
		return nil
	case <-time.After(raftShutdownWait):
		return fmt.Errorf("raft shutdown timed out after %s", raftShutdownWait)
	}
}

// put stores a key-value pair through raft consensus.
func (s *Server) put(key string, value []byte) error {
	return s.apply(Command{Type: CommandPut, Key: []byte(key), Value: value})
}

// delete removes a key through raft consensus.
func (s *Server) delete(key string) error {
	return s.apply(Command{Type: CommandDelete, Key: []byte(key)})
}

// apply commits one command, which only the leader may do.
func (s *Server) apply(cmd Command) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}

	future := s.raft.Apply(data, applyTimeout)
	if err := future.Error(); err != nil {
		return fmt.Errorf("raft apply failed: %w", err)
	}

	// The FSM reports a rejected command through the future's response.
	if err, ok := future.Response().(error); ok {
		return err
	}
	return nil
}

// get reads a value from the local store. It may return stale data on a
// follower; the client reads through the leader when it needs freshness.
func (s *Server) get(key string) ([]byte, error) {
	return s.fsm.Get(key)
}

// scan iterates over every key with the given prefix.
func (s *Server) scan(prefix string, fn func(key string, value []byte) error) error {
	return s.fsm.Scan(prefix, fn)
}

// leaderAddr returns the advertise address of the current leader, empty while
// there is none.
func (s *Server) leaderAddr() string {
	addr, _ := s.raft.LeaderWithID()
	return string(addr)
}
