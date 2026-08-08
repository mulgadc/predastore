package state

import (
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
	"github.com/mulgadc/predastore/internal/topology"
)

// Server is one state replica: the raft node itself, its FSM and badger
// store, and the rpc handlers fronting them (see handlers.go). A process
// running several replicas builds one Server per node, each on its own rpc
// server, so a Server never learns that it has siblings.
type Server struct {
	id        topology.NodeID
	config    *ClusterConfig
	raft      *raft.Raft
	fsm       *FSM
	layer     *RPCStreamLayer
	transport *raft.NetworkTransport
	logStore  raft.LogStore
	stable    raft.StableStore
	snapshots raft.SnapshotStore
	badgerDB  *badger.DB
}

// NewServer creates and initializes a state replica from its cluster config.
func NewServer(config *ClusterConfig) (*Server, error) {
	// Fail closed: the raft transport carries committed log entries (object
	// metadata, IAM state) and the stream layer is what encrypts them. There
	// is no self-hosted fallback.
	if config.StreamLayer == nil {
		return nil, fmt.Errorf("state: a stream layer is required; refusing to start without one")
	}

	node := &Server{id: config.NodeID, config: config, layer: config.StreamLayer}

	// Create data directories
	dataDir := config.DataDir
	if err := os.MkdirAll(dataDir, 0750); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Initialize Badger for FSM storage
	badgerDir := filepath.Join(dataDir, "badger")
	if err := os.MkdirAll(badgerDir, 0750); err != nil {
		return nil, fmt.Errorf("failed to create badger directory: %w", err)
	}

	badgerOpts := badger.DefaultOptions(badgerDir).
		WithLoggingLevel(badger.WARNING).
		WithSyncWrites(true) // Ensure durability

	var err error
	node.badgerDB, err = badger.Open(badgerOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger: %w", err)
	}

	// Create FSM
	node.fsm = NewFSM(node.badgerDB)

	// Configure Raft
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(strconv.FormatUint(uint64(config.NodeID), 10))
	raftConfig.HeartbeatTimeout = config.HeartbeatTimeout
	raftConfig.ElectionTimeout = config.ElectionTimeout
	raftConfig.CommitTimeout = config.CommitTimeout
	raftConfig.SnapshotInterval = config.SnapshotInterval
	raftConfig.SnapshotThreshold = config.SnapshotThreshold
	raftConfig.TrailingLogs = config.TrailingLogs
	raftConfig.LeaderLeaseTimeout = config.LeaderLeaseTimeout

	// Setup Raft transport
	slog.Info("Setting up Raft transport over rpc stream layer", "advertiseAddr", config.StreamLayer.Addr())
	node.transport = raft.NewNetworkTransport(config.StreamLayer, 3, 10*time.Second, os.Stderr)

	// Setup log store and stable store (BoltDB)
	boltDBPath := filepath.Join(dataDir, "raft.db")
	boltStore, err := raftboltdb.NewBoltStore(boltDBPath)
	if err != nil {
		if cerr := node.Close(); cerr != nil {
			slog.Debug("Failed to close node during cleanup", "error", cerr)
		}
		return nil, fmt.Errorf("failed to create bolt store: %w", err)
	}
	node.logStore = boltStore
	node.stable = boltStore

	// Setup snapshot store
	snapshotDir := filepath.Join(dataDir, "snapshots")
	node.snapshots, err = raft.NewFileSnapshotStore(snapshotDir, 2, os.Stderr)
	if err != nil {
		if cerr := node.Close(); cerr != nil {
			slog.Debug("Failed to close node during cleanup", "error", cerr)
		}
		return nil, fmt.Errorf("failed to create snapshot store: %w", err)
	}

	// Create Raft instance
	node.raft, err = raft.NewRaft(raftConfig, node.fsm, node.logStore, node.stable, node.snapshots, node.transport)
	if err != nil {
		if cerr := node.Close(); cerr != nil {
			slog.Debug("Failed to close node during cleanup", "error", cerr)
		}
		return nil, fmt.Errorf("failed to create raft: %w", err)
	}

	// Bootstrap cluster if requested
	if config.Bootstrap {
		if err := node.bootstrap(); err != nil {
			if cerr := node.Close(); cerr != nil {
				slog.Debug("Failed to close node during cleanup", "error", cerr)
			}
			return nil, fmt.Errorf("failed to bootstrap cluster: %w", err)
		}
	}

	return node, nil
}

// bootstrap initializes the cluster with all configured nodes.
func (n *Server) bootstrap() error {
	// Peers carry node-identifying addresses that the stream layer's dial
	// function resolves.
	servers := make([]raft.Server, 0, len(n.config.Peers))
	for _, peer := range n.config.Peers {
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(strconv.FormatUint(uint64(peer.ID), 10)),
			Address: raft.ServerAddress(peer.Address),
		})
	}

	config := raft.Configuration{Servers: servers}
	future := n.raft.BootstrapCluster(config)
	if err := future.Error(); err != nil {
		// ErrCantBootstrap is ok - means cluster already bootstrapped
		if !errors.Is(err, raft.ErrCantBootstrap) {
			return err
		}
	}
	return nil
}

// Put stores a key-value pair through Raft consensus.
func (n *Server) Put(key string, value []byte) error {
	if n.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	cmd := Command{
		Type:  CommandPut,
		Key:   []byte(key),
		Value: value,
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	future := n.raft.Apply(data, 10*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("raft apply failed: %w", err)
	}

	// Check if the FSM returned an error
	if resp := future.Response(); resp != nil {
		if err, ok := resp.(error); ok {
			return err
		}
	}

	return nil
}

// Delete removes a key through Raft consensus.
func (n *Server) Delete(key string) error {
	if n.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	cmd := Command{
		Type: CommandDelete,
		Key:  []byte(key),
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	future := n.raft.Apply(data, 10*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("raft apply failed: %w", err)
	}

	// Check if the FSM returned an error
	if resp := future.Response(); resp != nil {
		if err, ok := resp.(error); ok {
			return err
		}
	}

	return nil
}

// Get reads a value from the local store
// Note: This may return stale data on followers. For strong consistency,
// use GetConsistent which forwards reads to the leader.
func (n *Server) Get(key string) ([]byte, error) {
	return n.fsm.Get(key)
}

// Scan iterates over every key with the given prefix.
func (n *Server) Scan(prefix string, fn func(key string, value []byte) error) error {
	return n.fsm.Scan(prefix, fn)
}

// IsLeader returns true if this node is the current Raft leader.
func (n *Server) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

// LeaderAddr returns the address of the current leader.
func (n *Server) LeaderAddr() string {
	addr, _ := n.raft.LeaderWithID()
	return string(addr)
}

// LeaderID returns the ID of the current leader.
func (n *Server) LeaderID() string {
	_, id := n.raft.LeaderWithID()
	return string(id)
}

// WaitForLeader blocks until a leader is elected or timeout.
func (n *Server) WaitForLeader(timeout time.Duration) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ticker.C:
			if leader := n.LeaderAddr(); leader != "" {
				return nil
			}
		case <-timer.C:
			return fmt.Errorf("timeout waiting for leader election")
		}
	}
}

// Stats returns Raft statistics.
func (n *Server) Stats() map[string]string {
	return n.raft.Stats()
}

// Close shuts down the Raft node cleanly with a timeout.
// The shutdown process:
// 1. Close transport first to stop network activity immediately
// 2. Attempt graceful Raft shutdown with 5s timeout
// 3. Close BoltDB log store
// 4. Close Badger FSM storage.
func (n *Server) Close() error {
	slog.Info("state: starting shutdown")

	// Close transport first to stop all network activity.
	// This prevents election loops when other nodes have already stopped,
	// and causes immediate connection errors instead of timeouts.
	if n.transport != nil {
		slog.Info("state: closing transport")
		if err := n.transport.Close(); err != nil {
			slog.Warn("state: failed to close transport", "error", err)
		}
	}

	// Shutdown Raft with a timeout to avoid blocking forever
	// when we can't reach quorum (other nodes already stopped)
	if n.raft != nil {
		slog.Info("state: initiating raft shutdown")
		future := n.raft.Shutdown()

		// Wait for shutdown with timeout
		done := make(chan error, 1)
		go func() {
			done <- future.Error()
		}()

		select {
		case err := <-done:
			if err != nil {
				slog.Warn("state: raft shutdown returned error", "error", err)
			} else {
				slog.Info("state: raft shutdown completed gracefully")
			}
		case <-time.After(5 * time.Second):
			slog.Warn("state: raft shutdown timed out after 5s, forcing close")
		}
	}

	// Close BoltDB log store
	if store, ok := n.logStore.(*raftboltdb.BoltStore); ok {
		slog.Info("state: closing BoltDB log store")
		if err := store.Close(); err != nil {
			slog.Warn("state: failed to close BoltDB log store", "error", err)
		}
	}

	// Close Badger FSM storage
	if n.badgerDB != nil {
		slog.Info("state: closing Badger DB")
		if err := n.badgerDB.Close(); err != nil {
			slog.Warn("state: failed to close Badger DB", "error", err)
		}
	}

	slog.Info("state: shutdown complete")
	return nil
}

// Join adds a new node to the cluster (must be called on leader).
func (n *Server) Join(nodeID string, addr string) error {
	if n.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	configFuture := n.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return fmt.Errorf("failed to get raft configuration: %w", err)
	}

	// Check if node is already in the cluster
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(nodeID) {
			if srv.Address == raft.ServerAddress(addr) {
				// Already joined
				return nil
			}
			// Node exists but with different address, remove first
			removeFuture := n.raft.RemoveServer(srv.ID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				return fmt.Errorf("failed to remove existing server: %w", err)
			}
		}
	}

	// Add the new node as a voter
	future := n.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to add voter: %w", err)
	}

	return nil
}

// Leave removes this node from the cluster.
func (n *Server) Leave() error {
	if n.raft.State() == raft.Leader {
		// Transfer leadership first
		future := n.raft.LeadershipTransfer()
		if err := future.Error(); err != nil {
			slog.Error("failed to transfer leadership before leaving cluster", "error", err)
		}
	}

	return nil
}
