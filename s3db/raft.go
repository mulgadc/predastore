package s3db

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// RaftNode wraps Raft consensus with Badger storage
type RaftNode struct {
	config    *ClusterConfig
	raft      *raft.Raft
	fsm       *FSM
	transport *raft.NetworkTransport
	logStore  raft.LogStore
	stable    raft.StableStore
	snapshots raft.SnapshotStore
	badgerDB  *badger.DB
}

// NewRaftNode creates and initializes a new Raft node
func NewRaftNode(config *ClusterConfig) (*RaftNode, error) {
	node := &RaftNode{config: config}

	thisNode := config.GetThisNode()
	if thisNode == nil {
		return nil, fmt.Errorf("node ID %d not found in cluster config", config.NodeID)
	}

	// Create data directories
	dataDir := filepath.Join(config.DataDir, fmt.Sprintf("node-%d", config.NodeID))
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
	raftConfig.LocalID = raft.ServerID(fmt.Sprintf("%d", config.NodeID))
	raftConfig.HeartbeatTimeout = config.HeartbeatTimeout
	raftConfig.ElectionTimeout = config.ElectionTimeout
	raftConfig.CommitTimeout = config.CommitTimeout
	raftConfig.SnapshotInterval = config.SnapshotInterval
	raftConfig.SnapshotThreshold = config.SnapshotThreshold
	raftConfig.TrailingLogs = config.TrailingLogs
	raftConfig.LeaderLeaseTimeout = config.LeaderLeaseTimeout

	// Setup Raft transport
	// bindAddr is the address to listen on (can be 0.0.0.0)
	// advertiseAddr is the address advertised to other nodes (must be reachable)
	bindAddr := thisNode.RaftAddr()
	advertiseAddr := thisNode.RaftAdvertiseAddr()

	tcpAddr, err := net.ResolveTCPAddr("tcp", advertiseAddr)
	if err != nil {
		node.badgerDB.Close()
		return nil, fmt.Errorf("failed to resolve TCP address: %w", err)
	}

	slog.Info("Setting up Raft transport", "bindAddr", bindAddr, "advertiseAddr", advertiseAddr)
	node.transport, err = raft.NewTCPTransport(bindAddr, tcpAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		node.badgerDB.Close()
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	// Setup log store and stable store (BoltDB)
	boltDBPath := filepath.Join(dataDir, "raft.db")
	boltStore, err := raftboltdb.NewBoltStore(boltDBPath)
	if err != nil {
		node.Close()
		return nil, fmt.Errorf("failed to create bolt store: %w", err)
	}
	node.logStore = boltStore
	node.stable = boltStore

	// Setup snapshot store
	snapshotDir := filepath.Join(dataDir, "snapshots")
	node.snapshots, err = raft.NewFileSnapshotStore(snapshotDir, 2, os.Stderr)
	if err != nil {
		node.Close()
		return nil, fmt.Errorf("failed to create snapshot store: %w", err)
	}

	// Create Raft instance
	node.raft, err = raft.NewRaft(raftConfig, node.fsm, node.logStore, node.stable, node.snapshots, node.transport)
	if err != nil {
		node.Close()
		return nil, fmt.Errorf("failed to create raft: %w", err)
	}

	// Bootstrap cluster if requested
	if config.Bootstrap {
		if err := node.bootstrap(); err != nil {
			node.Close()
			return nil, fmt.Errorf("failed to bootstrap cluster: %w", err)
		}
	}

	return node, nil
}

// bootstrap initializes the cluster with all configured nodes
func (n *RaftNode) bootstrap() error {
	servers := make([]raft.Server, 0, len(n.config.Nodes))
	for _, node := range n.config.Nodes {
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(fmt.Sprintf("%d", node.ID)),
			Address: raft.ServerAddress(node.RaftAdvertiseAddr()),
		})
	}

	config := raft.Configuration{Servers: servers}
	future := n.raft.BootstrapCluster(config)
	if err := future.Error(); err != nil {
		// ErrCantBootstrap is ok - means cluster already bootstrapped
		if err != raft.ErrCantBootstrap {
			return err
		}
	}
	return nil
}

// Put stores a key-value pair through Raft consensus
func (n *RaftNode) Put(table, key string, value []byte) error {
	if n.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	cmd := Command{
		Type:  CommandPut,
		Table: table,
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

// Delete removes a key through Raft consensus
func (n *RaftNode) Delete(table, key string) error {
	if n.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	cmd := Command{
		Type:  CommandDelete,
		Table: table,
		Key:   []byte(key),
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
func (n *RaftNode) Get(table, key string) ([]byte, error) {
	return n.fsm.Get(table, key)
}

// Scan iterates over keys with prefix in the given table
func (n *RaftNode) Scan(table, prefix string, fn func(key string, value []byte) error) error {
	return n.fsm.Scan(table, prefix, fn)
}

// IsLeader returns true if this node is the current Raft leader
func (n *RaftNode) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

// LeaderAddr returns the address of the current leader
func (n *RaftNode) LeaderAddr() string {
	addr, _ := n.raft.LeaderWithID()
	return string(addr)
}

// LeaderID returns the ID of the current leader
func (n *RaftNode) LeaderID() string {
	_, id := n.raft.LeaderWithID()
	return string(id)
}

// WaitForLeader blocks until a leader is elected or timeout
func (n *RaftNode) WaitForLeader(timeout time.Duration) error {
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

// Stats returns Raft statistics
func (n *RaftNode) Stats() map[string]string {
	return n.raft.Stats()
}

// Close shuts down the Raft node cleanly with a timeout.
// The shutdown process:
// 1. Close transport first to stop network activity immediately
// 2. Attempt graceful Raft shutdown with 5s timeout
// 3. Close BoltDB log store
// 4. Close Badger FSM storage
func (n *RaftNode) Close() error {
	slog.Info("RaftNode: starting shutdown")

	// Close transport first to stop all network activity.
	// This prevents election loops when other nodes have already stopped,
	// and causes immediate connection errors instead of timeouts.
	if n.transport != nil {
		slog.Info("RaftNode: closing transport")
		n.transport.Close()
	}

	// Shutdown Raft with a timeout to avoid blocking forever
	// when we can't reach quorum (other nodes already stopped)
	if n.raft != nil {
		slog.Info("RaftNode: initiating raft shutdown")
		future := n.raft.Shutdown()

		// Wait for shutdown with timeout
		done := make(chan error, 1)
		go func() {
			done <- future.Error()
		}()

		select {
		case err := <-done:
			if err != nil {
				slog.Warn("RaftNode: raft shutdown returned error", "error", err)
			} else {
				slog.Info("RaftNode: raft shutdown completed gracefully")
			}
		case <-time.After(5 * time.Second):
			slog.Warn("RaftNode: raft shutdown timed out after 5s, forcing close")
		}
	}

	// Close BoltDB log store
	if store, ok := n.logStore.(*raftboltdb.BoltStore); ok {
		slog.Info("RaftNode: closing BoltDB log store")
		store.Close()
	}

	// Close Badger FSM storage
	if n.badgerDB != nil {
		slog.Info("RaftNode: closing Badger DB")
		n.badgerDB.Close()
	}

	slog.Info("RaftNode: shutdown complete")
	return nil
}

// Join adds a new node to the cluster (must be called on leader)
func (n *RaftNode) Join(nodeID string, addr string) error {
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

// Leave removes this node from the cluster
func (n *RaftNode) Leave() error {
	if n.raft.State() == raft.Leader {
		// Transfer leadership first
		future := n.raft.LeadershipTransfer()
		if err := future.Error(); err != nil {
			slog.Error("failed to transfer leadership before leaving cluster", "error", err)
		}
	}

	return nil
}

// Errors
var (
	ErrNotLeader = fmt.Errorf("not the leader")
)
