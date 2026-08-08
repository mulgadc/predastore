package state

import (
	"github.com/mulgadc/predastore/internal/config"
	"time"
)

// RaftPeer identifies one voting member. The advertise address is
// node-identifying, and the stream layer's dial function resolves it to a
// transport.
type RaftPeer struct {
	ID      config.NodeID
	Address string
}

// ClusterConfig represents the full cluster configuration.
type ClusterConfig struct {
	NodeID    config.NodeID // This node's ID
	DataDir   string        // Base data directory
	Bootstrap bool          // Whether to bootstrap a new cluster

	// StreamLayer carries all raft traffic; Peers lists the voting members
	// bootstrap uses. Addressing and encryption are the transport's concern,
	// so neither is configured here.
	StreamLayer *RPCStreamLayer
	Peers       []RaftPeer

	// Raft tuning
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	CommitTimeout      time.Duration
	SnapshotInterval   time.Duration
	SnapshotThreshold  uint64
	TrailingLogs       uint64
	MaxAppendEntries   uint64
	LeaderLeaseTimeout time.Duration
}

// DefaultClusterConfig returns sensible defaults for a cluster.
func DefaultClusterConfig() *ClusterConfig {
	return &ClusterConfig{
		HeartbeatTimeout:   1000 * time.Millisecond,
		ElectionTimeout:    1000 * time.Millisecond,
		CommitTimeout:      50 * time.Millisecond,
		SnapshotInterval:   120 * time.Second,
		SnapshotThreshold:  8192,
		TrailingLogs:       10240,
		MaxAppendEntries:   64,
		LeaderLeaseTimeout: 500 * time.Millisecond,
	}
}
