package s3db

import (
	"fmt"
	"time"

	"github.com/hashicorp/raft"
)

// DBNodeConfig represents a single database node in the cluster.
type DBNodeConfig struct {
	ID              uint64 `toml:"id"`
	Host            string `toml:"host"`
	Port            int    `toml:"port"`
	RaftPort        int    `toml:"raft_port"`      // Separate port for Raft transport
	AdvertiseHost   string `toml:"advertise_host"` // Address to advertise to other nodes (defaults to Host if empty, or 127.0.0.1 if Host is 0.0.0.0)
	Path            string `toml:"path"`
	AccessKeyID     string `toml:"access_key_id"`
	SecretAccessKey string `toml:"secret_access_key"`
	Leader          bool   `toml:"leader"` // Initial bootstrap leader hint
}

// RaftPeer identifies one voting member when raft runs over an injected
// stream layer: the advertise address is node-identifying, and the stream
// layer's dial function resolves it to a transport.
type RaftPeer struct {
	ID      uint64
	Address string
}

// ClusterConfig represents the full cluster configuration.
type ClusterConfig struct {
	NodeID    uint64         // This node's ID
	Nodes     []DBNodeConfig // All nodes in cluster
	DataDir   string         // Base data directory
	Bootstrap bool           // Whether to bootstrap a new cluster

	// StreamLayer, when set, carries raft traffic instead of the built-in
	// TCP+TLS transport; Peers then lists the voting members bootstrap
	// uses. Encryption is the transport's concern in this mode, so TLSCert
	// and TLSKey are not required.
	StreamLayer raft.StreamLayer
	Peers       []RaftPeer

	// TLS material for the legacy TCP Raft transport, set programmatically by
	// the caller — not from cluster.toml. NewRaftNode refuses to start if
	// either is empty and no StreamLayer is injected.
	TLSCert string
	TLSKey  string

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

// GetNode returns the node config for the given ID.
func (c *ClusterConfig) GetNode(id uint64) *DBNodeConfig {
	for i := range c.Nodes {
		if c.Nodes[i].ID == id {
			return &c.Nodes[i]
		}
	}
	return nil
}

// GetThisNode returns the config for this node.
func (c *ClusterConfig) GetThisNode() *DBNodeConfig {
	return c.GetNode(c.NodeID)
}

// RaftAddr returns the Raft transport bind address for a node.
func (n *DBNodeConfig) RaftAddr() string {
	port := n.RaftPort
	if port == 0 {
		port = n.Port + 1000 // Default: base port + 1000
	}
	return fmt.Sprintf("%s:%d", n.Host, port)
}

// RaftAdvertiseAddr returns the address to advertise to other Raft nodes
// If AdvertiseHost is set, use it. If Host is 0.0.0.0, default to 127.0.0.1.
func (n *DBNodeConfig) RaftAdvertiseAddr() string {
	port := n.RaftPort
	if port == 0 {
		port = n.Port + 1000
	}

	host := n.AdvertiseHost
	if host == "" {
		host = n.Host
		// 0.0.0.0 is not a valid advertise address - default to localhost
		if host == "0.0.0.0" {
			host = "127.0.0.1"
		}
	}
	return fmt.Sprintf("%s:%d", host, port)
}
