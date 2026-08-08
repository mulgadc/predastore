// Package topology models the two-level shape of a predastore cluster:
// hosts, which are processes owning a socket and a data directory, and
// nodes, which are logical roles pinned to a host. It resolves node ids to
// dialable transport addresses given the host running locally.
//
// Host, Node and Role carry the toml tags for the [[host]] and [[node]]
// tables, and the root package re-exports them as HostConfig, NodeConfig and
// Role. The shape an operator writes and the shape placement is derived from
// are one thing, so there is nothing to keep in sync.
package topology

import (
	"fmt"
	"net"

	"github.com/mulgadc/predastore/internal/transport"
)

// NodeID and HostID identify the two levels of the cluster. They are distinct
// types because almost every function here takes one or the other and they are
// otherwise indistinguishable: passing a host id where a node id belongs is a
// mistake the compiler should catch rather than a lookup that quietly misses.
//
// They are uint64 rather than int because both are serialized — to TOML in the
// configuration, and node ids to JSON in the delete tombstones the compactor
// reads. A fixed width keeps that encoding the same on every platform, and an
// unsigned one makes a negative id unrepresentable rather than a validation
// step everything downstream has to trust. The width matches what consensus
// and the shard store already counted in, so a node id crosses those
// boundaries without a conversion to get wrong.
type (
	NodeID uint64
	HostID uint64
)

// Role is the function a node performs within the cluster, as written under
// [[node]].
type Role string

const (
	// RoleShardStorage stores erasure-coded object shards.
	RoleShardStorage Role = "shard-storage"
	// RoleStateReplica participates in Raft consensus over global state.
	RoleStateReplica Role = "state-replica"
)

// Host is one s3d process, as written under [[host]]: the endpoint that owns
// a socket and a data directory. Nodes pinned to it run inside that process
// as goroutines.
type Host struct {
	ID HostID `toml:"id"`
	// BindAddr is the local listen address; 0.0.0.0 binds all interfaces.
	BindAddr string `toml:"bind_addr"`
	// PublicAddr is the address other hosts dial, split from BindAddr for
	// NAT and multi-homed machines.
	PublicAddr string `toml:"public_addr"`
	// DataDir is the on-disk root; nodes derive their subdirectories from
	// node id and role. A relative path resolves against the config's
	// BasePath.
	DataDir string `toml:"data_dir"`
}

// Node is a logical role pinned to a host, as written under [[node]]. Nodes
// sharing a host are colocated and talk over the in-process pipe; nodes on
// different hosts talk over the network.
type Node struct {
	ID     NodeID `toml:"id"`
	HostID HostID `toml:"host_id"`
	Role   Role   `toml:"role"`
}

// Validate checks the topology as a whole: ids unique, placements resolvable,
// and roles known. An empty topology is invalid; callers gate on presence.
func Validate(hosts []Host, nodes []Node) error {
	if len(hosts) == 0 {
		return fmt.Errorf("topology: no hosts defined")
	}
	if len(nodes) == 0 {
		return fmt.Errorf("topology: no nodes defined")
	}

	hostIDs := make(map[HostID]bool, len(hosts))
	for _, h := range hosts {
		if h.ID == 0 {
			return fmt.Errorf("topology: host id must be positive")
		}
		if hostIDs[h.ID] {
			return fmt.Errorf("topology: duplicate host id %d", h.ID)
		}
		hostIDs[h.ID] = true
		if h.BindAddr == "" {
			return fmt.Errorf("topology: host %d missing bind_addr", h.ID)
		}
		if h.PublicAddr == "" {
			return fmt.Errorf("topology: host %d missing public_addr", h.ID)
		}
		if h.DataDir == "" {
			return fmt.Errorf("topology: host %d missing data_dir", h.ID)
		}
	}

	nodeIDs := make(map[NodeID]bool, len(nodes))
	for _, n := range nodes {
		if n.ID == 0 {
			return fmt.Errorf("topology: node id must be positive")
		}
		if nodeIDs[n.ID] {
			return fmt.Errorf("topology: duplicate node id %d", n.ID)
		}
		nodeIDs[n.ID] = true
		if !hostIDs[n.HostID] {
			return fmt.Errorf("topology: node %d references unknown host %d", n.ID, n.HostID)
		}
		switch n.Role {
		case RoleShardStorage, RoleStateReplica:
		default:
			return fmt.Errorf("topology: node %d has unknown role %q", n.ID, n.Role)
		}
	}

	return nil
}

// NodeKey is the name a node answers to on both transports: its pipe
// registry entry in-process, and the ALPN key selecting it on its host's
// shared socket. Everything per-node derives from the host base and this.
func NodeKey(nodeID NodeID) string {
	return fmt.Sprintf("node-%d", nodeID)
}

// Resolver turns node ids into dialable addresses for one process. Nodes
// pinned to the local host resolve to their in-process pipe endpoint; all
// others resolve to their host's public address, keyed by node. It answers
// nothing else about the cluster: inventory questions belong to whoever owns
// the configuration.
type Resolver struct {
	hosts map[HostID]Host
	nodes map[NodeID]Node
	local map[NodeID]bool
	// host is the host this process runs; every local node is pinned to it.
	host Host
}

// NewResolver validates the topology and selects the host this process runs.
// A process is one host, so the nodes it runs follow from the selection rather
// than being named individually.
func NewResolver(hosts []Host, nodes []Node, hostID HostID) (*Resolver, error) {
	if err := Validate(hosts, nodes); err != nil {
		return nil, err
	}

	t := &Resolver{
		hosts: make(map[HostID]Host, len(hosts)),
		nodes: make(map[NodeID]Node, len(nodes)),
		local: make(map[NodeID]bool, len(nodes)),
	}
	for _, h := range hosts {
		t.hosts[h.ID] = h
	}
	for _, n := range nodes {
		t.nodes[n.ID] = n
	}

	host, ok := t.hosts[hostID]
	if !ok {
		return nil, fmt.Errorf("topology: local host %d not in topology", hostID)
	}
	for _, n := range t.nodes {
		if n.HostID == hostID {
			t.local[n.ID] = true
		}
	}
	// A host with no nodes has nothing to serve, which is a misconfiguration
	// rather than an idle process.
	if len(t.local) == 0 {
		return nil, fmt.Errorf("topology: local host %d has no nodes", hostID)
	}
	t.host = host

	return t, nil
}

// NodeAddr resolves a node id to the address a client should dial: its
// in-process pipe endpoint for local nodes, its host's public address keyed
// by node otherwise. Callers never learn which they got.
func (t *Resolver) NodeAddr(nodeID NodeID) (net.Addr, error) {
	n, ok := t.nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("topology: unknown node %d", nodeID)
	}
	if t.local[n.ID] {
		return transport.ResolveAddr(string(transport.NetworkPipe), NodeKey(n.ID))
	}
	h := t.hosts[n.HostID]
	return transport.NewQUICAddr(h.PublicAddr, NodeKey(n.ID)), nil
}

// ListenAddrs are the addresses a local node's rpc server serves: its pipe
// endpoint always, plus this host's socket when some peer runs elsewhere.
// A process whose peers are all local never opens a network socket.
func (t *Resolver) ListenAddrs(nodeID NodeID) ([]net.Addr, error) {
	if !t.local[nodeID] {
		return nil, fmt.Errorf("topology: node %d does not run in this process", nodeID)
	}
	pipeAddr, err := transport.ResolveAddr(string(transport.NetworkPipe), NodeKey(nodeID))
	if err != nil {
		return nil, err
	}
	addrs := []net.Addr{pipeAddr}
	if t.NeedsNetwork() {
		addrs = append(addrs, transport.NewQUICAddr(t.host.BindAddr, NodeKey(nodeID)))
	}
	return addrs, nil
}

// NeedsNetwork reports whether any node in the topology runs outside this
// process; a process whose peers are all local opens no network socket.
func (t *Resolver) NeedsNetwork() bool {
	for id := range t.nodes {
		if !t.local[id] {
			return true
		}
	}
	return false
}
