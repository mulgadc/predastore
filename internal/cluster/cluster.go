// Package cluster models the two-level topology of a predastore cluster:
// hosts, which are processes owning a socket and a data directory, and
// nodes, which are logical roles pinned to a host. It resolves node ids to
// dialable transport addresses given the set of nodes running locally.
package cluster

import (
	"fmt"
	"net"
	"slices"

	"github.com/mulgadc/predastore/internal/transport"
)

// Role is the function a node performs within the cluster.
type Role string

const (
	// RoleShardStorage stores erasure-coded object shards.
	RoleShardStorage Role = "shard-storage"
	// RoleStateReplica participates in Raft consensus over global state.
	RoleStateReplica Role = "state-replica"
)

// Host is one s3d process: the endpoint that owns a socket and a data
// directory. Nodes pinned to it run inside that process as goroutines.
type Host struct {
	ID int `toml:"id"`
	// BindAddr is the local listen address; 0.0.0.0 binds all interfaces.
	BindAddr string `toml:"bind_addr"`
	// PublicAddr is the address other hosts dial, split from BindAddr for
	// NAT and multi-homed machines.
	PublicAddr string `toml:"public_addr"`
	// DataDir is the on-disk root; nodes derive their subdirectories from
	// node id and role.
	DataDir string `toml:"data_dir"`
}

// Node is a logical role pinned to a host. Nodes sharing a host are
// colocated and talk over the in-process pipe; nodes on different hosts
// talk over the network.
type Node struct {
	ID     int  `toml:"id"`
	HostID int  `toml:"host_id"`
	Role   Role `toml:"role"`
}

// Validate checks the topology as a whole: ids unique, placements resolvable,
// and roles known. An empty topology is invalid; callers gate on presence.
func Validate(hosts []Host, nodes []Node) error {
	if len(hosts) == 0 {
		return fmt.Errorf("cluster: no hosts defined")
	}
	if len(nodes) == 0 {
		return fmt.Errorf("cluster: no nodes defined")
	}

	hostIDs := make(map[int]bool, len(hosts))
	for _, h := range hosts {
		if h.ID <= 0 {
			return fmt.Errorf("cluster: host id %d must be positive", h.ID)
		}
		if hostIDs[h.ID] {
			return fmt.Errorf("cluster: duplicate host id %d", h.ID)
		}
		hostIDs[h.ID] = true
		if h.BindAddr == "" {
			return fmt.Errorf("cluster: host %d missing bind_addr", h.ID)
		}
		if h.PublicAddr == "" {
			return fmt.Errorf("cluster: host %d missing public_addr", h.ID)
		}
		if h.DataDir == "" {
			return fmt.Errorf("cluster: host %d missing data_dir", h.ID)
		}
	}

	nodeIDs := make(map[int]bool, len(nodes))
	for _, n := range nodes {
		if n.ID <= 0 {
			return fmt.Errorf("cluster: node id %d must be positive", n.ID)
		}
		if nodeIDs[n.ID] {
			return fmt.Errorf("cluster: duplicate node id %d", n.ID)
		}
		nodeIDs[n.ID] = true
		if !hostIDs[n.HostID] {
			return fmt.Errorf("cluster: node %d references unknown host %d", n.ID, n.HostID)
		}
		switch n.Role {
		case RoleShardStorage, RoleStateReplica:
		default:
			return fmt.Errorf("cluster: node %d has unknown role %q", n.ID, n.Role)
		}
	}

	return nil
}

// Topology resolves node ids to dialable addresses for one process. Nodes
// launched in this process resolve to the process pipe endpoint; all others
// resolve to their host's public address over the network.
type Topology struct {
	hosts map[int]Host
	nodes map[int]Node
	local map[int]bool
	// pipeAddr is this process's pipe endpoint, shared by all local nodes.
	pipeAddr net.Addr
}

// NewTopology validates the topology and the local node selection. pipeName
// names this process's pipe endpoint; every local node is reachable there.
func NewTopology(hosts []Host, nodes []Node, localNodeIDs []int, pipeName string) (*Topology, error) {
	if err := Validate(hosts, nodes); err != nil {
		return nil, err
	}
	if len(localNodeIDs) == 0 {
		return nil, fmt.Errorf("cluster: no local nodes selected")
	}
	if pipeName == "" {
		return nil, fmt.Errorf("cluster: missing pipe endpoint name")
	}

	t := &Topology{
		hosts: make(map[int]Host, len(hosts)),
		nodes: make(map[int]Node, len(nodes)),
		local: make(map[int]bool, len(localNodeIDs)),
	}
	for _, h := range hosts {
		t.hosts[h.ID] = h
	}
	for _, n := range nodes {
		t.nodes[n.ID] = n
	}
	for _, id := range localNodeIDs {
		if _, ok := t.nodes[id]; !ok {
			return nil, fmt.Errorf("cluster: local node %d not in topology", id)
		}
		if t.local[id] {
			return nil, fmt.Errorf("cluster: local node %d selected twice", id)
		}
		t.local[id] = true
	}

	pipeAddr, err := transport.ResolveAddr(string(transport.NetworkPipe), pipeName)
	if err != nil {
		return nil, err
	}
	t.pipeAddr = pipeAddr

	return t, nil
}

// IsLocal reports whether the node runs in this process.
func (t *Topology) IsLocal(nodeID int) bool { return t.local[nodeID] }

// Node returns the node's config.
func (t *Topology) Node(nodeID int) (Node, bool) {
	n, ok := t.nodes[nodeID]
	return n, ok
}

// Host returns the host's config.
func (t *Topology) Host(hostID int) (Host, bool) {
	h, ok := t.hosts[hostID]
	return h, ok
}

// NodeAddr resolves a node id to the address a client should dial: the
// process pipe endpoint for local nodes, the node's host public address
// over the network otherwise.
func (t *Topology) NodeAddr(nodeID int) (net.Addr, error) {
	n, ok := t.nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("cluster: unknown node %d", nodeID)
	}
	if t.local[n.ID] {
		return t.pipeAddr, nil
	}
	h := t.hosts[n.HostID]
	return transport.ResolveAddr(string(transport.NetworkQUIC), h.PublicAddr)
}

// PipeName is the name of this process's pipe endpoint.
func (t *Topology) PipeName() string { return t.pipeAddr.String() }

// LocalNodes returns the nodes running in this process, sorted by id.
func (t *Topology) LocalNodes() []Node {
	return t.selectNodes(func(n Node) bool { return t.local[n.ID] })
}

// NodesByRole returns every node with the role, local or not, sorted by id.
func (t *Topology) NodesByRole(role Role) []Node {
	return t.selectNodes(func(n Node) bool { return n.Role == role })
}

// NeedsNetwork reports whether any node in the topology runs outside this
// process; a process whose peers are all local opens no network socket.
func (t *Topology) NeedsNetwork() bool {
	for id := range t.nodes {
		if !t.local[id] {
			return true
		}
	}
	return false
}

// LocalBindAddrs returns the distinct bind addresses of hosts with a node in
// this process, sorted; these are the network listen addresses.
func (t *Topology) LocalBindAddrs() []string {
	seen := make(map[string]bool)
	var addrs []string
	for id := range t.local {
		h := t.hosts[t.nodes[id].HostID]
		if !seen[h.BindAddr] {
			seen[h.BindAddr] = true
			addrs = append(addrs, h.BindAddr)
		}
	}
	slices.Sort(addrs)
	return addrs
}

func (t *Topology) selectNodes(keep func(Node) bool) []Node {
	var out []Node
	for _, n := range t.nodes {
		if keep(n) {
			out = append(out, n)
		}
	}
	slices.SortFunc(out, func(a, b Node) int { return a.ID - b.ID })
	return out
}
