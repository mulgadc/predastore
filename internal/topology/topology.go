// Package topology models the two-level shape of a predastore cluster:
// hosts, which are processes owning a socket and a data directory, and
// nodes, which are logical roles pinned to a host. It resolves node ids to
// dialable transport addresses given the set of nodes running locally.
package topology

import (
	"fmt"
	"net"
	"path/filepath"
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
		return fmt.Errorf("topology: no hosts defined")
	}
	if len(nodes) == 0 {
		return fmt.Errorf("topology: no nodes defined")
	}

	hostIDs := make(map[int]bool, len(hosts))
	for _, h := range hosts {
		if h.ID <= 0 {
			return fmt.Errorf("topology: host id %d must be positive", h.ID)
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

	nodeIDs := make(map[int]bool, len(nodes))
	for _, n := range nodes {
		if n.ID <= 0 {
			return fmt.Errorf("topology: node id %d must be positive", n.ID)
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
func NodeKey(nodeID int) string {
	return fmt.Sprintf("node-%d", nodeID)
}

// Topology resolves node ids to dialable addresses for one process. Nodes
// launched in this process resolve to their in-process pipe endpoint; all
// others resolve to their host's public address, keyed by node.
type Topology struct {
	hosts map[int]Host
	nodes map[int]Node
	local map[int]bool
	// host is the host this process runs; every local node belongs to it.
	host Host
}

// NewTopology validates the topology and the local node selection.
//
// A process that has remote peers binds one host's socket, so its local nodes
// must all be pinned to that host. A process running the entire cluster binds
// nothing, so its nodes may span hosts: that is the single-process mode.
func NewTopology(hosts []Host, nodes []Node, localNodeIDs []int) (*Topology, error) {
	if err := Validate(hosts, nodes); err != nil {
		return nil, err
	}
	if len(localNodeIDs) == 0 {
		return nil, fmt.Errorf("topology: no local nodes selected")
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

	hostID := 0
	spansHosts := false
	for _, id := range localNodeIDs {
		n, ok := t.nodes[id]
		if !ok {
			return nil, fmt.Errorf("topology: local node %d not in topology", id)
		}
		if t.local[id] {
			return nil, fmt.Errorf("topology: local node %d selected twice", id)
		}
		if hostID == 0 {
			hostID = n.HostID
		} else if n.HostID != hostID {
			spansHosts = true
		}
		t.local[id] = true
	}
	// Spanning hosts is only coherent when nothing is reachable over the
	// network, since otherwise there is no single socket to bind.
	if spansHosts && t.NeedsNetwork() {
		return nil, fmt.Errorf("topology: local nodes span hosts but some node runs elsewhere; a process with remote peers runs one host")
	}
	t.host = t.hosts[hostID]

	return t, nil
}

// LocalHost is the host whose socket this process binds. It is meaningful
// only when NeedsNetwork reports true.
func (t *Topology) LocalHost() Host { return t.host }

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

// NodeAddr resolves a node id to the address a client should dial: its
// in-process pipe endpoint for local nodes, its host's public address keyed
// by node otherwise. Callers never learn which they got.
func (t *Topology) NodeAddr(nodeID int) (net.Addr, error) {
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
func (t *Topology) ListenAddrs(nodeID int) ([]net.Addr, error) {
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

// DataDir is where a node keeps its state, derived from its own host's base
// directory and its node id.
func (t *Topology) DataDir(nodeID int) string {
	h := t.hosts[t.nodes[nodeID].HostID]
	return filepath.Join(h.DataDir, NodeKey(nodeID))
}

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
