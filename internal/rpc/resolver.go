package rpc

import (
	"fmt"
	"net"
	"strconv"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/transport"
)

// Route is how one node is reached: the transport that dials it and the
// address to dial. The transport is held directly rather than named, so
// dialing is a table lookup and nothing else.
type Route struct {
	Transport transport.Transport
	Addr      net.Addr
}

// addrKey identifies an address by its network as well as its host:port. A
// node's pipe and quic addresses are the same host:port and differ only by
// network, so host:port alone names two routes.
type addrKey struct {
	network  string
	hostPort string
}

func addrKeyOf(a net.Addr) addrKey { return addrKey{a.Network(), a.String()} }

// Resolver is the flat table of routes one node dials, built once from the
// configuration. It is the only place a node id becomes an address: callers
// name peers by id and never handle an address themselves.
type Resolver struct {
	routes map[config.NodeID]Route
	nodes  map[addrKey]config.NodeID
}

// NewResolver builds the table source dials from. Nodes on source's own host
// are reached over the pipe and every other node over quic. Gateways are left
// out: nothing dials one, their port is an S3 port and their rpc sockets are
// ephemeral.
//
// Whether a node needs a pipe or a quic transport follows from the
// configuration, so it cannot be a compile-time guarantee. A route with no
// matching transport fails here rather than at the dial it would have served.
func NewResolver(cfg *config.Config, source config.NodeID, trs ...transport.Transport) (*Resolver, error) {
	hosts := make(map[config.HostID]config.Host, len(cfg.Hosts))
	for _, h := range cfg.Hosts {
		hosts[h.ID] = h
	}
	byNetwork := make(map[string]transport.Transport, len(trs))
	for _, tr := range trs {
		byNetwork[tr.Network()] = tr
	}

	var local config.Node
	var found bool
	for _, n := range cfg.Nodes {
		if n.ID == source {
			local, found = n, true
		}
	}
	if !found {
		return nil, fmt.Errorf("rpc: node %d is not in the configuration", source)
	}

	r := &Resolver{
		routes: make(map[config.NodeID]Route, len(cfg.Nodes)),
		nodes:  make(map[addrKey]config.NodeID, len(cfg.Nodes)),
	}
	for _, n := range cfg.Nodes {
		if n.ID == source || n.Role == config.RoleGateway {
			continue
		}
		h, ok := hosts[n.HostID]
		if !ok {
			return nil, fmt.Errorf("rpc: node %d references unknown host %d", n.ID, n.HostID)
		}
		network := transport.NetworkQUIC
		if n.HostID == local.HostID {
			network = transport.NetworkPipe
		}
		tr, ok := byNetwork[string(network)]
		if !ok {
			return nil, fmt.Errorf("rpc: node %d needs a %s transport", n.ID, network)
		}

		addr := transport.NewAddr(network, net.JoinHostPort(h.PublicAddr, strconv.Itoa(n.Port)))
		// Two nodes at one address is a configuration the table cannot reverse,
		// so NodeAt would have to guess between them.
		if other, ok := r.nodes[addrKeyOf(addr)]; ok {
			return nil, fmt.Errorf("rpc: nodes %d and %d are both at %s %s", other, n.ID, addr.Network(), addr)
		}
		r.routes[n.ID] = Route{Transport: tr, Addr: addr}
		r.nodes[addrKeyOf(addr)] = n.ID
	}

	return r, nil
}

// Route is the route to a node. A node with no route is one nothing dials:
// this node itself, or a gateway.
func (r *Resolver) Route(remote config.NodeID) (Route, error) {
	route, ok := r.routes[remote]
	if !ok {
		return Route{}, fmt.Errorf("rpc: no route to node %d", remote)
	}
	return route, nil
}

// NodeAt reverses the table: the node whose dialable address this is, if any.
// An address naming no node — an ephemeral socket, a rewritten one — is a
// false rather than an error, since callers ask in order to decide.
func (r *Resolver) NodeAt(addr net.Addr) (config.NodeID, bool) {
	id, ok := r.nodes[addrKeyOf(addr)]
	return id, ok
}
