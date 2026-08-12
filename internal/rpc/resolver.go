package rpc

import (
	"fmt"
	"net"
	"strconv"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/transport"
)

// Route is how one node is reached: the transport that dials it and the
// address to dial.
type Route struct {
	Transport transport.Transport
	Addr      net.Addr
}

// addrKey identifies an address by its network as well as its host:port. A
// node's pipe and quic addresses differ only by network, so host:port alone
// would name two routes.
type addrKey struct {
	network  string
	hostPort string
}

func addrKeyOf(a net.Addr) addrKey { return addrKey{a.Network(), a.String()} }

// Resolver is the flat table of routes one node dials, built once from the
// configuration. It is the only place a node id becomes an address.
type Resolver struct {
	routes map[config.NodeID]Route
	nodes  map[addrKey]config.NodeID
}

// NewResolver builds the table source dials from. Nodes on source's own host
// are reached over the pipe and every other node over quic; gates are left out,
// since nothing dials one. A route with no matching transport fails here.
func NewResolver(cfg *config.Config, source config.NodeID, trs ...transport.Transport) (*Resolver, error) {
	byNetwork := make(map[string]transport.Transport, len(trs))
	for _, tr := range trs {
		byNetwork[tr.Network()] = tr
	}

	var localHost config.HostID
	var found bool
	for _, h := range cfg.Hosts {
		for _, n := range h.Nodes {
			if n.ID == source {
				localHost, found = h.ID, true
			}
		}
	}
	if !found {
		return nil, fmt.Errorf("node %d is not in the configuration", source)
	}

	r := &Resolver{
		routes: make(map[config.NodeID]Route),
		nodes:  make(map[addrKey]config.NodeID),
	}
	for _, h := range cfg.Hosts {
		for _, n := range h.Nodes {
			if n.ID == source || n.Role == config.RoleGate {
				continue
			}
			network := transport.NetworkQUIC
			if h.ID == localHost {
				network = transport.NetworkPipe
			}
			tr, ok := byNetwork[string(network)]
			if !ok {
				return nil, fmt.Errorf("node %d needs a %s transport", n.ID, network)
			}

			addr := transport.NewAddr(network, net.JoinHostPort(h.Addr, strconv.Itoa(n.Port)))
			// Two nodes at one address leaves NodeAt guessing between them.
			if other, ok := r.nodes[addrKeyOf(addr)]; ok {
				return nil, fmt.Errorf("nodes %d and %d are both at %s %s", other, n.ID, addr.Network(), addr)
			}
			r.routes[n.ID] = Route{Transport: tr, Addr: addr}
			r.nodes[addrKeyOf(addr)] = n.ID
		}
	}

	return r, nil
}

// Route is the route to a node. A node with no route is one nothing dials:
// this node itself, or a gate.
func (r *Resolver) Route(remote config.NodeID) (Route, error) {
	route, ok := r.routes[remote]
	if !ok {
		return Route{}, fmt.Errorf("no route to node %d", remote)
	}
	return route, nil
}

// NodeAt reverses the table: the node whose dialable address this is, if any.
// An address naming no node — an ephemeral socket, a rewritten one — returns
// false rather than an error, since callers ask in order to decide.
func (r *Resolver) NodeAt(addr net.Addr) (config.NodeID, bool) {
	id, ok := r.nodes[addrKeyOf(addr)]
	return id, ok
}
