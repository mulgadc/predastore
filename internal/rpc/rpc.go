package rpc

import (
	"errors"
	"net"

	"github.com/mulgadc/predastore/internal/topology"
)

const maxHeaderSize = 1024 * 1024

var ErrHeaderTooLarge = errors.New("header too large")

type Opcode uint32

type Header interface {
	Append(buf []byte) ([]byte, error)
	Unmarshal([]byte) error
}

// Resolver maps node ids to the addresses this package dials and binds. It is
// the only place a node id becomes an address: callers name peers by id and
// never handle an address themselves. internal/topology implements it.
//
// The node id is topology's, not this package's. rpc already speaks node ids
// in every signature; borrowing the type only makes that visible, and the
// dependency the interface protects runs the other way — topology never
// imports rpc.
type Resolver interface {
	// NodeAddr is the address to dial to reach the node, wherever it runs.
	NodeAddr(nodeID topology.NodeID) (net.Addr, error)
	// ListenAddrs are the addresses a node running in this process serves.
	ListenAddrs(nodeID topology.NodeID) ([]net.Addr, error)
}
