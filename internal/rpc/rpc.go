package rpc

import (
	"errors"
	"net"
)

const maxHeaderSize = 1024 * 1024

var ErrHeaderTooLarge = errors.New("header too large")

type Opcode uint32

type Header interface {
	Append(buf []byte) ([]byte, error)
	Unmarshal([]byte) error
}

// Topology maps node ids to the addresses this package dials and binds. It is
// the only place a node id becomes an address: callers name peers by id and
// never handle an address themselves. internal/topology implements it.
type Topology interface {
	// NodeAddr is the address to dial to reach the node, wherever it runs.
	NodeAddr(nodeID int) (net.Addr, error)
	// ListenAddrs are the addresses a node running in this process serves.
	ListenAddrs(nodeID int) ([]net.Addr, error)
}
