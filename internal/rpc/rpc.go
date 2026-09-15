package rpc

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"time"
)

const DefaultReadOpcodeTimeout = 2 * time.Second

type NodeID string

type Node interface {
	ID() NodeID
	Addrs() []net.Addr
}

type Resolver interface {
	Lookup(id NodeID) (Node, bool)
}

type Dialer interface {
	Dial(ctx context.Context, peer NodeID) (net.Conn, error)
}

type Listener interface {
	Accept() (net.Conn, error)
	Close() error
}

type Server interface {
	Serve(l Listener)
	Shutdown(ctx context.Context) error
	Close() error
}
