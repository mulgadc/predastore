package driver

import (
	"context"
	"net"
)

type Driver interface {
	Dialer
	Listener
}

type Dialer interface {
	Dial(ctx context.Context, addr net.Addr) (Conn, error)
}

type Listener interface {
	Addr() net.Addr
	Accept(ctx context.Context) (Conn, error)
	Close() error
}

type Conn interface {
	AcceptStream(ctx context.Context) (Stream, error)
	OpenStream(ctx context.Context) (Stream, error)
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	Context() context.Context
	Close() error
}

type StreamErrorCode uint64

type Stream interface {
	net.Conn
	CancelRead(code StreamErrorCode)
	CancelWrite(code StreamErrorCode)
	Context() context.Context
}
