package transport

import (
	"context"
	"errors"
	"io"
	"net"
)

var ErrMissingAddr = errors.New("missing address")
var ErrAddrAlreadyInUse = errors.New("address already in use")
var ErrTransportClosed = errors.New("transport closed")
var ErrListenerClosed = errors.New("listener closed")
var ErrConnClosed = errors.New("connection closed")
var ErrNoListener = errors.New("no listener")

type UnknownNetworkError string

func (e UnknownNetworkError) Error() string { return "unknown network " + string(e) }

type Network string

func ResolveAddr(network, addr string) (net.Addr, error) {
	switch network {
	case string(NetworkPipe):
		return newPipeAddr(addr), nil
	default:
		return nil, UnknownNetworkError(network)
	}
}

type Transport interface {
	Dial(ctx context.Context, addr net.Addr) (Conn, error)
	Listen() (Listener, error)
	Addr() net.Addr
}

type Listener interface {
	// Accept returns new connections. It should be called in a loop.
	Accept(ctx context.Context) (Conn, error)
	// Addr returns the local network address that the server is listening on.
	Addr() net.Addr
	// Close closes the listener. Accept will return ErrListenerClosed as soon as
	// all connections in the accept queue have been accepted. Already established
	// (accepted) connections will be unaffected.
	Close() error
}

type Conn interface {
	AcceptStream(ctx context.Context) (Stream, error)
	OpenStream(ctx context.Context) (Stream, error)
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	Close() error
}

type StreamErrorCode uint64

type Stream interface {
	io.ReadWriteCloser
	io.ReaderFrom
	io.WriterTo
	CancelRead(code StreamErrorCode)
	CancelWrite(code StreamErrorCode)
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
}
