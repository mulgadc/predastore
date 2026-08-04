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
	case string(NetworkQUIC):
		return newQUICAddr(addr), nil
	default:
		return nil, UnknownNetworkError(network)
	}
}

// Transport carries streams over one network. A process creates one instance
// per network and uses it for every node it runs: Listen may be called
// repeatedly for different addresses, and implementations share whatever
// underlying resources that allows.
type Transport interface {
	// Network names the network this transport serves, matching the Network()
	// of every address it accepts.
	Network() string
	Dial(ctx context.Context, addr net.Addr) (Conn, error)
	// Listen serves addr. Calling it again for a different address adds
	// another listener rather than replacing the first.
	Listen(addr net.Addr) (Listener, error)
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

// ConnErrorCode is reported to the peer when a connection is closed with a
// reason; see the transport's ConnCode constants.
type ConnErrorCode uint64

type Conn interface {
	AcceptStream(ctx context.Context) (Stream, error)
	OpenStream(ctx context.Context) (Stream, error)
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	Context() context.Context
	Close() error // Idempotent
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
