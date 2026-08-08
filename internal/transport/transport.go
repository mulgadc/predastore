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

// Addr names one endpoint: the network carrying it and the host:port that
// selects it there. Both transports address their endpoints this way.
type Addr struct {
	network  Network
	hostPort string
}

func NewAddr(network Network, hostPort string) *Addr {
	return &Addr{network: network, hostPort: hostPort}
}

func (a *Addr) Network() string { return string(a.network) }
func (a *Addr) String() string  { return a.hostPort }

// Transport carries streams over one network for one node: it owns a single
// endpoint, bound at construction, that it both dials from and listens on.
type Transport interface {
	// Network names the network this transport serves, matching the Network()
	// of every address it accepts.
	Network() string
	Dial(ctx context.Context, remote net.Addr) (Conn, error)
	// Listen serves this transport's own endpoint. It may be called once.
	Listen() (Listener, error)
	// Addr is the endpoint the transport bound, which differs from the one
	// requested when the request named port 0.
	Addr() net.Addr
	Close() error
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
// reason. ConnCodeShutdown is the only one a transport sends.
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
