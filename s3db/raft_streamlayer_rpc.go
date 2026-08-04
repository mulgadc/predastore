package s3db

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/mulgadc/predastore/internal/transport"
)

// NetworkRaft names the raft advertise address namespace used when raft runs
// over the rpc stream layer: addresses identify nodes, not sockets, and the
// dial function resolves them to a transport.
const NetworkRaft = "raft"

// raftAddr is a node-identifying advertise address ("node-3"); routing to a
// socket happens in the dial function, not here.
type raftAddr string

func (a raftAddr) Network() string { return NetworkRaft }
func (a raftAddr) String() string  { return string(a) }

// RaftDialFunc opens a stream to the state replica behind a raft server
// address. The caller owns opcode and header framing, so the layer stays
// agnostic of both the rpc protocol and the topology.
type RaftDialFunc func(ctx context.Context, address raft.ServerAddress) (transport.Stream, error)

var _ raft.StreamLayer = (*RPCStreamLayer)(nil)

// RPCStreamLayer carries raft traffic over rpc streams. Outbound connections
// go through the dial function; inbound streams are handed over by the rpc
// handler via Deliver.
type RPCStreamLayer struct {
	advertise raftAddr
	dial      RaftDialFunc

	accept chan net.Conn
	closed chan struct{}
	once   sync.Once
}

func NewRPCStreamLayer(advertise string, dial RaftDialFunc) *RPCStreamLayer {
	return &RPCStreamLayer{
		advertise: raftAddr(advertise),
		dial:      dial,
		accept:    make(chan net.Conn),
		closed:    make(chan struct{}),
	}
}

func (l *RPCStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	ctx := context.Background()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	stream, err := l.dial(ctx, address)
	if err != nil {
		return nil, err
	}
	return transport.NewStreamConn(stream), nil
}

func (l *RPCStreamLayer) Accept() (net.Conn, error) {
	select {
	case <-l.closed:
		return nil, net.ErrClosed
	case conn := <-l.accept:
		return conn, nil
	}
}

func (l *RPCStreamLayer) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

// Addr returns the node-identifying advertise address; peers store it in the
// raft configuration and pass it back to Dial.
func (l *RPCStreamLayer) Addr() net.Addr { return l.advertise }

// Deliver hands an inbound stream to the raft transport and blocks until the
// connection is closed, keeping the rpc stream open for its lifetime. It is
// called from the rpc handler serving raft dial requests.
func (l *RPCStreamLayer) Deliver(ctx context.Context, stream transport.Stream) error {
	conn := transport.NewStreamConn(stream)
	select {
	case <-ctx.Done():
		conn.Close()
		return ctx.Err()
	case <-l.closed:
		conn.Close()
		return net.ErrClosed
	case l.accept <- conn:
	}

	// Raft owns the connection now; hold the stream open until it lets go.
	select {
	case <-conn.Done():
		return nil
	case <-ctx.Done():
		conn.Close()
		return ctx.Err()
	}
}
