package rpc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/transport"
	"golang.org/x/sync/singleflight"
)

// ErrClientClosed is returned by OpenStream once the client has been closed.
// A closed client is not reusable: it never dials again, so callers that need
// to reconnect must build a new one.
var ErrClientClosed = errors.New("client closed")

func poolKey(addr net.Addr) string { return addr.Network() + "|" + addr.String() }

type ClientOption func(*Client)

// WithDialTimeout bounds a single dial. It does not bound OpenStream, which
// stops when its own context does.
func WithDialTimeout(d time.Duration) ClientOption {
	return func(c *Client) { c.dialTimeout = d }
}

// Client opens streams to nodes named by id. The resolver turns an id into
// the route that reaches it, so the client holds no notion of a network.
type Client struct {
	res         *Resolver
	dialTimeout time.Duration
	sf          singleflight.Group

	mu     sync.RWMutex
	pool   map[string]transport.Conn
	closed bool
}

func NewClient(res *Resolver, opts ...ClientOption) *Client {
	const defaultDialTimeout = 15 * time.Second
	c := &Client{
		res:         res,
		dialTimeout: defaultDialTimeout,
		pool:        make(map[string]transport.Conn),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// OpenStream opens a stream to a node, addressed by id. Whether that node is
// reached over the in-process pipe or the network follows from the address the
// resolver returns, which no caller sees.
func OpenStream[T Header](
	ctx context.Context,
	c *Client,
	nodeID config.NodeID,
	op Opcode,
	header T,
) (transport.Stream, error) {
	// Dial connection.
	conn, addr, err := c.dial(ctx, nodeID)
	if err != nil {
		return nil, fmt.Errorf("dial connection: %w", err)
	}

	// Build stream metadata
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf, uint32(op))
	buf, err = header.Append(buf)
	if err != nil {
		return nil, fmt.Errorf("encode header: %w", err)
	}
	n := len(buf) - 8
	if n > maxHeaderSize {
		return nil, fmt.Errorf("%w: %v", ErrHeaderTooLarge, n)
	}
	binary.BigEndian.PutUint32(buf[4:8], uint32(n)) //nolint:gosec // G115: n is bounded by maxHeaderSize above.

	// Open stream.
	stream, err := conn.OpenStream(ctx)
	if err != nil {
		if conn.Context().Err() != nil {
			// TODO: Define isConnDead function for smart evict.
			c.evict(addr, conn)
		}
		return nil, err
	}

	// Write stream metadata
	if _, err := stream.Write(buf); err != nil {
		if conn.Context().Err() != nil {
			// TODO: Define isConnDead function for smart evict.
			c.evict(addr, conn)
		} else {
			// TODO: Figure out better error codes.
			stream.CancelRead(0)
			stream.CancelWrite(0)
		}
		return nil, fmt.Errorf("write metadata + header: %w", err)
	}

	return stream, nil
}

// dial resolves the node through the resolver and returns a pooled connection
// to it, along with the address it resolved to so callers can evict it.
func (c *Client) dial(ctx context.Context, nodeID config.NodeID) (transport.Conn, net.Addr, error) {
	if c.res == nil {
		return nil, nil, fmt.Errorf("client has no resolver")
	}
	route, err := c.res.Route(nodeID)
	if err != nil {
		return nil, nil, err
	}
	addr := route.Addr

	key := poolKey(addr)
	c.mu.RLock()
	conn, closed := c.pool[key], c.closed
	c.mu.RUnlock()
	if closed {
		return nil, nil, ErrClientClosed
	}
	if conn != nil && conn.Context().Err() == nil {
		return conn, addr, nil
	}

	ch := c.sf.DoChan(key, func() (any, error) {
		c.mu.RLock()
		conn := c.pool[key]
		c.mu.RUnlock()
		if conn != nil && conn.Context().Err() == nil {
			return conn, nil
		}

		dialCtx, cancelTimeout := context.WithTimeout(context.Background(), c.dialTimeout)
		conn, err := route.Transport.Dial(dialCtx, addr)
		cancelTimeout()
		if err != nil {
			return nil, fmt.Errorf("dial target address: %w", err)
		}

		c.mu.Lock()
		defer c.mu.Unlock()
		if c.closed {
			conn.Close()
			return nil, ErrClientClosed
		}

		if stale := c.pool[key]; stale != nil {
			stale.Close()
		}

		c.pool[key] = conn
		return conn, nil
	})

	select {
	case res := <-ch:
		// A failed dial carries no connection, so the assertion below only
		// holds once the error is out of the way.
		if res.Err != nil {
			return nil, nil, res.Err
		}
		conn, ok := res.Val.(transport.Conn)
		if !ok {
			panic("singleflight did not return a valid Conn")
		}
		return conn, addr, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (c *Client) evict(addr net.Addr, conn transport.Conn) {
	key := poolKey(addr)
	c.mu.Lock()
	if c.pool[key] == conn {
		delete(c.pool, key)
	}
	c.mu.Unlock()
	conn.Close()
}

func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true

	pool := c.pool
	c.pool = make(map[string]transport.Conn)
	c.mu.Unlock()

	errs := make([]error, 0, len(pool))
	for _, conn := range pool {
		if err := conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}
