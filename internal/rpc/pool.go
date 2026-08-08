package rpc

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/transport"
	"golang.org/x/sync/singleflight"
)

// ErrPoolClosed is returned by Dial once the pool has been closed. A closed
// pool is not reusable: it never dials again, so callers that need to
// reconnect must build a new one.
var ErrPoolClosed = errors.New("connection pool closed")

type PoolOption func(*ConnPool)

// WithDialTimeout bounds a single dial. It does not bound Dial, which stops
// when its own context does.
func WithDialTimeout(d time.Duration) PoolOption {
	return func(p *ConnPool) { p.dialTimeout = d }
}

// ConnPool holds one connection per peer node, whichever side opened it, and
// owns every connection it holds: nothing else closes one, and a connection
// leaves only through Evict or Close.
//
// It is keyed by node id rather than address so that a connection accepted
// from a peer and one dialed to it are the same entry on either transport.
type ConnPool struct {
	source      config.NodeID
	res         *Resolver
	dialTimeout time.Duration
	sf          singleflight.Group

	mu     sync.RWMutex
	conns  map[config.NodeID]transport.Conn
	closed bool
}

func NewConnPool(source config.NodeID, res *Resolver, opts ...PoolOption) *ConnPool {
	const defaultDialTimeout = 15 * time.Second
	p := &ConnPool{
		source:      source,
		res:         res,
		dialTimeout: defaultDialTimeout,
		conns:       make(map[config.NodeID]transport.Conn),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Dial returns the connection to a node, opening one if the pool holds none.
// The transport comes off the route, so dialing is a table lookup and nothing
// else; concurrent dials to one peer collapse onto a single connection.
func (p *ConnPool) Dial(ctx context.Context, remote config.NodeID) (transport.Conn, error) {
	p.mu.RLock()
	conn, closed := p.conns[remote], p.closed
	p.mu.RUnlock()
	if closed {
		return nil, ErrPoolClosed
	}
	if conn != nil && conn.Context().Err() == nil {
		return conn, nil
	}

	ch := p.sf.DoChan(strconv.FormatUint(uint64(remote), 10), func() (any, error) {
		p.mu.RLock()
		conn, closed := p.conns[remote], p.closed
		p.mu.RUnlock()
		if closed {
			return nil, ErrPoolClosed
		}
		if conn != nil && conn.Context().Err() == nil {
			return conn, nil
		}

		route, err := p.res.Route(remote)
		if err != nil {
			return nil, err
		}

		dialCtx, cancelTimeout := context.WithTimeout(context.Background(), p.dialTimeout)
		conn, err = route.Transport.Dial(dialCtx, route.Addr)
		cancelTimeout()
		if err != nil {
			return nil, fmt.Errorf("dial node %d: %w", remote, err)
		}

		p.mu.Lock()
		defer p.mu.Unlock()
		if p.closed {
			conn.Close()
			return nil, ErrPoolClosed
		}
		if stale := p.conns[remote]; stale != nil {
			stale.Close()
		}
		p.conns[remote] = conn
		return conn, nil
	})

	select {
	case res := <-ch:
		// A failed dial carries no connection, so the assertion below only
		// holds once the error is out of the way.
		if res.Err != nil {
			return nil, res.Err
		}
		conn, ok := res.Val.(transport.Conn)
		if !ok {
			panic("singleflight did not return a valid Conn")
		}
		return conn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Donate offers an accepted connection to the pool, which takes ownership when
// its remote names a node. A false return leaves the caller owning it: a peer
// dialing from an ephemeral socket names no node, so nothing would ever ask.
func (p *ConnPool) Donate(c transport.Conn) bool {
	remote, ok := p.res.NodeAt(c.RemoteAddr())
	if !ok {
		return false
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	if stale := p.conns[remote]; stale != nil && stale != c {
		stale.Close()
	}
	p.conns[remote] = c
	return true
}

// Evict drops a connection and closes it. It is the only way a connection
// leaves the pool short of Close, so both the dialing and the accepting side
// release one through here.
func (p *ConnPool) Evict(c transport.Conn) {
	p.mu.Lock()
	for remote, held := range p.conns {
		if held == c {
			delete(p.conns, remote)
			break
		}
	}
	p.mu.Unlock()
	c.Close()
}

// Close closes every pooled connection. It is idempotent, and whoever built
// the node calls it once both its server and its client have stopped.
func (p *ConnPool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	conns := p.conns
	p.conns = make(map[config.NodeID]transport.Conn)
	p.mu.Unlock()

	errs := make([]error, 0, len(conns))
	for _, conn := range conns {
		if err := conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}
