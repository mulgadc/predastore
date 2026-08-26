package rpc

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

var ErrClientClosed = errors.New("client closed")
var ErrUnknownAddr = errors.New("unknown peer address")

type entry struct {
	conn   Conn
	dialed bool
}

type Endpoint struct {
	res Resolver

	mu      sync.RWMutex
	conns   map[NodeID]*entry
	adopted chan Conn

	sf singleflight.Group
}

func New(self NodeID, res Resolver) *Endpoint {
	return &Endpoint{
		res:   res,
		conns: make(map[NodeID]*entry),
	}
}

func (e *Endpoint) Dial(ctx context.Context, addr net.Addr) (Conn, error) {
	peer, ok := e.res.LookupAddr(addr)
	if !ok {
		return nil, ErrUnknownAddr
	}

	if conn, ok := e.getConn(peer.ID()); ok {
		return conn, nil
	}

	ch := e.sf.DoChan(string(peer.ID()), func() (any, error) {
		if conn, ok := e.getConn(peer); ok {
			return conn, nil
		}

		conn, err := e.res(peer).Dial(ctx)
		if err != nil {
			return nil, err
		}

	})

	return conn, nil
}

func (e *Endpoint) getConn(key any) (Conn, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	entry, ok := e.conns[key]
	if ok && entry.conn.Context().Err() == nil {
		return entry.conn, true
	}

	return nil, false
}

func (e *Endpoint) putConn(key any, conn Conn, dialed bool) (Conn, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

}
