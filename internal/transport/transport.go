package transport

import (
	"context"
	"sync"

	"github.com/mulgadc/predastore/internal/transport/driver"
	"golang.org/x/sync/singleflight"
)

type Peer interface {
	ID() string
	Dial(ctx context.Context) (driver.Conn, error)
}

type PeerResolver interface {
	LookupID(id string) (Peer, error)
}

type Transport struct {
	id    string
	peers PeerResolver
	lns   []driver.Listener

	mu    sync.RWMutex
	conns map[string]*Conn
	sf    singleflight.Group
}

func New(id string, peers PeerResolver, lns ...driver.Listener) *Transport {
	return &Transport{
		id:    id,
		peers: peers,
		lns:   lns,
		conns: make(map[string]*Conn),
	}
}

func (t *Transport) Dial(ctx context.Context, peerID string) (*Conn, error) {
	peer, err := t.peers.LookupID(peerID)
	if err != nil {
		return nil, err
	}

	if conn, ok := t.fetch(peer.ID()); ok {
		return conn, nil
	}

	sh := t.sf.DoChan(peer.ID(), func() (any, error) {
		if conn, ok := t.fetch(peer.ID()); ok {
			return conn, nil
		}

		dConn, err := peer.Dial(ctx)
		if err != nil {
			return nil, err
		}

		return nil, nil
	})

	return nil, nil
}

func (t *Transport) fetch(peer string) (*Conn, bool) {
	t.mu.RLock()
	conn, ok := t.conns[peer]
	t.mu.RUnlock()

	if ok && conn.Context().Err() == nil {
		return conn, true
	}

	return nil, false
}

func (t *Transport) register(peer PeerID, dConn driver.Conn) (*Conn, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if old, ok := t.conns[peer]; ok {
		switch t.tiebreak(old.ID, peer) {
		case old.ID:

		}
	}
}

type Conn struct {
	dialed bool
	ctx    context.Context
}

func (c *Conn) Context() context.Context { return c.ctx }

type Stream = driver.Stream
type StreamErrorCode = driver.StreamErrorCode
