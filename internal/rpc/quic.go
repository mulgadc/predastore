package rpc

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"golang.org/x/sync/singleflight"
)

const (
	handshakeIdleTimeout time.Duration = 5 * time.Second
	keepAlivePeriod      time.Duration = 15 * time.Second
	maxIdleTimeout       time.Duration = 60 * time.Second

	maxIncomingStreams int64 = 1000

	initialStreamReceiveWindow uint64 = 2 * 1024 * 1024
	maxStreamReceiveWindow     uint64 = 8 * 1024 * 1024

	initialConnectionReceiveWindow uint64 = 16 * 1024 * 1024
	maxConnectionReceiveWindow     uint64 = 128 * 1024 * 1024
)

type QUICTransport struct {
	raw *quic.Transport

	sf singleflight.Group

	mu    sync.RWMutex
	conns map[string]*quic.Conn
}

func NewQUICTransport(addr *net.UDPAddr) (*QUICTransport, error) {
	conn, err := net.ListenUDP(addr.Network(), addr)
	if err != nil {
		return nil, err
	}

	return &QUICTransport{
		raw:   &quic.Transport{Conn: conn},
		conns: make(map[string]*quic.Conn),
	}, nil
}

func (qt *QUICTransport) Dial(ctx context.Context, addr net.Addr) (net.Conn, error) {
	// TODO: Key on TLS material instead of naive addr.
	key := addr.String()

	qt.mu.RLock()
	conn, ok := qt.conns[key]
	qt.mu.RUnlock()
	if ok {
		stream, err := conn.OpenStream()
		if err != nil {
			// TODO: Evict peer from pool here.
			return nil, err
		}

		return quicConn{Stream: stream, lAddr: qt.raw.Conn.LocalAddr(), rAddr: addr}, nil
	}

	done := qt.sf.DoChan(key, func() (any, error) {
		qt.mu.RLock()
		conn, ok := qt.conns[key]
		if ok {
			stream, err := conn.OpenStream()
			if err != nil {
				// TODO: Evict conn from pool here.
				return nil, err
			}

			return stream, nil
		}

		// TODO: Fill out TLS config.
		conn, err := qt.raw.Dial(context.TODO(), addr, &tls.Config{}, &quic.Config{})

		return nil, nil
	})

	select {
	case result := <-done:
		stream, ok := result.Val.(*quic.Stream)
		if !ok {
			return nil, &net.OpError{Op: "dial", Net: "quic", Addr: addr, Source: qt.raw.Conn.LocalAddr(), Err: result.Err}
		}

		return quicConn{Stream: stream, lAddr: qt.raw.Conn.LocalAddr(), rAddr: addr}, nil

	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (qt *QUICTransport) getConn(key string) (*quic.Conn, bool) {
	qt.mu.RLock()
	defer qt.mu.RUnlock()

	conn, ok := qt.conns[key]
	if ok {
		return
	}

	return nil, true
}

// TODO: Make quicConn properly satisfy the net.Conn contract.
type quicConn struct {
	*quic.Stream
	lAddr, rAddr net.Addr
}

func (qs quicConn) LocalAddr() net.Addr  { return qs.lAddr }
func (qs quicConn) RemoteAddr() net.Addr { return qs.rAddr }
