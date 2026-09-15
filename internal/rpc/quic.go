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

type QUICTransport struct {
	ctx    context.Context
	cancel context.CancelFunc

	tlsConf  *tls.Config
	quicConf *quic.Config
	raw      *quic.Transport

	mu    sync.RWMutex
	sf    singleflight.Group
	conns map[string]*quic.Conn
}

func NewQUICTransport(addr *net.UDPAddr, tlsConf *tls.Config) (*QUICTransport, error) {
	conn, err := net.ListenUDP(addr.Network(), addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	quicConf := &quic.Config{
		HandshakeIdleTimeout: 5 * time.Second,
		KeepAlivePeriod:      15 * time.Second,
		MaxIdleTimeout:       60 * time.Second,

		MaxIncomingStreams:    1000,
		MaxIncomingUniStreams: 1000,

		InitialStreamReceiveWindow:     2 * 1024 * 1024,   // 2MB
		MaxStreamReceiveWindow:         8 * 1024 * 1024,   // 8MB
		InitialConnectionReceiveWindow: 16 * 1024 * 1024,  // 16MB
		MaxConnectionReceiveWindow:     128 * 1024 * 1024, // 128MB
	}

	return &QUICTransport{
		ctx:    ctx,
		cancel: cancel,

		tlsConf:  tlsConf,
		quicConf: quicConf,
		raw:      &quic.Transport{Conn: conn},

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
			// TODO: Evict conn from pool here.
			return nil, err
		}

		return quicConn{Stream: stream, lAddr: qt.raw.Conn.LocalAddr(), rAddr: addr}, nil
	}

	done := qt.sf.DoChan(key, func() (any, error) {
		qt.mu.RLock()
		conn, ok := qt.conns[key]
		qt.mu.RUnlock()
		if ok {
			return conn, nil
		}

		// This Dial is bounded by the handshake timeout in qt.quicConf. No context
		// deadline is required.
		conn, err := qt.raw.Dial(qt.ctx, addr, qt.tlsConf, qt.quicConf)
		if err != nil {
			return nil, err
		}

		qt.mu.Lock()
		qt.conns[key] = conn
		qt.mu.Unlock()

		return conn, nil
	})

	select {
	case result := <-done:
		conn, ok := result.Val.(*quic.Conn)
		if !ok {
			return nil, &net.OpError{Op: "dial", Net: "quic", Addr: addr, Source: qt.raw.Conn.LocalAddr(), Err: result.Err}
		}

		stream, err := conn.OpenStream()
		if err != nil {
			// TODO: Evict conn from pool here.
			return nil, err
		}

		return quicConn{Stream: stream, lAddr: qt.raw.Conn.LocalAddr(), rAddr: addr}, nil

	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// TODO: Make quicConn properly satisfy the net.Conn contract.
type quicConn struct {
	*quic.Stream
	lAddr, rAddr net.Addr
}

func (qs quicConn) LocalAddr() net.Addr  { return qs.lAddr }
func (qs quicConn) RemoteAddr() net.Addr { return qs.rAddr }
