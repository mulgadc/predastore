package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/mulgadc/predastore/internal/tlsconfig"
	"github.com/mulgadc/predastore/quic/quicconf"
	"github.com/quic-go/quic-go"
)

const NetworkQUIC Network = "quic"

// alpn is the intra-cluster application protocol; both sides must advertise
// it or the TLS handshake fails.
const alpn = "mulga-repl-v1"

type QUICAddr struct {
	host string
}

func newQUICAddr(host string) *QUICAddr {
	return &QUICAddr{host: host}
}

func (qa *QUICAddr) Network() string {
	return string(NetworkQUIC)
}

func (qa *QUICAddr) String() string {
	return qa.host
}

var _ Transport = (*QUICTransport)(nil)

type QUICTransportConfig struct {
	// BindAddr is the local UDP address ("host:port") that Listen binds.
	BindAddr string
	// TLSCert and TLSKey are paths to the server certificate keypair,
	// required by Listen. A dial-only transport can leave them empty.
	TLSCert string
	TLSKey  string
	// RootCAs verifies peer certificates when dialing; nil uses the OS
	// trust store.
	RootCAs *x509.CertPool
}

// QUICTransport carries streams between hosts over QUIC with TLS 1.3.
// Intra-cluster auth is the server certificate: dialers verify against
// RootCAs (or the OS trust store) and both sides pin the cluster ALPN.
type QUICTransport struct {
	cfg QUICTransportConfig
	qa  *QUICAddr
}

func NewQUICTransport(cfg QUICTransportConfig) *QUICTransport {
	return &QUICTransport{cfg: cfg, qa: newQUICAddr(cfg.BindAddr)}
}

func (qt *QUICTransport) Addr() net.Addr { return qt.qa }

func (qt *QUICTransport) Listen() (Listener, error) {
	if qt.cfg.BindAddr == "" {
		return nil, &net.OpError{Op: "listen", Net: string(NetworkQUIC), Err: ErrMissingAddr}
	}
	if qt.cfg.TLSCert == "" || qt.cfg.TLSKey == "" {
		return nil, fmt.Errorf("quic transport: tls cert and key are required to listen")
	}
	cert, err := tls.LoadX509KeyPair(qt.cfg.TLSCert, qt.cfg.TLSKey)
	if err != nil {
		return nil, fmt.Errorf("load x509 key pair from %s/%s: %w", qt.cfg.TLSCert, qt.cfg.TLSKey, err)
	}
	tlsConf := &tls.Config{
		Certificates:     []tls.Certificate{cert},
		MinVersion:       tls.VersionTLS13,
		CurvePreferences: tlsconfig.Curves,
		NextProtos:       []string{alpn},
	}

	ln, err := quic.ListenAddr(qt.cfg.BindAddr, tlsConf, &quic.Config{
		KeepAlivePeriod:                15 * time.Second,
		MaxIdleTimeout:                 60 * time.Second,
		MaxIncomingStreams:             1000,
		MaxIncomingUniStreams:          1000,
		InitialStreamReceiveWindow:     quicconf.InitialStreamReceiveWindow,
		MaxStreamReceiveWindow:         quicconf.MaxStreamReceiveWindow,
		InitialConnectionReceiveWindow: quicconf.InitialConnectionReceiveWindow,
		MaxConnectionReceiveWindow:     quicconf.MaxConnectionReceiveWindow,
	})
	if err != nil {
		return nil, fmt.Errorf("quic listen on %s: %w", qt.cfg.BindAddr, err)
	}
	return &QUICListener{ln: ln}, nil
}

func (qt *QUICTransport) Dial(ctx context.Context, addr net.Addr) (Conn, error) {
	if addr == nil {
		return nil, &net.OpError{Op: "dial", Net: string(NetworkQUIC), Source: qt.qa, Err: ErrMissingAddr}
	}
	// Strict server verification: RootCAs nil means the OS trust store, and
	// ServerName is derived from the dial address by the stdlib.
	tlsConf := &tls.Config{
		RootCAs:          qt.cfg.RootCAs,
		MinVersion:       tls.VersionTLS13,
		CurvePreferences: tlsconfig.Curves,
		NextProtos:       []string{alpn},
	}
	conn, err := quic.DialAddr(ctx, addr.String(), tlsConf, &quic.Config{
		HandshakeIdleTimeout:           5 * time.Second,
		KeepAlivePeriod:                15 * time.Second,
		MaxIdleTimeout:                 60 * time.Second,
		InitialStreamReceiveWindow:     quicconf.InitialStreamReceiveWindow,
		MaxStreamReceiveWindow:         quicconf.MaxStreamReceiveWindow,
		InitialConnectionReceiveWindow: quicconf.InitialConnectionReceiveWindow,
		MaxConnectionReceiveWindow:     quicconf.MaxConnectionReceiveWindow,
	})
	if err != nil {
		return nil, fmt.Errorf("quic dial %s: %w", addr, err)
	}
	return &QUICConn{conn: conn}, nil
}

var _ Listener = (*QUICListener)(nil)

type QUICListener struct {
	ln *quic.Listener
}

func (ql *QUICListener) Accept(ctx context.Context) (Conn, error) {
	conn, err := ql.ln.Accept(ctx)
	if err != nil {
		if errors.Is(err, quic.ErrServerClosed) {
			return nil, &net.OpError{Op: "accept", Net: string(NetworkQUIC), Addr: ql.Addr(), Err: ErrListenerClosed}
		}
		return nil, err
	}
	return &QUICConn{conn: conn}, nil
}

// Addr reports the bound address in the transport's quic namespace, which
// may differ from the configured BindAddr when binding port 0.
func (ql *QUICListener) Addr() net.Addr {
	return newQUICAddr(ql.ln.Addr().String())
}

func (ql *QUICListener) Close() error {
	return ql.ln.Close()
}

var _ Conn = (*QUICConn)(nil)

type QUICConn struct {
	conn *quic.Conn
}

func (qc *QUICConn) LocalAddr() net.Addr  { return qc.conn.LocalAddr() }
func (qc *QUICConn) RemoteAddr() net.Addr { return qc.conn.RemoteAddr() }

func (qc *QUICConn) OpenStream(ctx context.Context) (Stream, error) {
	s, err := qc.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return &QUICStream{s: s, laddr: qc.conn.LocalAddr(), raddr: qc.conn.RemoteAddr()}, nil
}

func (qc *QUICConn) AcceptStream(ctx context.Context) (Stream, error) {
	s, err := qc.conn.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}
	return &QUICStream{s: s, laddr: qc.conn.LocalAddr(), raddr: qc.conn.RemoteAddr()}, nil
}

func (qc *QUICConn) Close() error {
	return qc.conn.CloseWithError(0, "")
}

var _ Stream = (*QUICStream)(nil)

type QUICStream struct {
	s *quic.Stream
	// quic streams carry no addresses of their own; these are the parent
	// connection's, captured for logging.
	laddr net.Addr
	raddr net.Addr
}

func (qs *QUICStream) LocalAddr() net.Addr  { return qs.laddr }
func (qs *QUICStream) RemoteAddr() net.Addr { return qs.raddr }

func (qs *QUICStream) Read(p []byte) (int, error) {
	n, err := qs.s.Read(p)
	return n, translateQUICErr(err)
}

func (qs *QUICStream) Write(p []byte) (int, error) {
	n, err := qs.s.Write(p)
	return n, translateQUICErr(err)
}

// Close closes the write side: the peer observes io.EOF after draining. The
// read side stays open.
func (qs *QUICStream) Close() error {
	return qs.s.Close()
}

func (qs *QUICStream) CancelRead(code StreamErrorCode) {
	qs.s.CancelRead(quic.StreamErrorCode(code))
}

func (qs *QUICStream) CancelWrite(code StreamErrorCode) {
	qs.s.CancelWrite(quic.StreamErrorCode(code))
}

func (qs *QUICStream) ReadFrom(r io.Reader) (int64, error) {
	n, err := io.Copy(qs.s, r)
	return n, translateQUICErr(err)
}

func (qs *QUICStream) WriteTo(w io.Writer) (int64, error) {
	n, err := io.Copy(w, qs.s)
	return n, translateQUICErr(err)
}

// translateQUICErr maps quic-go stream aborts onto the transport's
// StreamError so callers handle one error surface across transports.
func translateQUICErr(err error) error {
	if err == nil {
		return nil
	}
	if se, ok := errors.AsType[*quic.StreamError](err); ok {
		return &StreamError{Code: StreamErrorCode(se.ErrorCode)}
	}
	return err
}
