package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/mulgadc/predastore/internal/tlsconfig"
	"github.com/quic-go/quic-go"
)

const NetworkQUIC Network = "quic"

// alpnProto is the intra-cluster application protocol. Both sides pin it, so
// a handshake against anything but a cluster peer fails outright.
const alpnProto = "mulga-repl-v1"

// ConnCodeShutdown is reported to the peer when a connection is closed
// because the listener is going away.
const ConnCodeShutdown ConnErrorCode = 3

var _ Transport = (*QUICTransport)(nil)

type QUICOption func(*QUICTransport)

// WithRootCAs verifies peer certificates against p when dialing. Unset means
// the OS trust store.
func WithRootCAs(p *x509.CertPool) QUICOption {
	return func(qt *QUICTransport) { qt.rootCAs = p }
}

// QUICTransport carries streams to other hosts over QUIC with TLS 1.3, one
// UDP socket per node. Intra-cluster auth is the server certificate: dialers
// verify it against RootCAs, and both sides pin the cluster ALPN.
type QUICTransport struct {
	pc      *net.UDPConn
	tr      *quic.Transport
	cert    tls.Certificate
	rootCAs *x509.CertPool
	addr    *Addr

	mu     sync.Mutex
	ln     *quic.Listener
	closed bool
}

// NewQUICTransport binds a UDP socket at addr:port. Port 0 takes an
// OS-assigned port, which yields a transport that dials but is never dialed.
func NewQUICTransport(addr string, port int, cert, key string, opts ...QUICOption) (*QUICTransport, error) {
	kp, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, fmt.Errorf("load x509 key pair from %s/%s: %w", cert, key, err)
	}
	hostPort := net.JoinHostPort(addr, strconv.Itoa(port))
	udpAddr, err := net.ResolveUDPAddr("udp", hostPort)
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", hostPort, err)
	}
	pc, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, fmt.Errorf("quic bind %s: %w", hostPort, err)
	}

	qt := &QUICTransport{
		pc:   pc,
		tr:   &quic.Transport{Conn: pc},
		cert: kp,
		addr: NewAddr(NetworkQUIC, pc.LocalAddr().String()),
	}
	for _, opt := range opts {
		opt(qt)
	}
	return qt, nil
}

func (qt *QUICTransport) Network() string { return string(NetworkQUIC) }

// Addr is the endpoint the socket bound, which differs from the requested one
// when the request named port 0.
func (qt *QUICTransport) Addr() net.Addr { return qt.addr }

// Listen accepts connections on the transport's own socket. One socket serves
// one node, so it may be called once.
func (qt *QUICTransport) Listen() (Listener, error) {
	qt.mu.Lock()
	defer qt.mu.Unlock()
	if qt.closed {
		return nil, ErrTransportClosed
	}
	if qt.ln != nil {
		return nil, &net.OpError{Op: "listen", Net: string(NetworkQUIC), Addr: qt.addr, Err: ErrAddrAlreadyInUse}
	}

	tlsConf := &tls.Config{
		Certificates:     []tls.Certificate{qt.cert},
		MinVersion:       tls.VersionTLS13,
		CurvePreferences: tlsconfig.Curves,
		NextProtos:       []string{alpnProto},
	}
	ln, err := qt.tr.Listen(tlsConf, listenQUICConfig())
	if err != nil {
		return nil, fmt.Errorf("quic listen on %s: %w", qt.addr, err)
	}
	qt.ln = ln
	return &quicListener{ln: ln, addr: qt.addr}, nil
}

func (qt *QUICTransport) Dial(ctx context.Context, remote net.Addr) (Conn, error) {
	if remote == nil {
		return nil, &net.OpError{Op: "dial", Net: string(NetworkQUIC), Source: qt.addr, Err: ErrMissingAddr}
	}
	if remote.Network() != string(NetworkQUIC) {
		return nil, &net.OpError{Op: "dial", Net: string(NetworkQUIC), Source: qt.addr, Addr: remote, Err: UnknownNetworkError(remote.Network())}
	}
	qt.mu.Lock()
	closed := qt.closed
	qt.mu.Unlock()
	if closed {
		return nil, ErrTransportClosed
	}

	udpAddr, err := net.ResolveUDPAddr("udp", remote.String())
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", remote, err)
	}
	host, _, err := net.SplitHostPort(remote.String())
	if err != nil {
		return nil, fmt.Errorf("split %s: %w", remote, err)
	}

	// Strict server verification: RootCAs nil means the OS trust store, and
	// ServerName is the peer host, which is the certificate subject.
	tlsConf := &tls.Config{
		ServerName:       host,
		RootCAs:          qt.rootCAs,
		MinVersion:       tls.VersionTLS13,
		CurvePreferences: tlsconfig.Curves,
		NextProtos:       []string{alpnProto},
	}
	conn, err := qt.tr.Dial(ctx, udpAddr, tlsConf, dialQUICConfig())
	if err != nil {
		return nil, fmt.Errorf("quic dial %s: %w", remote, err)
	}
	return newQUICConn(conn), nil
}

// Close tears down the listener and the socket. Established connections are
// terminated abruptly, so callers drain them first.
func (qt *QUICTransport) Close() error {
	qt.mu.Lock()
	if qt.closed {
		qt.mu.Unlock()
		return nil
	}
	qt.closed = true
	ln := qt.ln
	qt.mu.Unlock()

	if ln != nil {
		ln.Close()
	}
	err := qt.tr.Close()
	// quic-go leaves a caller-supplied socket open.
	if cerr := qt.pc.Close(); err == nil {
		err = cerr
	}
	return err
}

// QUIC flow-control window sizes, shared by the listen and dial configs so the
// two sides of a connection cannot drift apart.
//
// Sizing targets a worst case of ~30 concurrent streams per connection
// (AWS CLI default 10 concurrent parts × 3 shards) carrying 4 MiB shards, so
// the connection window covers every in-flight shard and no sender blocks on
// flow control. The per-stream window is set above shard size to avoid
// stream-level throttling. Initial windows are raised above the quic-go
// 512 KiB defaults to skip slow-start.
const (
	initialStreamReceiveWindow     uint64 = 2 * 1024 * 1024
	maxStreamReceiveWindow         uint64 = 8 * 1024 * 1024
	initialConnectionReceiveWindow uint64 = 16 * 1024 * 1024
	maxConnectionReceiveWindow     uint64 = 128 * 1024 * 1024
)

func listenQUICConfig() *quic.Config {
	return &quic.Config{
		KeepAlivePeriod:                15 * time.Second,
		MaxIdleTimeout:                 60 * time.Second,
		MaxIncomingStreams:             1000,
		MaxIncomingUniStreams:          1000,
		InitialStreamReceiveWindow:     initialStreamReceiveWindow,
		MaxStreamReceiveWindow:         maxStreamReceiveWindow,
		InitialConnectionReceiveWindow: initialConnectionReceiveWindow,
		MaxConnectionReceiveWindow:     maxConnectionReceiveWindow,
	}
}

func dialQUICConfig() *quic.Config {
	return &quic.Config{
		HandshakeIdleTimeout:           5 * time.Second,
		KeepAlivePeriod:                15 * time.Second,
		MaxIdleTimeout:                 60 * time.Second,
		MaxIncomingStreams:             1000,
		MaxIncomingUniStreams:          1000,
		InitialStreamReceiveWindow:     initialStreamReceiveWindow,
		MaxStreamReceiveWindow:         maxStreamReceiveWindow,
		InitialConnectionReceiveWindow: initialConnectionReceiveWindow,
		MaxConnectionReceiveWindow:     maxConnectionReceiveWindow,
	}
}

var _ Listener = (*quicListener)(nil)

// quicListener is the transport's single listener, wrapping quic-go's.
type quicListener struct {
	ln   *quic.Listener
	addr *Addr
}

func (l *quicListener) Accept(ctx context.Context) (Conn, error) {
	conn, err := l.ln.Accept(ctx)
	if err != nil {
		if errors.Is(err, quic.ErrServerClosed) {
			err = ErrListenerClosed
		}
		return nil, &net.OpError{Op: "accept", Net: string(NetworkQUIC), Addr: l.addr, Err: err}
	}
	return newQUICConn(conn), nil
}

func (l *quicListener) Addr() net.Addr { return l.addr }

// Close stops accepting and closes whatever is still queued: nothing will
// read those connections, and the peer is told why.
func (l *quicListener) Close() error {
	l.ln.Close()
	for {
		conn, err := l.ln.Accept(context.Background())
		if err != nil {
			return nil
		}
		conn.CloseWithError(quic.ApplicationErrorCode(ConnCodeShutdown), "listener closed")
	}
}

var _ Conn = (*QUICConn)(nil)

// QUICConn reports its endpoints as transport addresses rather than the UDP
// addresses underneath, so a connection's remote is comparable with the
// addresses a route names.
type QUICConn struct {
	conn  *quic.Conn
	laddr *Addr
	raddr *Addr
}

func newQUICConn(conn *quic.Conn) *QUICConn {
	return &QUICConn{
		conn:  conn,
		laddr: NewAddr(NetworkQUIC, conn.LocalAddr().String()),
		raddr: NewAddr(NetworkQUIC, conn.RemoteAddr().String()),
	}
}

func (qc *QUICConn) LocalAddr() net.Addr  { return qc.laddr }
func (qc *QUICConn) RemoteAddr() net.Addr { return qc.raddr }

// Context returns a context that is cancelled when the connection is closed,
// letting callers detect a dead connection without a stream operation.
func (qc *QUICConn) Context() context.Context { return qc.conn.Context() }

func (qc *QUICConn) OpenStream(ctx context.Context) (Stream, error) {
	s, err := qc.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return &QUICStream{s: s, laddr: qc.laddr, raddr: qc.raddr}, nil
}

func (qc *QUICConn) AcceptStream(ctx context.Context) (Stream, error) {
	s, err := qc.conn.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}
	return &QUICStream{s: s, laddr: qc.laddr, raddr: qc.raddr}, nil
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
