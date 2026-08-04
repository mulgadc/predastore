package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/mulgadc/predastore/internal/tlsconfig"
	"github.com/quic-go/quic-go"
)

const NetworkQUIC Network = "quic"

// alpnPrefix is the intra-cluster application protocol. The target node's key
// is appended to it, so ALPN negotiation both pins the cluster protocol and
// selects which node on the endpoint the connection belongs to. A dial for a
// node the peer does not run fails the handshake instead of misrouting.
const alpnPrefix = "mulga-repl-v1/"

// Connection-level error codes the transport reports to peers.
const (
	// ConnCodeUnknownNode closes a connection whose ALPN names a node this
	// process does not serve.
	ConnCodeUnknownNode ConnErrorCode = 1
	// ConnCodeBusy closes a connection the target node's accept queue has no
	// room for.
	ConnCodeBusy ConnErrorCode = 2
	// ConnCodeShutdown closes a connection because the listener is going away.
	ConnCodeShutdown ConnErrorCode = 3
)

// acceptQueueDepth bounds how many accepted connections may wait for one
// node. Beyond it the transport refuses connections rather than blocking the
// shared accept router and starving the other nodes on the socket.
const acceptQueueDepth = 32

// QUICAddr identifies one node reachable over QUIC: the endpoint whose socket
// carries it, and the node key that selects it there. Several nodes share an
// endpoint, so the key is what makes the address unique.
type QUICAddr struct {
	endpoint string
	node     string
}

func newQUICAddr(addr string) *QUICAddr {
	endpoint, node, _ := strings.Cut(addr, "/")
	return &QUICAddr{endpoint: endpoint, node: node}
}

// NewQUICAddr builds an address from its parts. The node key is opaque to the
// transport; callers derive it from the node id.
func NewQUICAddr(endpoint, node string) *QUICAddr {
	return &QUICAddr{endpoint: endpoint, node: node}
}

func (qa *QUICAddr) Network() string { return string(NetworkQUIC) }

// Endpoint is the "host:port" whose socket serves this address.
func (qa *QUICAddr) Endpoint() string { return qa.endpoint }

// Node is the key selecting this node among those sharing the endpoint.
func (qa *QUICAddr) Node() string { return qa.node }

func (qa *QUICAddr) String() string {
	if qa.node == "" {
		return qa.endpoint
	}
	return qa.endpoint + "/" + qa.node
}

var _ Transport = (*QUICTransport)(nil)

type QUICTransportConfig struct {
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
//
// One instance serves a whole process over a single UDP socket. quic-go
// allows only one listener per socket, so Listen registers each node against
// a shared accept router that hands every inbound connection to the node its
// ALPN names. Dialling opens one connection per target node over the same
// socket, which QUIC demultiplexes by connection id.
type QUICTransport struct {
	cfg QUICTransportConfig

	mu sync.Mutex
	// tr carries both directions once Listen has bound the socket; dialTr is
	// the ephemeral fallback for a process that never listens.
	tr     *quic.Transport
	dialTr *quic.Transport
	ln     *quic.Listener
	// requested is the endpoint callers named; endpoint is what the socket
	// actually bound, which differs when the request used port 0.
	requested string
	endpoint  string
	lns       map[string]*quicListener
	closed    bool
}

func NewQUICTransport(cfg QUICTransportConfig) *QUICTransport {
	return &QUICTransport{cfg: cfg, lns: make(map[string]*quicListener)}
}

func (qt *QUICTransport) Network() string { return string(NetworkQUIC) }

// quicAddr narrows a net.Addr to this transport's address type.
func quicAddr(op string, addr net.Addr) (*QUICAddr, error) {
	if addr == nil {
		return nil, &net.OpError{Op: op, Net: string(NetworkQUIC), Err: ErrMissingAddr}
	}
	qa, ok := addr.(*QUICAddr)
	if !ok {
		return nil, &net.OpError{Op: op, Net: string(NetworkQUIC), Addr: addr, Err: UnknownNetworkError(addr.Network())}
	}
	if qa.endpoint == "" || qa.node == "" {
		return nil, &net.OpError{Op: op, Net: string(NetworkQUIC), Addr: addr, Err: ErrMissingAddr}
	}
	return qa, nil
}

// Listen serves one node at addr. The first call binds the process socket;
// later calls register additional nodes on it and must name the same endpoint.
func (qt *QUICTransport) Listen(addr net.Addr) (Listener, error) {
	qa, err := quicAddr("listen", addr)
	if err != nil {
		return nil, err
	}

	qt.mu.Lock()
	defer qt.mu.Unlock()
	if qt.closed {
		return nil, ErrTransportClosed
	}
	if qt.ln == nil {
		if err := qt.bindLocked(qa.endpoint); err != nil {
			return nil, err
		}
	} else if qt.requested != qa.endpoint {
		return nil, fmt.Errorf("quic transport: already bound to %s, cannot also listen on %s", qt.requested, qa.endpoint)
	}
	if _, ok := qt.lns[qa.node]; ok {
		return nil, &net.OpError{Op: "listen", Net: string(NetworkQUIC), Addr: qa, Err: ErrAddrAlreadyInUse}
	}

	// Report the address peers can actually reach, which is not the requested
	// one when the socket bound an ephemeral port.
	l := &quicListener{
		tr:     qt,
		qa:     NewQUICAddr(qt.endpoint, qa.node),
		accept: make(chan *QUICConn, acceptQueueDepth),
		closed: make(chan struct{}),
	}
	qt.lns[qa.node] = l
	return l, nil
}

// bindLocked opens the process socket and starts the accept router. The
// listener's ALPN list is resolved per handshake so nodes registering after
// the bind are still reachable.
func (qt *QUICTransport) bindLocked(endpoint string) error {
	if qt.cfg.TLSCert == "" || qt.cfg.TLSKey == "" {
		return fmt.Errorf("quic transport: tls cert and key are required to listen")
	}
	cert, err := tls.LoadX509KeyPair(qt.cfg.TLSCert, qt.cfg.TLSKey)
	if err != nil {
		return fmt.Errorf("load x509 key pair from %s/%s: %w", qt.cfg.TLSCert, qt.cfg.TLSKey, err)
	}
	udpAddr, err := net.ResolveUDPAddr("udp", endpoint)
	if err != nil {
		return fmt.Errorf("resolve %s: %w", endpoint, err)
	}
	pc, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return fmt.Errorf("quic listen on %s: %w", endpoint, err)
	}

	base := &tls.Config{
		Certificates:     []tls.Certificate{cert},
		MinVersion:       tls.VersionTLS13,
		CurvePreferences: tlsconfig.Curves,
	}
	base.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
		c := base.Clone()
		c.GetConfigForClient = nil
		c.NextProtos = qt.alpnList()
		return c, nil
	}

	tr := &quic.Transport{Conn: pc}
	ln, err := tr.Listen(base, listenQUICConfig())
	if err != nil {
		pc.Close()
		return fmt.Errorf("quic listen on %s: %w", endpoint, err)
	}

	qt.tr = tr
	qt.ln = ln
	qt.requested = endpoint
	qt.endpoint = pc.LocalAddr().String()
	go qt.route(ln)
	return nil
}

// alpnList reports the protocol string of every node currently registered.
// It runs on the handshake path, so it must not be called with mu held.
func (qt *QUICTransport) alpnList() []string {
	qt.mu.Lock()
	defer qt.mu.Unlock()
	out := make([]string, 0, len(qt.lns))
	for node := range qt.lns {
		out = append(out, alpnPrefix+node)
	}
	slices.Sort(out)
	return out
}

// route hands each accepted connection to the node its ALPN names. It is the
// only reader of the shared listener, so it must never block.
func (qt *QUICTransport) route(ln *quic.Listener) {
	for {
		conn, err := ln.Accept(context.Background())
		if err != nil {
			qt.failListeners(err)
			return
		}
		node := strings.TrimPrefix(conn.ConnectionState().TLS.NegotiatedProtocol, alpnPrefix)
		qt.deliver(node, conn)
	}
}

func (qt *QUICTransport) deliver(node string, conn *quic.Conn) {
	qt.mu.Lock()
	l := qt.lns[node]
	qt.mu.Unlock()

	// ALPN negotiation should already have rejected an unknown node; close
	// rather than drop so the dialer learns why.
	if l == nil {
		conn.CloseWithError(quic.ApplicationErrorCode(ConnCodeUnknownNode), "unknown node")
		return
	}
	select {
	case <-l.closed:
		conn.CloseWithError(quic.ApplicationErrorCode(ConnCodeShutdown), "listener closed")
		return
	default:
	}
	select {
	case l.accept <- &QUICConn{conn: conn}:
	default:
		conn.CloseWithError(quic.ApplicationErrorCode(ConnCodeBusy), "accept queue full")
	}
}

// failListeners terminates every node listener when the shared router dies,
// so nobody is left blocked in Accept on a socket that no longer reads.
func (qt *QUICTransport) failListeners(err error) {
	qt.mu.Lock()
	lns := slices.Collect(maps.Values(qt.lns))
	qt.mu.Unlock()
	for _, l := range lns {
		l.fail(err)
	}
}

// release drops a node's registration. The socket and shared listener belong
// to the transport, not to the last node standing: one node draining must
// leave its siblings serving, and a process closes the transport itself when
// it exits. Connections arriving for a released node are refused as unknown.
func (qt *QUICTransport) release(node string, l *quicListener) {
	qt.mu.Lock()
	defer qt.mu.Unlock()
	if qt.lns[node] == l {
		delete(qt.lns, node)
	}
}

// dialer returns the transport to dial over: the bound socket when this
// process listens, otherwise a lazily created ephemeral one.
func (qt *QUICTransport) dialer() (*quic.Transport, error) {
	qt.mu.Lock()
	defer qt.mu.Unlock()
	if qt.closed {
		return nil, ErrTransportClosed
	}
	if qt.tr != nil {
		return qt.tr, nil
	}
	if qt.dialTr == nil {
		pc, err := net.ListenUDP("udp", &net.UDPAddr{})
		if err != nil {
			return nil, fmt.Errorf("quic dial socket: %w", err)
		}
		qt.dialTr = &quic.Transport{Conn: pc}
	}
	return qt.dialTr, nil
}

func (qt *QUICTransport) Dial(ctx context.Context, addr net.Addr) (Conn, error) {
	qa, err := quicAddr("dial", addr)
	if err != nil {
		return nil, err
	}
	tr, err := qt.dialer()
	if err != nil {
		return nil, err
	}
	udpAddr, err := net.ResolveUDPAddr("udp", qa.endpoint)
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", qa.endpoint, err)
	}
	host, _, err := net.SplitHostPort(qa.endpoint)
	if err != nil {
		return nil, fmt.Errorf("split %s: %w", qa.endpoint, err)
	}

	// Strict server verification: RootCAs nil means the OS trust store, and
	// ServerName comes from the endpoint rather than the node key, which
	// names a node rather than a certificate subject.
	tlsConf := &tls.Config{
		ServerName:       host,
		RootCAs:          qt.cfg.RootCAs,
		MinVersion:       tls.VersionTLS13,
		CurvePreferences: tlsconfig.Curves,
		NextProtos:       []string{alpnPrefix + qa.node},
	}
	conn, err := tr.Dial(ctx, udpAddr, tlsConf, dialQUICConfig())
	if err != nil {
		return nil, fmt.Errorf("quic dial %s: %w", qa, err)
	}
	return &QUICConn{conn: conn}, nil
}

// Close tears down every node listener, the shared listener and the sockets.
func (qt *QUICTransport) Close() error {
	qt.mu.Lock()
	if qt.closed {
		qt.mu.Unlock()
		return nil
	}
	qt.closed = true
	lns := slices.Collect(maps.Values(qt.lns))
	clear(qt.lns)
	ln, tr, dialTr := qt.ln, qt.tr, qt.dialTr
	qt.ln, qt.tr, qt.dialTr = nil, nil, nil
	qt.mu.Unlock()

	for _, l := range lns {
		l.fail(ErrTransportClosed)
	}
	if ln != nil {
		ln.Close()
	}
	if tr != nil {
		tr.Close()
	}
	if dialTr != nil {
		dialTr.Close()
	}
	return nil
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
		InitialStreamReceiveWindow:     initialStreamReceiveWindow,
		MaxStreamReceiveWindow:         maxStreamReceiveWindow,
		InitialConnectionReceiveWindow: initialConnectionReceiveWindow,
		MaxConnectionReceiveWindow:     maxConnectionReceiveWindow,
	}
}

var _ Listener = (*quicListener)(nil)

// quicListener is one node's view of the shared socket: the accept router
// feeds it only the connections whose ALPN named this node.
type quicListener struct {
	tr     *QUICTransport
	qa     *QUICAddr
	accept chan *QUICConn
	closed chan struct{}
	once   sync.Once

	mu sync.Mutex
	// err records why the shared router stopped, so Accept reports the cause
	// rather than a bare closed-listener error.
	err error
}

func (l *quicListener) Accept(ctx context.Context) (Conn, error) {
	select {
	case <-ctx.Done():
		return nil, &net.OpError{Op: "accept", Net: string(NetworkQUIC), Addr: l.qa, Err: ctx.Err()}
	case <-l.closed:
		return nil, &net.OpError{Op: "accept", Net: string(NetworkQUIC), Addr: l.qa, Err: l.cause()}
	case conn := <-l.accept:
		return conn, nil
	}
}

func (l *quicListener) Addr() net.Addr { return l.qa }

func (l *quicListener) cause() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return l.err
	}
	return ErrListenerClosed
}

// fail terminates the listener with the router's error. Connections already
// queued are dropped: nothing will read them.
func (l *quicListener) fail(err error) {
	l.mu.Lock()
	if l.err == nil {
		l.err = err
	}
	l.mu.Unlock()
	l.shutdown()
}

func (l *quicListener) Close() error {
	l.shutdown()
	return nil
}

// shutdown deregisters the node and closes the shared socket once the last
// node has gone, so one node draining leaves its siblings serving.
func (l *quicListener) shutdown() {
	l.once.Do(func() {
		close(l.closed)
		for {
			select {
			case conn := <-l.accept:
				conn.conn.CloseWithError(quic.ApplicationErrorCode(ConnCodeShutdown), "listener closed")
				continue
			default:
			}
			break
		}
		if l.tr != nil {
			l.tr.release(l.qa.node, l)
		}
	})
}

var _ Conn = (*QUICConn)(nil)

type QUICConn struct {
	conn *quic.Conn
}

func (qc *QUICConn) LocalAddr() net.Addr  { return qc.conn.LocalAddr() }
func (qc *QUICConn) RemoteAddr() net.Addr { return qc.conn.RemoteAddr() }

// Context returns a context that is cancelled when the connection is closed,
// letting callers detect a dead connection without a stream operation.
func (qc *QUICConn) Context() context.Context { return qc.conn.Context() }

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
