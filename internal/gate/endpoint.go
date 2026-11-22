package gate

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/mulgadc/bluebottle/pkg/iampolicy"
	"github.com/mulgadc/bluebottle/pkg/tlsconfig"
	"github.com/mulgadc/predastore/endpointproxy"
)

// endpointHandshakeTimeout bounds the node-to-node handshake and the PROXY
// header read, so a peer that connects and stalls holds no accept slot.
const endpointHandshakeTimeout = 10 * time.Second

type endpointCtxKey struct{}

// endpointIdentity is the endpoint a connection arrived through, with its
// policy parsed once per connection. A version 1 connection has no policy: its
// endpoint holds the default, which allows everything.
type endpointIdentity struct {
	endpointproxy.Header

	policy *iampolicy.PolicyDocument
}

// allows reports whether the endpoint policy, the second gate, permits action
// on resource; an action the gate could not name is not one it can permit.
func (e endpointIdentity) allows(action, resource string, keys iampolicy.ConditionKeys) bool {
	if e.policy == nil {
		return true
	}
	return action != "" && iampolicy.EvaluateEndpointPolicy(action, resource, *e.policy, keys) == iampolicy.Allow
}

// permits is allows, logging a refusal.
func (e endpointIdentity) permits(ctx context.Context, action, resource string, keys iampolicy.ConditionKeys) bool {
	if e.allows(action, resource, keys) {
		return true
	}
	slog.DebugContext(ctx, "S3 access denied by endpoint policy",
		"action", action, "resource", resource, "vpce_id", e.VpceID)
	return false
}

// endpointFrom returns the endpoint a request arrived through, if any.
func endpointFrom(ctx context.Context) (endpointIdentity, bool) {
	e, ok := ctx.Value(endpointCtxKey{}).(endpointIdentity)
	return e, ok
}

// parseEndpointPolicy reads a version 2 header's policy, refusing one the gate
// would not enforce as written, as the write path refuses it.
func parseEndpointPolicy(h endpointproxy.Header) (*iampolicy.PolicyDocument, error) {
	if h.Version < endpointproxy.ContractVersion2 {
		return nil, nil
	}
	doc, err := iampolicy.ParseEndpointPolicy([]byte(h.Policy))
	if err != nil {
		return nil, fmt.Errorf("endpoint policy does not parse: %w", err)
	}
	if err := iampolicy.CheckEndpointPolicy(doc); err != nil {
		return nil, fmt.Errorf("endpoint policy is not enforceable: %w", err)
	}
	return &doc, nil
}

// endpointConnContext attaches the endpoint identity of the connection under
// the guest's TLS to every request served on it.
func endpointConnContext(ctx context.Context, c net.Conn) context.Context {
	if tc, ok := c.(*tls.Conn); ok {
		if ec, ok := tc.NetConn().(*endpointConn); ok {
			return context.WithValue(ctx, endpointCtxKey{}, ec.endpoint)
		}
	}
	return ctx
}

// endpointConn is the node-to-node session after its header: the guest's TLS
// stream, addressed as the guest.
type endpointConn struct {
	net.Conn

	endpoint endpointIdentity
}

func (c *endpointConn) RemoteAddr() net.Addr { return net.TCPAddrFromAddrPort(c.endpoint.Source) }

// endpointTLSConfig is the outer session: TLS 1.3 with a client certificate
// that chains to the formation CA for client authentication, which only node
// certificates carry.
func endpointTLSConfig(cert tls.Certificate, caPath string) (*tls.Config, error) {
	pem, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("read endpoint CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("endpoint CA %s holds no certificate", caPath)
	}
	return &tls.Config{
		Certificates:     []tls.Certificate{cert},
		ClientAuth:       tls.RequireAndVerifyClientCert,
		ClientCAs:        pool,
		MinVersion:       tls.VersionTLS13,
		CurvePreferences: tlsconfig.Curves,
	}, nil
}

// endpointListener yields connections whose node handshake and header have
// both succeeded; each is accepted concurrently so one slow peer blocks none.
type endpointListener struct {
	inner   net.Listener
	tls     *tls.Config
	ready   chan net.Conn
	done    chan struct{}
	once    sync.Once
	wg      sync.WaitGroup
	mu      sync.Mutex
	closed  bool
	failed  error
	pending map[net.Conn]struct{} // connections still in their handshake
}

var _ net.Listener = (*endpointListener)(nil)

func newEndpointListener(inner net.Listener, cfg *tls.Config) *endpointListener {
	l := &endpointListener{
		inner: inner, tls: cfg, ready: make(chan net.Conn), done: make(chan struct{}),
		pending: make(map[net.Conn]struct{}),
	}
	l.wg.Go(l.acceptLoop)
	return l
}

func (l *endpointListener) acceptLoop() {
	for {
		raw, err := l.inner.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) && l.open() {
				// A transient failure such as EMFILE must not end the listener.
				slog.Warn("gate: endpoint accept failed, retrying", "error", err)
				time.Sleep(50 * time.Millisecond)
				continue
			}
			l.mu.Lock()
			l.failed = err
			l.mu.Unlock()
			l.once.Do(func() { close(l.done) })
			return
		}
		// Registration and Close's sweep of pending are one critical section, so
		// a connection accepted as Close runs is either swept or never admitted.
		l.mu.Lock()
		if l.closed {
			l.mu.Unlock()
			_ = raw.Close()
			return
		}
		l.pending[raw] = struct{}{}
		l.mu.Unlock()
		l.wg.Go(func() { l.admit(raw) })
	}
}

// admit completes one connection or closes it. A refusal is logged without the
// header's identifiers, which are the peer's claim until the header validates.
func (l *endpointListener) admit(raw net.Conn) {
	outer, endpoint, ok := l.handshake(raw)
	// Leaving pending hands the connection over: from here Close no longer
	// owns it, and once served, only the HTTP server's drain does.
	l.mu.Lock()
	delete(l.pending, raw)
	closed := l.closed
	l.mu.Unlock()
	if !ok || closed {
		_ = raw.Close()
		return
	}
	select {
	case l.ready <- &endpointConn{Conn: outer, endpoint: endpoint}:
	case <-l.done:
		_ = outer.Close()
	}
}

// handshake runs the node session and reads its header under one deadline. A
// policy the gate cannot enforce closes the connection, which the proxy reads
// as a failed check, rather than serve it under a narrower reading.
func (l *endpointListener) handshake(raw net.Conn) (*tls.Conn, endpointIdentity, bool) {
	_ = raw.SetDeadline(time.Now().Add(endpointHandshakeTimeout))
	outer := tls.Server(raw, l.tls)
	if err := outer.Handshake(); err != nil {
		slog.Warn("gate: endpoint handshake refused", "peer", raw.RemoteAddr().String(), "error", err)
		return nil, endpointIdentity{}, false
	}
	header, err := endpointproxy.Read(outer)
	if err != nil {
		slog.Warn("gate: endpoint header refused", "peer", raw.RemoteAddr().String(),
			"unsupported_version", errors.Is(err, endpointproxy.ErrUnsupportedVersion), "error", err)
		return nil, endpointIdentity{}, false
	}
	policy, err := parseEndpointPolicy(header)
	if err != nil {
		slog.Warn("gate: endpoint policy refused", "peer", raw.RemoteAddr().String(),
			"vpce_id", header.VpceID, "policy_generation", header.PolicyGeneration, "error", err)
		return nil, endpointIdentity{}, false
	}
	_ = raw.SetDeadline(time.Time{})
	return outer, endpointIdentity{Header: header, policy: policy}, true
}

func (l *endpointListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.ready:
		return c, nil
	case <-l.done:
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.failed != nil && !errors.Is(l.failed, net.ErrClosed) {
			return nil, l.failed
		}
		return nil, net.ErrClosed
	}
}

// open reports whether Close has not been called.
func (l *endpointListener) open() bool {
	select {
	case <-l.done:
		return false
	default:
		return true
	}
}

// Close stops accepting and abandons every handshake in progress, so it never
// waits out a stalled peer's deadline.
func (l *endpointListener) Close() error {
	l.once.Do(func() { close(l.done) })
	err := l.inner.Close()
	l.mu.Lock()
	l.closed = true
	for c := range l.pending {
		_ = c.Close()
	}
	l.mu.Unlock()
	l.wg.Wait()
	return err
}

func (l *endpointListener) Addr() net.Addr { return l.inner.Addr() }
