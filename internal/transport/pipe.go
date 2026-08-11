package transport

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
)

const NetworkPipe Network = "pipe"

// pipeRegistry maps bound addresses to live listeners so Dial can find a peer
// without any shared setup between the two transports.
type pipeRegistry struct {
	m  map[string]*PipeListener
	mu sync.RWMutex
}

var pipeReg = &pipeRegistry{m: make(map[string]*PipeListener)}

// ephemeralPipeSeq names the port of a port 0 transport. Such a source is
// never dialed or parsed; the counter only keeps two of them apart in logs.
var ephemeralPipeSeq atomic.Uint64

var _ Transport = (*PipeTransport)(nil)

// PipeTransport connects endpoints within a single process: connections are
// channel-linked endpoint pairs and streams are in-memory pipes. It carries
// traffic between colocated nodes, where no network or auth is required.
//
// It binds one address in a process-wide namespace, which it both dials from
// and listens on, so an accepted connection's remote is the dialer's source.
type PipeTransport struct {
	addr *Addr

	mu     sync.Mutex
	ln     *PipeListener
	closed bool
}

// NewPipeTransport binds the in-process name addr:port. Port 0 takes a
// synthesized unique name, which yields a transport that dials but is never
// dialed.
func NewPipeTransport(addr string, port int) *PipeTransport {
	p := strconv.Itoa(port)
	if port == 0 {
		p = "e" + strconv.FormatUint(ephemeralPipeSeq.Add(1), 10)
	}
	return &PipeTransport{addr: NewAddr(NetworkPipe, net.JoinHostPort(addr, p))}
}

func (pt *PipeTransport) Network() string { return string(NetworkPipe) }

// Addr is the name the transport bound, which differs from the requested one
// when the request named port 0.
func (pt *PipeTransport) Addr() net.Addr { return pt.addr }

// Listen accepts connections on the transport's own name. One name serves one
// node, so it may be called once.
func (pt *PipeTransport) Listen() (Listener, error) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	if pt.closed {
		return nil, ErrTransportClosed
	}
	if pt.ln != nil {
		return nil, &net.OpError{Op: "listen", Net: string(NetworkPipe), Addr: pt.addr, Err: ErrAddrAlreadyInUse}
	}

	pl := &PipeListener{
		addr:   pt.addr,
		accept: make(chan *PipeConn),
		closed: make(chan struct{}),
	}

	pipeReg.mu.Lock()
	defer pipeReg.mu.Unlock()
	if _, ok := pipeReg.m[pt.addr.String()]; ok {
		return nil, &net.OpError{Op: "listen", Net: string(NetworkPipe), Addr: pt.addr, Err: ErrAddrAlreadyInUse}
	}
	pipeReg.m[pt.addr.String()] = pl
	pt.ln = pl

	return pl, nil
}

func (pt *PipeTransport) Dial(ctx context.Context, remote net.Addr) (Conn, error) {
	if remote == nil {
		return nil, &net.OpError{Op: "dial", Net: string(NetworkPipe), Source: pt.addr, Err: ErrMissingAddr}
	}
	// Pipe and QUIC addresses for one node differ only by network, so an
	// unchecked network would let a QUIC address reach a pipe listener.
	if remote.Network() != string(NetworkPipe) {
		return nil, &net.OpError{Op: "dial", Net: string(NetworkPipe), Source: pt.addr, Addr: remote, Err: UnknownNetworkError(remote.Network())}
	}
	pt.mu.Lock()
	closed := pt.closed
	pt.mu.Unlock()
	if closed {
		return nil, &net.OpError{Op: "dial", Net: string(NetworkPipe), Source: pt.addr, Addr: remote, Err: ErrTransportClosed}
	}

	pipeReg.mu.RLock()
	pl, ok := pipeReg.m[remote.String()]
	pipeReg.mu.RUnlock()
	if !ok {
		return nil, &net.OpError{Op: "dial", Net: string(NetworkPipe), Source: pt.addr, Addr: remote, Err: ErrNoListener}
	}

	local, accepted := newPipeConnPair(pt.addr, pl.addr)

	// Rendezvous with the listener's accept loop; a listener that closes
	// while we wait is equivalent to it never having existed.
	select {
	case <-ctx.Done():
		return nil, &net.OpError{Op: "dial", Net: string(NetworkPipe), Source: pt.addr, Addr: remote, Err: ctx.Err()}
	case <-pl.closed:
		return nil, &net.OpError{Op: "dial", Net: string(NetworkPipe), Source: pt.addr, Addr: remote, Err: ErrNoListener}
	case pl.accept <- accepted:
		return local, nil
	}
}

// Close closes the transport and its listener. Established connections are
// unaffected.
func (pt *PipeTransport) Close() error {
	pt.mu.Lock()
	if pt.closed {
		pt.mu.Unlock()
		return nil
	}
	pt.closed = true
	ln := pt.ln
	pt.mu.Unlock()

	if ln != nil {
		ln.Close()
	}
	return nil
}

var _ Listener = (*PipeListener)(nil)

// PipeListener is the transport's single listener, holding its registry entry.
type PipeListener struct {
	addr   *Addr
	accept chan *PipeConn
	closed chan struct{}
	once   sync.Once
}

func (pl *PipeListener) Accept(ctx context.Context) (Conn, error) {
	select {
	case <-ctx.Done():
		return nil, &net.OpError{Op: "accept", Net: string(NetworkPipe), Addr: pl.addr, Err: ctx.Err()}
	case <-pl.closed:
		return nil, &net.OpError{Op: "accept", Net: string(NetworkPipe), Addr: pl.addr, Err: ErrListenerClosed}
	case pc := <-pl.accept:
		return pc, nil
	}
}

func (pl *PipeListener) Addr() net.Addr { return pl.addr }

func (pl *PipeListener) Close() error {
	pl.once.Do(func() {
		// Deregister first so new dials fail with ErrNoListener; dials
		// already blocked in the accept rendezvous observe the closed
		// channel instead.
		pipeReg.mu.Lock()
		if pipeReg.m[pl.addr.String()] == pl {
			delete(pipeReg.m, pl.addr.String())
		}
		pipeReg.mu.Unlock()

		close(pl.closed)
	})
	return nil
}

var _ Conn = (*PipeConn)(nil)

// pipeConnShared is the closure state common to both endpoints of a pipe
// connection: closing either endpoint closes the connection for both. The
// context mirrors closed as a cancellation signal, so callers can probe
// liveness without a stream operation.
type pipeConnShared struct {
	closed chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
	once   sync.Once
}

type PipeConn struct {
	laddr net.Addr
	raddr net.Addr
	// accept receives streams opened by the peer; peer is the reverse
	// direction, feeding the peer endpoint's accept channel.
	accept chan *PipeStream
	peer   chan *PipeStream
	shared *pipeConnShared
}

// newPipeConnPair builds the two endpoints of one connection, cross-wiring
// their stream channels.
func newPipeConnPair(dialer, listener net.Addr) (dc, lc *PipeConn) {
	ctx, cancel := context.WithCancel(context.Background())
	shared := &pipeConnShared{closed: make(chan struct{}), ctx: ctx, cancel: cancel}
	d2l := make(chan *PipeStream)
	l2d := make(chan *PipeStream)
	dc = &PipeConn{laddr: dialer, raddr: listener, accept: l2d, peer: d2l, shared: shared}
	lc = &PipeConn{laddr: listener, raddr: dialer, accept: d2l, peer: l2d, shared: shared}
	return dc, lc
}

func (pc *PipeConn) LocalAddr() net.Addr  { return pc.laddr }
func (pc *PipeConn) RemoteAddr() net.Addr { return pc.raddr }

// Context returns a context that is cancelled when either endpoint closes the
// connection, letting callers detect a dead connection without a stream
// operation.
func (pc *PipeConn) Context() context.Context { return pc.shared.ctx }

func (pc *PipeConn) OpenStream(ctx context.Context) (Stream, error) {
	local, remote := newPipeStreamPair(pc.laddr, pc.raddr)
	select {
	case <-ctx.Done():
		return nil, &net.OpError{Op: "open-stream", Net: string(NetworkPipe), Source: pc.laddr, Addr: pc.raddr, Err: ctx.Err()}
	case <-pc.shared.closed:
		return nil, &net.OpError{Op: "open-stream", Net: string(NetworkPipe), Source: pc.laddr, Addr: pc.raddr, Err: ErrConnClosed}
	case pc.peer <- remote:
		return local, nil
	}
}

func (pc *PipeConn) AcceptStream(ctx context.Context) (Stream, error) {
	select {
	case <-ctx.Done():
		return nil, &net.OpError{Op: "accept-stream", Net: string(NetworkPipe), Source: pc.laddr, Addr: pc.raddr, Err: ctx.Err()}
	case <-pc.shared.closed:
		return nil, &net.OpError{Op: "accept-stream", Net: string(NetworkPipe), Source: pc.laddr, Addr: pc.raddr, Err: ErrConnClosed}
	case ps := <-pc.accept:
		return ps, nil
	}
}

// Close closes the connection for both endpoints: pending and future
// OpenStream/AcceptStream calls fail with ErrConnClosed. Streams already
// established are unaffected.
func (pc *PipeConn) Close() error {
	pc.shared.once.Do(func() {
		close(pc.shared.closed)
		pc.shared.cancel()
	})
	return nil
}

var _ Stream = (*PipeStream)(nil)

// PipeStream is one end of a bidirectional in-memory stream: two
// unidirectional io.Pipes cross-wired between the endpoints. Writes
// rendezvous with peer reads, so backpressure is immediate.
type PipeStream struct {
	laddr net.Addr
	raddr net.Addr
	r     *io.PipeReader
	w     *io.PipeWriter
}

func newPipeStreamPair(opener, acceptor net.Addr) (os, as *PipeStream) {
	or, ow := io.Pipe()
	ar, aw := io.Pipe()
	os = &PipeStream{laddr: opener, raddr: acceptor, r: ar, w: ow}
	as = &PipeStream{laddr: acceptor, raddr: opener, r: or, w: aw}
	return os, as
}

func (ps *PipeStream) LocalAddr() net.Addr  { return ps.laddr }
func (ps *PipeStream) RemoteAddr() net.Addr { return ps.raddr }

func (ps *PipeStream) Read(p []byte) (int, error)  { return ps.r.Read(p) }
func (ps *PipeStream) Write(p []byte) (int, error) { return ps.w.Write(p) }

// Close closes the write side: the peer observes io.EOF after draining
// buffered data. The read side stays open, matching QUIC stream semantics.
func (ps *PipeStream) Close() error { return ps.w.Close() }

// CancelRead aborts the read side: the peer's writes and our own reads fail
// from now on. The code is surfaced to the peer as a StreamError.
func (ps *PipeStream) CancelRead(code StreamErrorCode) {
	ps.r.CloseWithError(&StreamError{Code: code})
}

// CancelWrite aborts the write side: the peer's reads fail with a
// StreamError carrying the code instead of io.EOF.
func (ps *PipeStream) CancelWrite(code StreamErrorCode) {
	ps.w.CloseWithError(&StreamError{Code: code})
}

// ReadFrom and WriteTo satisfy transport.Stream; io.Pipe has no zero-copy
// path, so both delegate to io.Copy on the underlying pipe half.
func (ps *PipeStream) ReadFrom(r io.Reader) (int64, error) { return io.Copy(ps.w, r) }
func (ps *PipeStream) WriteTo(w io.Writer) (int64, error)  { return io.Copy(w, ps.r) }
