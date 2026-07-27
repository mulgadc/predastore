package transport

import (
	"context"
	"io"
	"net"
	"sync"
)

const NetworkPipe Network = "pipe"

// PipeAddr names an in-process endpoint. Pipe addresses share a process-wide
// namespace: dialing a name reaches whichever listener currently holds it.
type PipeAddr struct {
	name string
}

func newPipeAddr(name string) *PipeAddr {
	return &PipeAddr{name: name}
}

func (pa *PipeAddr) Network() string {
	return string(NetworkPipe)
}

func (pa *PipeAddr) String() string {
	return pa.name
}

// pipeRegistry maps listener names to live listeners so Dial can find a peer
// without any shared setup between the two transports.
type pipeRegistry struct {
	m  map[string]*PipeListener
	mu sync.RWMutex
}

var pipeReg = &pipeRegistry{m: make(map[string]*PipeListener)}

var _ Transport = (*PipeTransport)(nil)

// PipeTransport connects endpoints within a single process: connections are
// channel-linked endpoint pairs and streams are in-memory pipes. It carries
// traffic between colocated nodes, where no network or auth is required.
type PipeTransport struct {
	pa *PipeAddr

	mu       sync.Mutex
	ln       *PipeListener
	closeErr error
}

func NewPipeTransport(name string) *PipeTransport {
	return &PipeTransport{pa: newPipeAddr(name)}
}

func (pt *PipeTransport) Addr() net.Addr { return pt.pa }

func (pt *PipeTransport) Listen() (Listener, error) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	if pt.closeErr != nil {
		return nil, pt.closeErr
	}

	pl := &PipeListener{
		pa:     pt.pa,
		accept: make(chan *PipeConn),
		closed: make(chan struct{}),
	}

	pipeReg.mu.Lock()
	defer pipeReg.mu.Unlock()
	if _, ok := pipeReg.m[pt.pa.String()]; ok {
		return nil, &net.OpError{Op: "listen", Net: string(NetworkPipe), Addr: pt.pa, Err: ErrAddrAlreadyInUse}
	}
	pipeReg.m[pt.pa.String()] = pl
	pt.ln = pl

	return pl, nil
}

func (pt *PipeTransport) Dial(ctx context.Context, addr net.Addr) (Conn, error) {
	if addr == nil {
		return nil, &net.OpError{Op: "dial", Net: string(NetworkPipe), Source: pt.pa, Err: ErrMissingAddr}
	}
	pt.mu.Lock()
	closeErr := pt.closeErr
	pt.mu.Unlock()
	if closeErr != nil {
		return nil, &net.OpError{Op: "dial", Net: string(NetworkPipe), Source: pt.pa, Addr: addr, Err: closeErr}
	}

	pipeReg.mu.RLock()
	pl, ok := pipeReg.m[addr.String()]
	pipeReg.mu.RUnlock()
	if !ok {
		return nil, &net.OpError{Op: "dial", Net: string(NetworkPipe), Source: pt.pa, Addr: addr, Err: ErrNoListener}
	}

	local, remote := newPipeConnPair(pt.pa, pl.pa)

	// Rendezvous with the listener's accept loop; a listener that closes
	// while we wait is equivalent to it never having existed.
	select {
	case <-ctx.Done():
		return nil, &net.OpError{Op: "dial", Net: string(NetworkPipe), Source: pt.pa, Addr: addr, Err: ctx.Err()}
	case <-pl.closed:
		return nil, &net.OpError{Op: "dial", Net: string(NetworkPipe), Source: pt.pa, Addr: addr, Err: ErrNoListener}
	case pl.accept <- remote:
		return local, nil
	}
}

// Close closes the transport and its active listener. Established connections
// are unaffected.
func (pt *PipeTransport) Close() error {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	if pt.closeErr != nil {
		return nil
	}
	pt.closeErr = ErrTransportClosed
	if pt.ln != nil {
		pt.ln.Close()
	}
	return nil
}

var _ Listener = (*PipeListener)(nil)

type PipeListener struct {
	pa     *PipeAddr
	accept chan *PipeConn
	closed chan struct{}
	once   sync.Once
}

func (pl *PipeListener) Accept(ctx context.Context) (Conn, error) {
	select {
	case <-ctx.Done():
		return nil, &net.OpError{Op: "accept", Net: string(NetworkPipe), Addr: pl.pa, Err: ctx.Err()}
	case <-pl.closed:
		return nil, &net.OpError{Op: "accept", Net: string(NetworkPipe), Addr: pl.pa, Err: ErrListenerClosed}
	case pc := <-pl.accept:
		return pc, nil
	}
}

func (pl *PipeListener) Addr() net.Addr { return pl.pa }

func (pl *PipeListener) Close() error {
	pl.once.Do(func() {
		// Deregister first so new dials fail with ErrNoListener; dials
		// already blocked in the accept rendezvous observe the closed
		// channel instead.
		pipeReg.mu.Lock()
		if pipeReg.m[pl.pa.String()] == pl {
			delete(pipeReg.m, pl.pa.String())
		}
		pipeReg.mu.Unlock()
		close(pl.closed)
	})
	return nil
}

var _ Conn = (*PipeConn)(nil)

// pipeConnShared is the closure state common to both endpoints of a pipe
// connection: closing either endpoint closes the connection for both.
type pipeConnShared struct {
	closed chan struct{}
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
	shared := &pipeConnShared{closed: make(chan struct{})}
	d2l := make(chan *PipeStream)
	l2d := make(chan *PipeStream)
	dc = &PipeConn{laddr: dialer, raddr: listener, accept: l2d, peer: d2l, shared: shared}
	lc = &PipeConn{laddr: listener, raddr: dialer, accept: d2l, peer: l2d, shared: shared}
	return dc, lc
}

func (pc *PipeConn) LocalAddr() net.Addr  { return pc.laddr }
func (pc *PipeConn) RemoteAddr() net.Addr { return pc.raddr }

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
	pc.shared.once.Do(func() { close(pc.shared.closed) })
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
