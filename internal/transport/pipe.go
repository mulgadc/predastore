package transport

import (
	"context"
	"net"
	"sync"
)

const NetworkPipe Network = "pipe"

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

type pipeRegistryEntry struct {
	conns   chan<- *PipeConn
	streams chan<- *PipeStream
}

type pipeRegistry struct {
	m  map[string]*pipeRegistryEntry
	mu sync.RWMutex
}

var pipeReg = &pipeRegistry{m: make(map[string]*pipeRegistryEntry)}

var _ Transport = (*PipeTransport)(nil)

type PipeTransport struct {
	mu       sync.Mutex
	pa       *PipeAddr
	closeErr error
}

func (pt *PipeTransport) Addr() net.Addr { return pt.pa }

func (pt *PipeTransport) Listen() (Listener, error) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	if pt.closeErr != nil {
		return nil, pt.closeErr
	}
	pipeReg.mu.Lock()
	defer pipeReg.mu.Unlock()
	if _, ok := pipeReg.m[pt.pa.String()]; ok {
		return nil, &net.OpError{Op: "listen", Net: string(NetworkPipe), Source: nil, Addr: pt.pa, Err: ErrAddrAlreadyInUse}
	}
	pl := &PipeListener{
		accept: make(chan *PipeConn),
		pa:     pt.pa,
	}
	pipeReg.m[pt.pa.String()] = &pipeRegistryEntry{conns: pl.accept}
	return pl, nil
}

func (pt *PipeTransport) Dial(ctx context.Context, addr net.Addr) (Conn, error) {
	if addr == nil {
		return nil, &net.OpError{Op: "dial", Net: string(NetworkPipe), Source: pt.pa, Addr: nil, Err: ErrMissingAddr}
	}
	pipeReg.mu.RLock()
	pre, ok := pipeReg.m[addr.String()]
	pipeReg.mu.RUnlock()
	if !ok {
		return nil, &net.OpError{Op: "dial", Net: string(NetworkPipe), Source: pt.pa, Addr: addr, Err: ErrNoListener}
	}

	pcl := &PipeConn{
		accept: pre.conns,
		pal:    pt.pa, par: addr,
	}
	pcr := &PipeConn{
		tx: r2l, rx: l2r,
		laddr: raddr, raddr: laddr,
	}

	select {
	case <-ctx.Done():
		return nil, &net.OpError{Op: "dial", Net: string(NetworkPipe), Source: pt.pa, Addr: addr, Err: ctx.Err()}
	case pre.conns <- pcr:
		return pcl, nil
	}
}

func (pt *PipeTransport) Close() error {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	if pt.closeErr != nil {
		return pt.closeErr
	}
	pt.closeErr = ErrTransportClosed

	pipeReg.mu.Lock()
	defer pipeReg.mu.Unlock()
	if _, ok := pipeReg.m[pt.pa.String()]; ok {
		delete(pipeReg.m, pt.pa.String())
	}

	return pt.closeErr
}

type PipeListener struct {
	accept chan *PipeConn
	once   sync.Once
	pa     *PipeAddr
}

func (pl *PipeListener) Accept(ctx context.Context) (Conn, error) {
	select {
	case <-ctx.Done():
		return nil, &net.OpError{Op: "accept", Net: string(NetworkPipe), Source: nil, Addr: pl.pa, Err: ctx.Err()}
	case pc, ok := <-pl.accept:
		if !ok {
			return nil, &net.OpError{Op: "accept", Net: string(NetworkPipe), Source: nil, Addr: pl.pa, Err: ErrListenerClosed}
		}
		return pc, nil
	}
}

func (pl *PipeListener) Addr() net.Addr { return pl.pa }

func (pl *PipeListener) Close() error {
	pl.once.Do(func() {
		close(pl.accept)
		pipeReg.mu.Lock()
		pipeReg.m[pl.pa.String()].conns = nil
		pipeReg.mu.Unlock()
	})
	return nil
}

type PipeConn struct {
	accept chan *PipeStream
	once   sync.Once
	pal    *PipeAddr
	par    *PipeAddr
}

func (pc *PipeConn) LocalAddr() net.Addr  { return pc.pal }
func (pc *PipeConn) RemoteAddr() net.Addr { return pc.par }

func (pc *PipeConn) AcceptStream(ctx context.Context) (Stream, error) {
	select {
	case <-ctx.Done():
		return nil, &net.OpError{Op: "accept-stream", Net: string(NetworkPipe), Source: pc.pal, Addr: pc.par, Err: ctx.Err()}
	case ps, ok := <-pc.accept:
		if !ok {
			return nil, &net.OpError{Op: "accept-stream", Net: string(NetworkPipe), Source: pc.pal, Addr: pc.par, Err: ErrConnClosed}
		}
		return ps, nil
	}
}

func (pc *PipeConn) Close() error {
	pc.once.Do(func() {
		close(pc.accept)
		pipeReg.mu.Lock()
		pipeReg.m[pc.pal.String()].streams = nil
		pipeReg.mu.Unlock()
	})
	return nil
}

type PipeStream struct {
	tx     chan<- Pipe
	rx     <-chan Buffer
	mu     sync.RWMutex
	closed bool

	laddr *PipeAddr
	raddr *PipeAddr
}

func (c *PipeStream) Send(ctx context.Context, b transport.Buffer) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return &net.OpError{Op: "send", Net: Network, Source: c.laddr, Addr: c.raddr, Err: errSendClosed}
	}
	// Hold RLock across the send: Close can't proceed (needs WLock)
	// until every in-flight Send releases, so c.tx is guaranteed open here.
	select {
	case <-ctx.Done():
		return &net.OpError{Op: "send", Net: Network, Source: c.laddr, Addr: c.raddr, Err: ctx.Err()}
	case c.tx <- b:
		return nil
	}
}

func (c *PipeStream) Recv(ctx context.Context) (transport.Buffer, error) {
	select {
	case <-ctx.Done():
		return nil, &net.OpError{Op: "recv", Net: Network, Source: c.laddr, Addr: c.raddr, Err: ctx.Err()}
	case b, ok := <-c.rx:
		if !ok {
			return nil, &net.OpError{Op: "recv", Net: Network, Source: c.laddr, Addr: c.raddr, Err: errRecvClosed}
		}
		return b, nil
	}
}

func (c *PipeStream) LocalAddr() transport.Addr {
	return c.laddr
}

func (c *PipeStream) RemoteAddr() transport.Addr {
	return c.raddr
}

func (c *PipeStream) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	close(c.tx)
	return nil
}
