package rpc

import (
	"context"
	"errors"
	"net"
	"sync"
)

var ErrNoRegistry = errors.New("no registry provided")
var ErrAddrInUse = errors.New("address already in use")
var ErrConnRefused = errors.New("connection refused")

type PipeRegistry struct {
	mu  sync.RWMutex
	trs map[string]*PipeTransport
}

func NewPipeRegistry() *PipeRegistry {
	return &PipeRegistry{trs: make(map[string]*PipeTransport)}
}

func (pr *PipeRegistry) put(new *PipeTransport) bool {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	key := new.addr.String()

	old, ok := pr.trs[key]
	if ok && old != nil {
		return false
	}

	pr.trs[key] = new
	return true
}

func (pr *PipeRegistry) get(addr net.Addr) (*PipeTransport, bool) {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	pt, ok := pr.trs[addr.String()]
	return pt, ok
}

func (pr *PipeRegistry) delete(addr net.Addr) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	delete(pr.trs, addr.String())
}

type PipeTransport struct {
	addr     net.Addr
	registry *PipeRegistry

	once   sync.Once
	accept chan net.Conn
	done   chan struct{}
}

func NewPipeTransport(addr net.Addr, reg *PipeRegistry) (*PipeTransport, error) {
	if reg == nil {
		return nil, ErrNoRegistry
	}

	pt := &PipeTransport{
		addr:     addr,
		registry: reg,
		accept:   make(chan net.Conn),
		done:     make(chan struct{}),
	}

	if !reg.put(pt) {
		return nil, &net.OpError{Op: "listen", Net: "pipe", Addr: addr, Err: ErrAddrInUse}
	}

	return pt, nil
}

func (pt *PipeTransport) Accept() (net.Conn, error) {
	select {
	case conn := <-pt.accept:
		return conn, nil

	case <-pt.done:
		return nil, &net.OpError{Op: "accept", Net: "pipe", Addr: pt.addr, Err: net.ErrClosed}
	}
}

func (pt *PipeTransport) Addr() net.Addr { return pt.addr }

func (pt *PipeTransport) Close() error {
	pt.once.Do(func() {
		pt.registry.delete(pt.addr)
		close(pt.done)
	})

	return nil
}

func (pt *PipeTransport) Dial(ctx context.Context, addr net.Addr) (net.Conn, error) {
	listener, ok := pt.registry.get(addr)
	if !ok {
		return nil, &net.OpError{Op: "dial", Net: "pipe", Addr: addr, Source: pt.addr, Err: ErrConnRefused}
	}

	lConn, rConn := net.Pipe()

	select {
	case listener.accept <- pipeConn{Conn: rConn, lAddr: addr, rAddr: pt.addr}:
		return pipeConn{Conn: lConn, lAddr: pt.addr, rAddr: addr}, nil

	case <-listener.done:
		return nil, &net.OpError{Op: "dial", Net: "pipe", Addr: addr, Source: pt.addr, Err: ErrConnRefused}

	case <-ctx.Done():
		return nil, &net.OpError{Op: "dial", Net: "pipe", Addr: addr, Source: pt.addr, Err: ctx.Err()}
	}
}

type pipeConn struct {
	net.Conn
	lAddr, rAddr net.Addr
}

func (pc pipeConn) LocalAddr() net.Addr  { return pc.lAddr }
func (pc pipeConn) RemoteAddr() net.Addr { return pc.rAddr }
