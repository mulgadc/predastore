package rpc

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var ErrServerClosed = errors.New("server closed")

type HandleFunc func(ctx context.Context, conn net.Conn)

type Mux struct {
	mu  sync.RWMutex
	fns map[Opcode]HandleFunc
}

func NewMux() *Mux {
	return &Mux{fns: make(map[Opcode]HandleFunc)}
}

func (m *Mux) Handle(code Opcode, fn HandleFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fns[code] = fn
}

func (m *Mux) lookup(code Opcode) (HandleFunc, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	fn, ok := m.fns[code]
	return fn, ok
}

type Server struct {
	mux *Mux

	acceptCtx     context.Context
	cancelAccepts context.CancelFunc
	listeners     sync.WaitGroup

	handlersCtx    context.Context
	cancelHandlers context.CancelFunc
	conns          sync.WaitGroup

	drainOnce sync.Once
	drainDone chan struct{}

	closed atomic.Bool
}

type ServerConfig struct {
	Mux *Mux
}

func NewServer(cfg *ServerConfig) *Server {
	acceptCtx, cancelAccepts := context.WithCancel(context.Background())
	handlersCtx, cancelHandlers := context.WithCancel(context.Background())

	s := &Server{
		mux:            NewMux(),
		acceptCtx:      acceptCtx,
		cancelAccepts:  cancelAccepts,
		handlersCtx:    handlersCtx,
		cancelHandlers: cancelHandlers,
		drainDone:      make(chan struct{}),
	}

	if cfg != nil {
		if cfg.Mux != nil {
			s.mux = cfg.Mux
		}
	}

	return s
}

func (s *Server) Serve(l net.Listener) error {
	if s.closed.Load() {
		return ErrServerClosed
	}

	s.listeners.Add(1)
	defer s.listeners.Done()

	var delay time.Duration
	for {
		conn, err := l.Accept()
		if err != nil {
			if s.acceptCtx.Err() != nil {
				return ErrServerClosed
			}

			if s.isRecoverable(err) {
				slog.Error("rpc: server: failed to accept connection, retrying...", "error", err)
				delay = min(max(2*delay, 5*time.Millisecond), time.Second)
				select {
				case <-time.After(delay):
					continue
				case <-s.acceptCtx.Done():
					return ErrServerClosed
				}
			}

			return err
		}

		delay = 0
		s.conns.Go(func() { s.handle(s.handlersCtx, conn) })
	}
}

func (s *Server) isRecoverable(err error) bool {
	if errno, ok := errors.AsType[syscall.Errno](err); ok {
		switch errno {
		case syscall.EMFILE, syscall.ENFILE, syscall.ENOBUFS, syscall.ENOMEM:
			return true
		}
	}

	return false
}

func (s *Server) handle(ctx context.Context, conn net.Conn) {
	closeConn := func() {
		if err := conn.Close(); err != nil {
			slog.Error("rpc: server: failed to close connection", "error", err)
		}
	}

	stop := context.AfterFunc(ctx, func() { closeConn() })
	defer func() {
		if stop() {
			closeConn()
		}
	}()

	code, err := readOpcode(conn)
	if err != nil {
		slog.Error("rpc: server: failed to read opcode", "error", err)
		return
	}

	fn, ok := s.mux.lookup(code)
	if !ok {
		slog.Error("rpc: server: no matching handler found", "opcode", code)
		return
	}

	// TODO: Idle timeout.
	// TODO: Gracefully catch and recover from panics in fn.
	fn(ctx, conn)
}

func (s *Server) Close() error {
	s.closed.Store(true)

	s.cancelHandlers()
	<-s.drain()

	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.closed.Store(true)

	select {
	case <-s.drain():
		s.cancelHandlers()

		return nil

	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) drain() <-chan struct{} {
	s.drainOnce.Do(func() {
		s.cancelAccepts()

		go func() {
			s.listeners.Wait()
			s.conns.Wait()

			close(s.drainDone)
		}()
	})

	return s.drainDone
}
