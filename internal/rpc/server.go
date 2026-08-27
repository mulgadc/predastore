package rpc

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"syscall"
	"time"
)

var ErrServerClosed = errors.New("server closed")

type HandlerFunc func(ctx context.Context, conn io.ReadWriter)

type Server struct {
	acceptCtx     context.Context
	cancelAccepts context.CancelFunc
	listeners     sync.WaitGroup

	handlersCtx    context.Context
	cancelHandlers context.CancelFunc
	conns          sync.WaitGroup

	drainOnce sync.Once
	drainDone chan struct{}

	mu     sync.RWMutex
	fns    map[Opcode]HandlerFunc
	closed bool
}

func NewServer() *Server {
	acceptCtx, cancelAccepts := context.WithCancel(context.Background())
	handlersCtx, cancelHandlers := context.WithCancel(context.Background())

	return &Server{
		fns:            make(map[Opcode]HandlerFunc),
		acceptCtx:      acceptCtx,
		cancelAccepts:  cancelAccepts,
		handlersCtx:    handlersCtx,
		cancelHandlers: cancelHandlers,
		drainDone:      make(chan struct{}),
	}
}

func (s *Server) RegisterOpcode(code Opcode, fn HandlerFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fns[code] = fn
}

func (s *Server) Serve(l net.Listener) error {
	closeListener := func() {
		if err := l.Close(); err != nil {
			slog.Error("rpc: server: failed to close listener", "error", err)
		}
	}

	stop := context.AfterFunc(s.acceptCtx, func() { closeListener() })
	defer func() {
		if stop() {
			closeListener()
		}
	}()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrServerClosed
	}
	s.listeners.Add(1)
	defer s.listeners.Done()
	s.mu.Unlock()

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

	s.mu.RLock()
	fn, ok := s.fns[code]
	s.mu.RUnlock()
	if !ok {
		slog.Error("rpc: server: no matching handler found", "opcode", code)
		return
	}

	// TODO: Idle timeout.
	// TODO: Gracefully catch and recover from panics in fn.
	fn(ctx, conn)
}

func (s *Server) Close() error {
	s.setClosed()

	s.cancelHandlers()
	<-s.drain()

	s.mu.RLock()
	defer s.mu.RUnlock()
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.setClosed()

	select {
	case <-s.drain():
		s.cancelHandlers()

		s.mu.RLock()
		defer s.mu.RUnlock()
		return nil

	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) setClosed() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
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
