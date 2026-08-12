package rpc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/mulgadc/predastore/internal/transport"
	"golang.org/x/sync/errgroup"
)

type rawHandler func(ctx context.Context, header []byte, stream transport.Stream) error
type Handler[T any] func(ctx context.Context, header T, stream transport.Stream) error

type Mux struct {
	handlers map[Opcode]rawHandler
}

func NewMux() *Mux {
	return &Mux{handlers: make(map[Opcode]rawHandler)}
}

func RegisterHandler[T any, PT interface {
	*T
	Header
}](m *Mux, op Opcode, h Handler[T]) {
	m.handlers[op] = func(ctx context.Context, raw []byte, stream transport.Stream) error {
		var decoded T
		if err := PT(&decoded).Unmarshal(raw); err != nil {
			return fmt.Errorf("decode header: %w", err)
		}
		return h(ctx, decoded, stream)
	}
}

type ServerOption func(*Server)

// WithDrainTimeout bounds how long Run waits for in-flight handlers once the
// accept loops have stopped.
func WithDrainTimeout(d time.Duration) ServerOption {
	return func(s *Server) { s.drainTimeout = d }
}

// Server answers rpc streams for one node on the listeners it is given, which
// arrive already bound.
//
// With a nil pool it owns and closes every connection it accepts, donating
// none.
type Server struct {
	mux          *Mux
	lns          []transport.Listener
	pool         *ConnPool
	drainTimeout time.Duration
}

func NewServer(mux *Mux, lns []transport.Listener, pool *ConnPool, opts ...ServerOption) (*Server, error) {
	if mux == nil {
		return nil, fmt.Errorf("server has no mux")
	}

	const defaultDrainTimeout = 30 * time.Second
	s := &Server{mux: mux, lns: lns, pool: pool, drainTimeout: defaultDrainTimeout}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// Run accepts on every listener until ctx is cancelled, then drains in-flight
// handlers before returning.
func (s *Server) Run(ctx context.Context) error {
	// Handlers hang off Background rather than ctx, so cancelling ctx stops the
	// accept loops without cutting a request short mid-flight. They are only
	// cancelled once the drain deadline expires.
	g, acceptCtx := errgroup.WithContext(ctx)
	handlerCtx, cancelHandlers := context.WithCancel(context.Background())
	defer cancelHandlers()

	var conns sync.WaitGroup

	for _, ln := range s.lns {
		g.Go(func() error {
			err := s.acceptConns(acceptCtx, handlerCtx, ln, &conns)
			switch {
			case err == nil,
				errors.Is(err, context.Canceled),
				errors.Is(err, transport.ErrListenerClosed):
				return nil
			default:
				slog.ErrorContext(acceptCtx, "listener error",
					"err", err,
					"addr", ln.Addr())
				return err
			}
		})
	}

	serveErr := g.Wait()
	for _, ln := range s.lns {
		ln.Close()
	}

	// Each conns goroutine waits for its own handlers, so this drains every
	// in-flight request.
	done := make(chan struct{})
	go func() {
		conns.Wait()
		close(done)
	}()

	t := time.NewTimer(s.drainTimeout)
	defer t.Stop()
	deadline := t.C

	select {
	case <-done:
		return serveErr
	case <-deadline:
		// Cancelling aborts the connections handlers are parked on, which is
		// what lets the second wait finish.
		cancelHandlers()
		<-done
		if serveErr != nil {
			return serveErr
		}
		return fmt.Errorf("exceeded drain timeout")
	}
}

func (s *Server) acceptConns(
	acceptCtx, handlerCtx context.Context,
	ln transport.Listener,
	conns *sync.WaitGroup,
) error {
	const maxBackoff = time.Second
	backoff := 5 * time.Millisecond

	for {
		conn, err := ln.Accept(acceptCtx)
		if err != nil {
			var te interface{ Temporary() bool }
			if errors.As(err, &te) && te.Temporary() {
				// Stopped inline, not deferred: this loop runs for the life of
				// the listener, so deferred Stops would accumulate per retry.
				t := time.NewTimer(backoff)
				select {
				case <-t.C:
					backoff = min(backoff*2, maxBackoff)
					continue
				case <-acceptCtx.Done():
					t.Stop()
					return acceptCtx.Err()
				}
			}
			return err
		}
		backoff = 5 * time.Millisecond

		conns.Go(func() {
			// Handlers outlive the accept loop, so the connection is released
			// here, once they have drained. Waiting inside this goroutine is
			// what makes Run's conns.Wait a full drain.
			var streams sync.WaitGroup

			// A donated connection belongs to the pool, so it leaves through
			// Evict rather than being closed behind the pool's back.
			if s.pool != nil && s.pool.Donate(conn) {
				defer s.pool.Evict(conn)
			} else {
				defer conn.Close()
			}

			// A handler parked in a stream read has no deadline and never
			// observes ctx, so closing the connection under it is the only way
			// to unblock one once the drain deadline has expired.
			drained := make(chan struct{})
			defer close(drained)
			go func() {
				select {
				case <-handlerCtx.Done():
					conn.Close()
				case <-drained:
				}
			}()

			err := s.acceptStreams(acceptCtx, handlerCtx, conn, &streams)
			streams.Wait()

			switch {
			case err == nil,
				errors.Is(err, context.Canceled),
				errors.Is(err, transport.ErrListenerClosed),
				errors.Is(err, transport.ErrConnClosed):
			default:
				slog.ErrorContext(acceptCtx, "connection error",
					"err", err,
					"source", conn.LocalAddr(),
					"addr", conn.RemoteAddr())
			}
		})
	}
}

func (s *Server) acceptStreams(
	acceptCtx, handlerCtx context.Context,
	conn transport.Conn,
	streams *sync.WaitGroup,
) error {
	for {
		stream, err := conn.AcceptStream(acceptCtx)
		if err != nil {
			return fmt.Errorf("accept stream: %w", err)
		}

		streams.Go(func() {
			// Closing the write side is what marks a response complete, so a
			// handler that answered at all lands in the second branch. A reset
			// says the stream broke before any answer was written.
			if code, err := s.handleStream(handlerCtx, stream); err != nil {
				slog.ErrorContext(acceptCtx, "stream error",
					"err", err,
					"code", code,
					"source", conn.LocalAddr(),
					"addr", conn.RemoteAddr())
				stream.CancelRead(code)
				stream.CancelWrite(code)
			} else {
				stream.Close()
			}
		})
	}
}

// handleStream dispatches one stream, returning the code to reset it with if
// it broke. A nil error means the handler ran to completion.
func (s *Server) handleStream(ctx context.Context, stream transport.Stream) (transport.StreamErrorCode, error) {
	// The opcode precedes the frame, since it is what picks the header type the
	// frame is then decoded as.
	var opBuf [4]byte
	if _, err := io.ReadFull(stream, opBuf[:]); err != nil {
		return ErrCodeBadHeader, fmt.Errorf("read opcode: %w", err)
	}
	op := Opcode(binary.BigEndian.Uint32(opBuf[:]))

	header, err := readFrame(stream)
	if err != nil {
		return ErrCodeBadHeader, fmt.Errorf("read header: %w", err)
	}

	h, ok := s.mux.handlers[op]
	if !ok {
		return ErrCodeUnknownOpcode, fmt.Errorf("no handler for opcode %#x", op)
	}
	if err := h(ctx, header, stream); err != nil {
		return ErrCodeHandlerFailed, err
	}
	return 0, nil
}
