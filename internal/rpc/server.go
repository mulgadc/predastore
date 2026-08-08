package rpc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
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

// Server answers rpc streams for one node on the listeners it is given. The
// listeners are already bound: whoever built the node owns its transports, so
// a server never learns which networks it is reached over.
//
// A nil pool is meaningful: such a server owns and closes what it accepts and
// donates nothing, which is what a node that never dials wants.
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

func (s *Server) Run(ctx context.Context) error {
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

	// Each conns goroutine waits for its own handlers, so this covers every
	// in-flight request: once it returns, nothing is still touching state the
	// caller is about to tear down.
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
				// The timer is stopped rather than deferred: this loop runs
				// for the life of the listener, so a deferred Stop per retry
				// would accumulate for as long as the errors keep coming.
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
			// here and only once they have drained. Waiting inside the conns
			// goroutine is what makes Run's conns.Wait a full drain.
			var streams sync.WaitGroup

			// A donated connection belongs to the pool, so releasing it means
			// evicting rather than closing: a connection that dies inbound is
			// taken out of the pool instead of handed to a dialer dead.
			if s.pool != nil && s.pool.Donate(conn) {
				defer s.pool.Evict(conn)
			} else {
				defer conn.Close()
			}

			// Cancelled handlers mean the drain deadline expired. Aborting the
			// connection is the only way to unblock a handler parked in a
			// stream read, which has no deadline and never observes ctx.
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
			if err := s.handleStream(handlerCtx, stream); err != nil {
				slog.ErrorContext(acceptCtx, "stream error",
					"err", err,
					"source", conn.LocalAddr(),
					"addr", conn.RemoteAddr())
				// TODO: Figure out better error codes.
				stream.CancelRead(0)
				stream.CancelWrite(0)
			} else {
				stream.Close()
			}
		})
	}
}

func (s *Server) handleStream(ctx context.Context, stream transport.Stream) error {
	buf := make([]byte, 8)
	if _, err := io.ReadFull(stream, buf); err != nil {
		return fmt.Errorf("read metadata: %w", err)
	}
	op := Opcode(binary.BigEndian.Uint32(buf))
	n := binary.BigEndian.Uint32(buf[4:8])
	if n > maxHeaderSize {
		return fmt.Errorf("%w: %v", ErrHeaderTooLarge, n)
	}
	buf = slices.Grow(buf, int(n))
	buf = buf[:8+n]
	if _, err := io.ReadFull(stream, buf[8:]); err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	h, ok := s.mux.handlers[op]
	if !ok {
		return fmt.Errorf("no handler found")
	}
	return h(ctx, buf[8:], stream)
}
