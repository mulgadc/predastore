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

const maxHeaderSize = 1024 * 1024

var ErrHeaderTooLarge = errors.New("header too large")

type Opcode uint32

type Header interface {
	Append(buf []byte) ([]byte, error)
	Unmarshal([]byte) error
}

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

type ServerConfig struct {
	Mux          *Mux
	Transports   []transport.Transport
	DrainTimeout time.Duration
}

type Server struct {
	cfg ServerConfig
	lns []transport.Listener
}

func NewServer(cfg ServerConfig) (*Server, error) {
	const defaultDrainTimeout = 30 * time.Second
	if cfg.DrainTimeout == 0 {
		cfg.DrainTimeout = defaultDrainTimeout
	}

	lns := make([]transport.Listener, len(cfg.Transports))
	for _, tr := range cfg.Transports {
		ln, err := tr.Listen()
		if err != nil {
			return nil, err
		}
		lns = append(lns, ln)
	}

	return &Server{
		cfg: cfg,
		lns: lns,
	}, nil
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
				return err
			}
		})
	}

	serveErr := g.Wait()
	for _, ln := range s.lns {
		ln.Close()
	}

	done := make(chan struct{})
	go func() {
		conns.Wait()
		close(done)
	}()

	t := time.NewTimer(s.cfg.DrainTimeout)
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
			slog.ErrorContext(acceptCtx, "accept connection",
				"err", err,
				"addr", ln.Addr())
			var te interface{ Temporary() bool }
			if errors.As(err, &te) && te.Temporary() {
				t := time.NewTimer(backoff)
				defer t.Stop()
				select {
				case <-t.C:
					backoff = min(backoff*2, maxBackoff)
					continue
				case <-acceptCtx.Done():
					return acceptCtx.Err()
				}
			}
			return err
		}
		backoff = 5 * time.Millisecond

		conns.Add(1)
		go s.serveConn(acceptCtx, handlerCtx, conn, conns)
	}
}

func (s *Server) serveConn(
	acceptCtx, handlerCtx context.Context,
	conn transport.Conn,
	conns *sync.WaitGroup,
) {
	defer conns.Done()

	var streams sync.WaitGroup
	for {
		stream, err := conn.AcceptStream(acceptCtx)
		if err != nil {
			slog.ErrorContext(acceptCtx, "accept stream",
				"err", err,
				"source", conn.LocalAddr(),
				"addr", conn.RemoteAddr())
			break
		}

		streams.Add(1)
		go func() {
			defer streams.Done()
			if err := s.handleStream(handlerCtx, stream); err != nil {
				slog.ErrorContext(handlerCtx, "handle stream",
					"err", err,
					"source", stream.LocalAddr(),
					"addr", stream.RemoteAddr())
				// TODO: Figure out better error codes.
				stream.CancelRead(0)
				stream.CancelWrite(0)
				return
			}
			stream.Close()
		}()
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

	h, ok := s.cfg.Mux.handlers[op]
	if !ok {
		return fmt.Errorf("no handler found")
	}
	return h(ctx, buf[8:], stream)
}
