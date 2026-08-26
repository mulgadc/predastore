package rpc

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const DefaultReadOpcodeTimeout = 2 * time.Second
const DefaultReadFrameTimeout = 8 * time.Second

const MaxHeaderSize = 1024 * 1024

var ErrHeaderTooLarge = errors.New("header too large")
var ErrServerClosed = errors.New("server closed")

type Handler func(io.ReadWriter) error

type Server struct {
	handlers map[Opcode]Handler

	closed atomic.Bool
	conns  sync.WaitGroup
}

func (s *Server) HandleFunc(code Opcode, fn Handler) { s.handlers[code] = fn }

func (s *Server) Serve(l net.Listener) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			if s.closed.Load() {
				return nil
			}
			// TODO: Delay and retry if non-fatal.
			return err
		}

		s.conns.Go(func() { s.handle(context.TODO(), conn) })
	}
}

type closeWriter interface{ CloseWrite() error }

func (s *Server) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	code, err := readOpcode(conn)
	if err != nil {
		slog.Error("rpc: read opcode: %w", err)
		return
	}

	fn, ok := s.handlers[code]
	if !ok {

	}
}

func (s *Server) Close() error {
	s.closed.Store(true)

	// TODO: Close listeners.
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.closed.Store(true)

	// TODO: Drain conns and close listeners.
}

type Opcode uint16

func readOpcode(conn net.Conn) (Opcode, error) {
	buf := make([]byte, 2)
	conn.SetReadDeadline(time.Now().Add(DefaultReadOpcodeTimeout))
	if _, err := io.ReadFull(conn, buf); err != nil {
		return Opcode(0), err
	}

	return Opcode(binary.BigEndian.Uint16(buf)), nil
}
