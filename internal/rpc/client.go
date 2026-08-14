package rpc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/transport"
)

// Client opens streams to nodes named by id. It holds the pool that turns an
// id into a connection and the framing every stream opens with, and nothing
// else: connection lifetime belongs to the pool.
type Client struct {
	pool *ConnPool
}

func NewClient(pool *ConnPool) *Client { return &Client{pool: pool} }

// OpenStream opens a stream to a node, addressed by id. Whether that node is
// reached over the in-process pipe or the network follows from the route the
// pool dials, which no caller sees.
func OpenStream[T Header](
	ctx context.Context,
	c *Client,
	remote config.NodeID,
	op Opcode,
	header T,
) (transport.Stream, error) {
	// Dial connection.
	conn, err := c.pool.Dial(ctx, remote)
	if err != nil {
		return nil, fmt.Errorf("dial connection: %w", err)
	}

	// Build stream metadata
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf, uint32(op))
	buf, err = header.Append(buf)
	if err != nil {
		return nil, fmt.Errorf("encode header: %w", err)
	}
	n := len(buf) - 8
	if n > maxHeaderSize {
		return nil, fmt.Errorf("%w: %v", ErrHeaderTooLarge, n)
	}
	binary.BigEndian.PutUint32(buf[4:8], uint32(n)) //nolint:gosec // G115: n is bounded by maxHeaderSize above.

	// Open stream. A connection that cannot open one is reported to the pool:
	// no response is ever read over it, so nothing else would ever record that
	// it has stopped carrying traffic, and it would be handed out forever.
	stream, err := conn.OpenStream(ctx)
	if err != nil {
		c.pool.noteFailure(conn)
		return nil, err
	}

	// Write stream metadata
	if _, err := stream.Write(buf); err != nil {
		// TODO: Figure out better error codes.
		stream.CancelRead(0)
		stream.CancelWrite(0)
		c.pool.noteFailure(conn)
		return nil, fmt.Errorf("write metadata + header: %w", err)
	}

	return &healthStream{Stream: stream, pool: c.pool, conn: conn}, nil
}

var _ transport.Stream = (*healthStream)(nil)

// healthStream reports the outcome of a response read back to the pool, which
// has no other way to tell a connection that is serviced from one that takes
// requests and answers none. A stream reports once: the first read decides.
type healthStream struct {
	transport.Stream

	pool     *ConnPool
	conn     transport.Conn
	reported atomic.Bool
}

func (s *healthStream) note(n int64, err error) {
	switch {
	// A clean EOF with no bytes still means the peer served the stream and
	// closed it, which is the opposite of a stall.
	case n > 0 || errors.Is(err, io.EOF):
		if s.reported.CompareAndSwap(false, true) {
			s.pool.noteProgress(s.conn)
		}
	case err != nil:
		if s.reported.CompareAndSwap(false, true) {
			s.pool.noteStall(s.conn)
		}
	}
}

func (s *healthStream) Read(p []byte) (int, error) {
	n, err := s.Stream.Read(p)
	s.note(int64(n), err)
	return n, err
}

func (s *healthStream) WriteTo(w io.Writer) (int64, error) {
	n, err := s.Stream.WriteTo(w)
	s.note(n, err)
	return n, err
}
