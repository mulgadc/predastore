package rpc

import (
	"context"
	"encoding/binary"
	"fmt"

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

	// Open stream.
	stream, err := conn.OpenStream(ctx)
	if err != nil {
		if conn.Context().Err() != nil {
			// TODO: Define isConnDead function for smart evict.
			c.pool.Evict(conn)
		}
		return nil, err
	}

	// Write stream metadata
	if _, err := stream.Write(buf); err != nil {
		if conn.Context().Err() != nil {
			// TODO: Define isConnDead function for smart evict.
			c.pool.Evict(conn)
		} else {
			// TODO: Figure out better error codes.
			stream.CancelRead(0)
			stream.CancelWrite(0)
		}
		return nil, fmt.Errorf("write metadata + header: %w", err)
	}

	return stream, nil
}
