package rpc

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/transport"
)

// Client opens streams to nodes named by id. Connection lifetime belongs to
// the pool, not to the streams opened over it.
type Client struct {
	pool *ConnPool
}

func NewClient(pool *ConnPool) *Client { return &Client{pool: pool} }

// OpenStream opens a stream to a node, addressed by id. The returned stream is
// positioned after the request header, ready for the body if the operation has
// one.
func OpenStream[T Header](
	ctx context.Context,
	c *Client,
	remote config.NodeID,
	op Opcode,
	header T,
) (transport.Stream, error) {
	conn, err := c.pool.Dial(ctx, remote)
	if err != nil {
		return nil, fmt.Errorf("dial connection: %w", err)
	}

	// The opcode selects the handler, so it precedes the frame rather than
	// riding inside it. Everything after is framed as the response half is.
	buf, err := appendFrame(binary.BigEndian.AppendUint32(nil, uint32(op)), header)
	if err != nil {
		return nil, err
	}

	stream, err := conn.OpenStream(ctx)
	if err != nil {
		if conn.Context().Err() != nil {
			// TODO: Define isConnDead function for smart evict.
			c.pool.Evict(conn)
		}
		return nil, err
	}

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
