package rpc

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"

	"github.com/mulgadc/predastore/internal/transport"
)

type connKey struct {
	network string
	string  string
}

type ClientConfig struct {
	Transports []transport.Transport
}

type Client struct {
	trs   map[string]transport.Transport
	conns map[connKey]transport.Conn
	mu    sync.Mutex
}

func NewClient(cfg ClientConfig) *Client {
	trs := make(map[string]transport.Transport)
	for _, tr := range cfg.Transports {
		trs[tr.Addr().Network()] = tr
	}

	return &Client{
		trs:   trs,
		conns: make(map[connKey]transport.Conn),
	}
}

func OpenStream[T Header](
	ctx context.Context,
	c *Client,
	addr net.Addr,
	op Opcode,
	header T,
) (transport.Stream, error) {
	c.mu.Lock()
	key := connKey{
		network: addr.Network(),
		string:  addr.String(),
	}
	conn, ok := c.conns[key]
	if !ok {
		tr, ok := c.trs[addr.Network()]
		if !ok {
			c.mu.Unlock()
			return nil, fmt.Errorf("no %s transport available", addr.Network())
		}
		var err error
		conn, err = tr.Dial(ctx, addr)
		if err != nil {
			c.mu.Unlock()
			return nil, fmt.Errorf("dial target address: %w", err)
		}
		c.conns[key] = conn
	}
	c.mu.Unlock()

	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf, uint32(op))
	buf, err := header.Append(buf)
	if err != nil {
		return nil, fmt.Errorf("encode header: %w", err)
	}
	n := len(buf) - 8
	if n > maxHeaderSize {
		return nil, fmt.Errorf("%w: %v", ErrHeaderTooLarge, n)
	}
	binary.BigEndian.PutUint32(buf[4:8], uint32(n)) //nolint:gosec // G115: n is bounded by maxHeaderSize above.

	stream, err := conn.OpenStream(ctx)
	if err != nil {
		return nil, fmt.Errorf("open from connection: %w", err)
	}
	if _, err := stream.Write(buf); err != nil {
		// TODO: Figure out better error codes.
		stream.CancelRead(0)
		stream.CancelWrite(0)
		return nil, fmt.Errorf("write metadata + header: %w", err)
	}

	return stream, nil
}
