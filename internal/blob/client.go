package blob

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/mulgadc/predastore/internal/blob/engine"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
)

// ErrNotFound is returned by gets when the node does not hold the value.
var ErrNotFound = errors.New("not found")

// PutRequest identifies the value a put commits. The bytes travel separately,
// as the body reader passed alongside it.
type PutRequest struct {
	Key   [32]byte
	Index uint32
	// Size is the number of body bytes to commit.
	Size int64
}

// PutResponse reports what a node committed.
type PutResponse struct {
	Size int64
	// PoolNearFull reports nearfull free-space pressure at commit time, so
	// callers can back off before writes are rejected outright.
	PoolNearFull bool
}

// GetRequest identifies the value a get reads.
type GetRequest struct {
	Key   [32]byte
	Index uint32
	// RangeStart and RangeEnd bound the read; -1 in either means unset and
	// reads the whole value.
	RangeStart int64
	RangeEnd   int64
}

// DeleteRequest identifies the value a delete removes.
type DeleteRequest struct {
	Key   [32]byte
	Index uint32
}

// DeleteResponse reports whether the node held the value.
type DeleteResponse struct {
	Deleted bool
}

// Client performs value operations against blob nodes over rpc streams,
// addressed by node id.
type Client struct {
	rpc *rpc.Client
}

type ClientConfig struct {
	// Client carries the streams; it owns the mapping from node id to
	// address, so this client only ever names nodes by id.
	Client *rpc.Client
}

func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("missing rpc client")
	}
	return &Client{rpc: cfg.Client}, nil
}

// open starts a stream against the node.
func (c *Client) open(ctx context.Context, nodeID config.NodeID, op rpc.Opcode, h *Request) (transport.Stream, error) {
	stream, err := rpc.OpenStream(ctx, c.rpc, nodeID, op, h)
	if err != nil {
		return nil, fmt.Errorf("open stream to blob node %d: %w", nodeID, err)
	}
	return stream, nil
}

// readEnvelope consumes the newline-terminated response envelope, leaving
// any body bytes in the reader.
func readEnvelope(br *bufio.Reader) (*Response, error) {
	line, err := br.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("read response envelope: %w", err)
	}
	var resp Response
	if err := json.Unmarshal(line, &resp); err != nil {
		return nil, fmt.Errorf("decode response envelope: %w", err)
	}
	return &resp, nil
}

// Put streams a value to the node and returns the commit result.
func (c *Client) Put(ctx context.Context, nodeID config.NodeID, req PutRequest, body io.Reader) (*PutResponse, error) {
	stream, err := c.open(ctx, nodeID, OpPut, &Request{
		Key:        req.Key,
		Index:      req.Index,
		Size:       req.Size,
		RangeStart: -1,
		RangeEnd:   -1,
	})
	if err != nil {
		return nil, err
	}

	if _, err := stream.ReadFrom(io.LimitReader(body, req.Size)); err != nil {
		stream.CancelRead(0)
		stream.CancelWrite(0)
		return nil, fmt.Errorf("stream body to node %d: %w", nodeID, err)
	}
	if err := stream.Close(); err != nil {
		return nil, fmt.Errorf("half-close put stream: %w", err)
	}

	resp, err := readEnvelope(bufio.NewReader(stream))
	if err != nil {
		stream.CancelRead(0)
		return nil, err
	}
	switch resp.Err {
	case "":
	case ErrCodeStoreFull:
		// The engine sentinel rather than an opaque message, so capacity
		// backoff upstream matches the same error either side of the wire.
		return nil, fmt.Errorf("put to node %d: %w", nodeID, engine.ErrStoreFull)
	default:
		return nil, fmt.Errorf("put to node %d: %s", nodeID, resp.Err)
	}
	return &PutResponse{Size: resp.Size, PoolNearFull: resp.PoolNearFull}, nil
}

// Delete marks a value deleted on the node.
func (c *Client) Delete(ctx context.Context, nodeID config.NodeID, req DeleteRequest) (*DeleteResponse, error) {
	stream, err := c.open(ctx, nodeID, OpDelete, &Request{
		Key:        req.Key,
		Index:      req.Index,
		RangeStart: -1,
		RangeEnd:   -1,
	})
	if err != nil {
		return nil, err
	}
	if err := stream.Close(); err != nil {
		return nil, fmt.Errorf("half-close delete stream: %w", err)
	}
	resp, err := readEnvelope(bufio.NewReader(stream))
	if err != nil {
		stream.CancelRead(0)
		return nil, err
	}
	if resp.Err != "" {
		return nil, fmt.Errorf("delete on node %d: %s", nodeID, resp.Err)
	}
	return &DeleteResponse{Deleted: resp.Deleted}, nil
}

// Get streams a value from the node: the whole of it, or the byte range the
// request bounds. The caller must Close the returned reader to release the
// stream.
func (c *Client) Get(ctx context.Context, nodeID config.NodeID, req GetRequest) (io.ReadCloser, error) {
	stream, err := c.open(ctx, nodeID, OpGet, &Request{
		Key:        req.Key,
		Index:      req.Index,
		RangeStart: req.RangeStart,
		RangeEnd:   req.RangeEnd,
	})
	if err != nil {
		return nil, err
	}
	// No request body: half-close immediately so the server can respond.
	if err := stream.Close(); err != nil {
		return nil, fmt.Errorf("half-close get stream: %w", err)
	}

	br := bufio.NewReader(stream)
	resp, err := readEnvelope(br)
	if err != nil {
		stream.CancelRead(0)
		return nil, err
	}
	switch resp.Err {
	case "":
	case ErrCodeNotFound:
		stream.CancelRead(0)
		return nil, fmt.Errorf("get from node %d: %w", nodeID, ErrNotFound)
	default:
		stream.CancelRead(0)
		return nil, fmt.Errorf("get from node %d: %s", nodeID, resp.Err)
	}

	if resp.BodyLen == 0 {
		stream.CancelRead(0)
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	return &bodyReadCloser{r: io.LimitReader(br, resp.BodyLen), stream: stream}, nil
}

// bodyReadCloser hands out the body bytes and releases the stream on Close.
type bodyReadCloser struct {
	r      io.Reader
	stream transport.Stream
}

func (s *bodyReadCloser) Read(p []byte) (int, error) { return s.r.Read(p) }

func (s *bodyReadCloser) Close() error {
	// Abort the read side in case the body was not fully drained; the
	// write side is already closed.
	s.stream.CancelRead(0)
	return nil
}
