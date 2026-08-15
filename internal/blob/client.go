package blob

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

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

// Default bounds for a client that does not configure its own.
const (
	DefaultEnvelopeTimeout = 10 * time.Second
	DefaultIdleTimeout     = 30 * time.Second
)

// Client performs value operations against blob nodes over rpc streams,
// addressed by node id.
type Client struct {
	rpc             *rpc.Client
	envelopeTimeout time.Duration
	idleTimeout     time.Duration
}

type ClientConfig struct {
	// Client carries the streams; it owns the mapping from node id to
	// address, so this client only ever names nodes by id.
	Client *rpc.Client
	// EnvelopeTimeout bounds the fixed exchanges either side of a body:
	// opening the stream, the half-close, and reading the response envelope.
	// These are small and their size is known, so a total cap fits them. It
	// is a fallback layered on the caller's context, which still wins.
	EnvelopeTimeout time.Duration
	// IdleTimeout bounds a body transfer that stops making progress. It is
	// deliberately not a total duration: a large value transferring steadily
	// must not be cut off, while one that stalls must not block forever.
	IdleTimeout time.Duration
}

func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("missing rpc client")
	}
	if cfg.EnvelopeTimeout <= 0 {
		cfg.EnvelopeTimeout = DefaultEnvelopeTimeout
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = DefaultIdleTimeout
	}
	return &Client{
		rpc:             cfg.Client,
		envelopeTimeout: cfg.EnvelopeTimeout,
		idleTimeout:     cfg.IdleTimeout,
	}, nil
}

// abortStream tears down both directions. It is the cancel action for every
// guard here: a bounded operation that gives up must leave nothing behind on
// the wire for the peer to answer into.
func abortStream(stream transport.Stream) {
	stream.CancelRead(0)
	stream.CancelWrite(0)
}

// open starts a stream against the node.
func (c *Client) open(ctx context.Context, nodeID config.NodeID, op rpc.Opcode, h *Request) (transport.Stream, error) {
	stream, err := rpc.OpenStream(ctx, c.rpc, nodeID, op, h)
	if err != nil {
		return nil, fmt.Errorf("open stream to blob node %d: %w", nodeID, err)
	}
	return stream, nil
}

// maxEnvelopeSize caps the response envelope. Without it a peer that never
// sends a newline grows the buffer until the process runs out of memory,
// which is a denial of service reachable from any blob node.
const maxEnvelopeSize = 64 << 10

// readEnvelope consumes the newline-terminated response envelope, leaving
// any body bytes in the reader. The line is read a byte at a time out of the
// buffered reader so the cap can be enforced without consuming body bytes.
func readEnvelope(br *bufio.Reader) (*Response, error) {
	line := make([]byte, 0, 256)
	for {
		b, err := br.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("read response envelope: %w", err)
		}
		if b == '\n' {
			break
		}
		line = append(line, b)
		if len(line) > maxEnvelopeSize {
			return nil, fmt.Errorf("response envelope exceeds %d bytes", maxEnvelopeSize)
		}
	}
	var resp Response
	if err := json.Unmarshal(line, &resp); err != nil {
		return nil, fmt.Errorf("decode response envelope: %w", err)
	}
	return &resp, nil
}

// openBounded opens a stream under a total cap, since opening is a fixed
// exchange. The returned stop releases the guard without cancelling the
// stream, for a caller moving on to a phase with its own bound.
func (c *Client) openBounded(ctx context.Context, nodeID config.NodeID, op rpc.Opcode, h *Request) (transport.Stream, error) {
	openCtx, cancel := context.WithTimeout(ctx, c.envelopeTimeout)
	defer cancel()
	return c.open(openCtx, nodeID, op, h)
}

// awaitEnvelope reads the response envelope under a total cap, aborting the
// read side if the peer goes quiet. This is where an unbounded client blocks
// forever against a node that accepts a stream and never answers.
func (c *Client) awaitEnvelope(ctx context.Context, stream transport.Stream, br *bufio.Reader) (*Response, error) {
	respCtx, cancel := context.WithTimeout(ctx, c.envelopeTimeout)
	defer cancel()
	stop := context.AfterFunc(respCtx, func() { stream.CancelRead(0) })
	defer stop()

	resp, err := readEnvelope(br)
	if err != nil {
		// Report our own deadline rather than the stream abort it caused.
		if respCtx.Err() != nil && ctx.Err() == nil {
			return nil, fmt.Errorf("await response envelope: %w", context.DeadlineExceeded)
		}
		return nil, err
	}
	return resp, nil
}

// Put streams a value to the node and returns the commit result.
func (c *Client) Put(ctx context.Context, nodeID config.NodeID, req PutRequest, body io.Reader) (*PutResponse, error) {
	stream, err := c.openBounded(ctx, nodeID, OpPut, &Request{
		Key:        req.Key,
		Index:      req.Index,
		Size:       req.Size,
		RangeStart: -1,
		RangeEnd:   -1,
	})
	if err != nil {
		return nil, err
	}

	// The body is bounded by progress, not duration: a large value moving
	// steadily must not be cut off. The caller's cancellation still applies.
	stopCaller := context.AfterFunc(ctx, func() { abortStream(stream) })
	guard := transport.NewWriteGuard(io.LimitReader(body, req.Size), stream, c.idleTimeout)
	_, err = stream.ReadFrom(guard)
	guard.Stop()
	if err != nil {
		stopCaller()
		abortStream(stream)
		if guard.Expired() {
			return nil, fmt.Errorf("stream body to node %d: %w", nodeID, transport.ErrIdleTimeout)
		}
		return nil, fmt.Errorf("stream body to node %d: %w", nodeID, err)
	}
	if err := stream.Close(); err != nil {
		stopCaller()
		abortStream(stream)
		return nil, fmt.Errorf("half-close put stream: %w", err)
	}
	stopCaller()

	resp, err := c.awaitEnvelope(ctx, stream, bufio.NewReader(stream))
	if err != nil {
		stream.CancelRead(0)
		return nil, fmt.Errorf("put to node %d: %w", nodeID, err)
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
	stream, err := c.openBounded(ctx, nodeID, OpDelete, &Request{
		Key:        req.Key,
		Index:      req.Index,
		RangeStart: -1,
		RangeEnd:   -1,
	})
	if err != nil {
		return nil, err
	}
	if err := stream.Close(); err != nil {
		abortStream(stream)
		return nil, fmt.Errorf("half-close delete stream: %w", err)
	}
	resp, err := c.awaitEnvelope(ctx, stream, bufio.NewReader(stream))
	if err != nil {
		stream.CancelRead(0)
		return nil, fmt.Errorf("delete on node %d: %w", nodeID, err)
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
	stream, err := c.openBounded(ctx, nodeID, OpGet, &Request{
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
		abortStream(stream)
		return nil, fmt.Errorf("half-close get stream: %w", err)
	}

	br := bufio.NewReader(stream)
	resp, err := c.awaitEnvelope(ctx, stream, br)
	if err != nil {
		stream.CancelRead(0)
		return nil, fmt.Errorf("get from node %d: %w", nodeID, err)
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

	// The envelope's cap has been released; the body gets a progress bound
	// instead, plus the caller's own cancellation. Both are owned by the
	// returned reader, since the caller reads it after this returns.
	guard := transport.NewReadGuard(io.LimitReader(br, resp.BodyLen), stream, c.idleTimeout)
	body := &bodyReadCloser{
		r:         guard,
		guard:     guard,
		stream:    stream,
		remaining: resp.BodyLen,
	}
	body.stopCaller = context.AfterFunc(ctx, func() { stream.CancelRead(0) })
	return body, nil
}

// bodyReadCloser hands out the body bytes and releases the stream on Close.
// It holds the guards covering the body, which outlive the Get call that
// created them.
type bodyReadCloser struct {
	r          io.Reader
	guard      *transport.IdleGuard
	stream     transport.Stream
	stopCaller func() bool
	// remaining counts down the bytes the envelope promised, so a peer that
	// stops early is reported rather than passed off as a complete value. A
	// short shard accepted silently would be reconstructed into a plausible
	// wrong object, which is worse than a failed read.
	remaining int64
}

func (s *bodyReadCloser) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	s.remaining -= int64(n)
	if errors.Is(err, io.EOF) && s.remaining > 0 {
		return n, fmt.Errorf("short body: %d of %d bytes missing: %w",
			s.remaining, s.remaining+int64(n), io.ErrUnexpectedEOF)
	}
	return n, err
}

func (s *bodyReadCloser) Close() error {
	s.guard.Stop()
	if s.stopCaller != nil {
		s.stopCaller()
	}
	// Abort the read side in case the body was not fully drained; the
	// write side is already closed.
	s.stream.CancelRead(0)
	return nil
}
