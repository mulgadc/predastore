package blob

import (
	"context"
	"fmt"
	"io"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
)

// Client performs value operations against blob nodes over rpc streams,
// addressed by node id.
//
// A failure the node reported arrives as a *rpc.ResponseError carrying the
// status it answered with. Callers match on that status rather than on a
// sentinel, so a code means the same thing on either side of the wire.
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

// Put streams a value to the node and returns the commit result.
func (c *Client) Put(ctx context.Context, nodeID config.NodeID, req PutRequest, body io.Reader) (*PutResponse, error) {
	stream, err := rpc.OpenStream(ctx, c.rpc, nodeID, OpPut, &req)
	if err != nil {
		return nil, fmt.Errorf("open put stream to node %d: %w", nodeID, err)
	}

	// The body is bounded by what the header declared, so an over-long one is
	// truncated here rather than overrunning the value the node reserved.
	//nolint:gosec // G115: a declared body length never exceeds int64.
	if _, err := stream.ReadFrom(io.LimitReader(body, int64(req.Size))); err != nil {
		// Reset both halves: a partial body commits nothing, so there is no
		// answer worth waiting for.
		stream.CancelRead(0)
		stream.CancelWrite(0)
		return nil, fmt.Errorf("stream body to node %d: %w", nodeID, err)
	}

	// Half-close: the body is complete, and the node answers on its own half.
	if err := stream.Close(); err != nil {
		return nil, fmt.Errorf("half-close put stream: %w", err)
	}

	resp, err := rpc.ReadResponse[PutResponse](stream)
	if err != nil {
		stream.CancelRead(0)
		return nil, fmt.Errorf("put to node %d: %w", nodeID, err)
	}
	return &resp, nil
}

// Delete marks a value deleted on the node.
func (c *Client) Delete(ctx context.Context, nodeID config.NodeID, req DeleteRequest) (*DeleteResponse, error) {
	stream, err := rpc.OpenStream(ctx, c.rpc, nodeID, OpDelete, &req)
	if err != nil {
		return nil, fmt.Errorf("open delete stream to node %d: %w", nodeID, err)
	}
	// No request body: half-close immediately so the node can answer.
	if err := stream.Close(); err != nil {
		return nil, fmt.Errorf("half-close delete stream: %w", err)
	}

	resp, err := rpc.ReadResponse[DeleteResponse](stream)
	if err != nil {
		stream.CancelRead(0)
		return nil, fmt.Errorf("delete on node %d: %w", nodeID, err)
	}
	return &resp, nil
}

// Get streams a value from the node: the whole of it, or the byte range the
// request bounds. The caller must Close the returned reader to release the
// stream.
func (c *Client) Get(ctx context.Context, nodeID config.NodeID, req GetRequest) (io.ReadCloser, error) {
	stream, err := rpc.OpenStream(ctx, c.rpc, nodeID, OpGet, &req)
	if err != nil {
		return nil, fmt.Errorf("open get stream to node %d: %w", nodeID, err)
	}
	// No request body: half-close immediately so the node can answer.
	if err := stream.Close(); err != nil {
		return nil, fmt.Errorf("half-close get stream: %w", err)
	}

	// Reading the frame leaves the stream on the first body byte, so the caller
	// can take the rest of it as the value.
	if _, err := rpc.ReadResponse[GetResponse](stream); err != nil {
		stream.CancelRead(0)
		return nil, fmt.Errorf("get from node %d: %w", nodeID, err)
	}
	return &bodyReadCloser{stream: stream}, nil
}

// bodyReadCloser hands out the response body and releases the stream on Close.
// A clean EOF is the whole body; a truncated one surfaces as a
// transport.StreamError.
type bodyReadCloser struct {
	stream transport.Stream
}

func (s *bodyReadCloser) Read(p []byte) (int, error) { return s.stream.Read(p) }

func (s *bodyReadCloser) Close() error {
	// Abort the read side in case the body was not fully drained; the
	// write side is already closed.
	s.stream.CancelRead(0)
	return nil
}
