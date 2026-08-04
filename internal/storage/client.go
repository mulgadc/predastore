package storage

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/mulgadc/predastore/quic/quicclient"
	"github.com/mulgadc/predastore/quic/quicserver"
)

// ErrShardNotFound is returned by gets when the node does not hold the shard.
var ErrShardNotFound = errors.New("shard not found")

// Client performs shard operations against storage nodes over rpc streams,
// addressed by node id. Request and response types are shared with the
// legacy quic client so backend call sites are transport-agnostic.
type Client struct {
	rpc     *rpc.Client
	resolve func(nodeID int) (net.Addr, error)
}

type ClientConfig struct {
	// Client carries the streams; its transports decide pipe vs network
	// per address.
	Client *rpc.Client
	// Resolve maps a storage node id to the address to dial.
	Resolve func(nodeID int) (net.Addr, error)
}

func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("storage client: missing rpc client")
	}
	if cfg.Resolve == nil {
		return nil, fmt.Errorf("storage client: missing resolver")
	}
	return &Client{rpc: cfg.Client, resolve: cfg.Resolve}, nil
}

// open starts a shard stream against the node.
func (c *Client) open(ctx context.Context, nodeID int, op rpc.Opcode, h *ShardRequest) (transport.Stream, error) {
	addr, err := c.resolve(nodeID)
	if err != nil {
		return nil, fmt.Errorf("resolve storage node %d: %w", nodeID, err)
	}
	stream, err := rpc.OpenStream(ctx, c.rpc, addr, op, h)
	if err != nil {
		return nil, fmt.Errorf("open stream to storage node %d: %w", nodeID, err)
	}
	return stream, nil
}

// readEnvelope consumes the newline-terminated response envelope, leaving
// any body bytes in the reader.
func readEnvelope(br *bufio.Reader) (*ShardResponse, error) {
	line, err := br.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("read response envelope: %w", err)
	}
	var resp ShardResponse
	if err := json.Unmarshal(line, &resp); err != nil {
		return nil, fmt.Errorf("decode response envelope: %w", err)
	}
	return &resp, nil
}

// PutShard streams a shard to the node and returns the commit result.
func (c *Client) PutShard(ctx context.Context, nodeID int, req quicserver.PutRequest, body io.Reader) (*quicserver.PutResponse, error) {
	stream, err := c.open(ctx, nodeID, OpShardPut, &ShardRequest{
		Bucket:     req.Bucket,
		Object:     req.Object,
		ObjectHash: req.ObjectHash,
		ShardIndex: req.ShardIndex,
		ShardSize:  int64(req.ShardSize),
		RangeStart: -1,
		RangeEnd:   -1,
	})
	if err != nil {
		return nil, err
	}

	if _, err := stream.ReadFrom(io.LimitReader(body, int64(req.ShardSize))); err != nil {
		stream.CancelRead(0)
		stream.CancelWrite(0)
		return nil, fmt.Errorf("stream shard to node %d: %w", nodeID, err)
	}
	if err := stream.Close(); err != nil {
		return nil, fmt.Errorf("half-close shard stream: %w", err)
	}

	resp, err := readEnvelope(bufio.NewReader(stream))
	if err != nil {
		stream.CancelRead(0)
		return nil, err
	}
	switch resp.Err {
	case "":
	case ErrCodeStoreFull:
		// Same sentinel the legacy quic client returns, so capacity
		// backoff logic upstream is transport-agnostic.
		return nil, fmt.Errorf("put shard to node %d: %w", nodeID, quicclient.ErrInsufficientStorage)
	default:
		return nil, fmt.Errorf("put shard to node %d: %s", nodeID, resp.Err)
	}
	return &quicserver.PutResponse{ShardSize: resp.ShardSize, PoolNearFull: resp.PoolNearFull}, nil
}

// DeleteShard marks a shard deleted on the node.
func (c *Client) DeleteShard(ctx context.Context, nodeID int, req quicserver.DeleteRequest) (*quicserver.DeleteResponse, error) {
	stream, err := c.open(ctx, nodeID, OpShardDelete, &ShardRequest{
		Bucket:     req.Bucket,
		Object:     req.Object,
		ObjectHash: req.ObjectHash,
		ShardIndex: req.ShardIndex,
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
		return nil, fmt.Errorf("delete shard on node %d: %s", nodeID, resp.Err)
	}
	return &quicserver.DeleteResponse{Deleted: resp.Deleted}, nil
}

// GetShard streams a whole shard from the node. The caller must Close the
// returned reader to release the stream.
func (c *Client) GetShard(ctx context.Context, nodeID int, req quicserver.ObjectRequest) (io.ReadCloser, error) {
	return c.get(ctx, nodeID, req)
}

// GetShardRange streams the byte range [RangeStart, RangeEnd] of a shard.
func (c *Client) GetShardRange(ctx context.Context, nodeID int, req quicserver.ObjectRequest) (io.ReadCloser, error) {
	return c.get(ctx, nodeID, req)
}

func (c *Client) get(ctx context.Context, nodeID int, req quicserver.ObjectRequest) (io.ReadCloser, error) {
	stream, err := c.open(ctx, nodeID, OpShardGet, &ShardRequest{
		Bucket:     req.Bucket,
		Object:     req.Object,
		ShardIndex: req.ShardIndex,
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
		return nil, fmt.Errorf("get shard from node %d: %w", nodeID, ErrShardNotFound)
	default:
		stream.CancelRead(0)
		return nil, fmt.Errorf("get shard from node %d: %s", nodeID, resp.Err)
	}

	if resp.BodyLen == 0 {
		stream.CancelRead(0)
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	return &shardReadCloser{r: io.LimitReader(br, resp.BodyLen), stream: stream}, nil
}

// shardReadCloser hands out the body bytes and releases the stream on Close.
type shardReadCloser struct {
	r      io.Reader
	stream transport.Stream
}

func (s *shardReadCloser) Read(p []byte) (int, error) { return s.r.Read(p) }

func (s *shardReadCloser) Close() error {
	// Abort the read side in case the body was not fully drained; the
	// write side is already closed.
	s.stream.CancelRead(0)
	return nil
}
