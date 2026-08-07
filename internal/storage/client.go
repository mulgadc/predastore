package storage

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
)

// ErrShardNotFound is returned by gets when the node does not hold the shard.
var ErrShardNotFound = errors.New("shard not found")

// ErrStoreFull is returned by puts the node rejected because its store had
// dropped below the full free-space watermark.
var ErrStoreFull = errors.New("store full")

// PutRequest identifies the shard a put commits. The shard bytes travel
// separately, as the body reader passed alongside it.
type PutRequest struct {
	ObjectHash [32]byte
	ShardIndex uint32
	// ShardSize is the number of body bytes to commit.
	ShardSize int64
}

// PutResponse reports what a node committed.
type PutResponse struct {
	ShardSize int64
	// PoolNearFull reports nearfull free-space pressure at commit time, so
	// callers can back off before writes are rejected outright.
	PoolNearFull bool
}

// GetRequest identifies the shard a get reads.
type GetRequest struct {
	ObjectHash [32]byte
	ShardIndex uint32
	// RangeStart and RangeEnd bound the read; -1 in either means unset and
	// reads the whole shard.
	RangeStart int64
	RangeEnd   int64
}

// DeleteRequest identifies the shard a delete removes.
type DeleteRequest struct {
	ObjectHash [32]byte
	ShardIndex uint32
}

// DeleteResponse reports whether the node held the shard.
type DeleteResponse struct {
	Deleted bool
}

// Client performs shard operations against storage nodes over rpc streams,
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
		return nil, fmt.Errorf("storage client: missing rpc client")
	}
	return &Client{rpc: cfg.Client}, nil
}

// open starts a shard stream against the node.
func (c *Client) open(ctx context.Context, nodeID int, op rpc.Opcode, h *ShardRequest) (transport.Stream, error) {
	stream, err := rpc.OpenStream(ctx, c.rpc, nodeID, op, h)
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
func (c *Client) PutShard(ctx context.Context, nodeID int, req PutRequest, body io.Reader) (*PutResponse, error) {
	stream, err := c.open(ctx, nodeID, OpShardPut, &ShardRequest{
		ObjectHash: req.ObjectHash,
		ShardIndex: req.ShardIndex,
		ShardSize:  req.ShardSize,
		RangeStart: -1,
		RangeEnd:   -1,
	})
	if err != nil {
		return nil, err
	}

	if _, err := stream.ReadFrom(io.LimitReader(body, req.ShardSize)); err != nil {
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
		// Sentinel rather than an opaque message, so capacity backoff
		// upstream can match on it.
		return nil, fmt.Errorf("put shard to node %d: %w", nodeID, ErrStoreFull)
	default:
		return nil, fmt.Errorf("put shard to node %d: %s", nodeID, resp.Err)
	}
	return &PutResponse{ShardSize: resp.ShardSize, PoolNearFull: resp.PoolNearFull}, nil
}

// DeleteShard marks a shard deleted on the node.
func (c *Client) DeleteShard(ctx context.Context, nodeID int, req DeleteRequest) (*DeleteResponse, error) {
	stream, err := c.open(ctx, nodeID, OpShardDelete, &ShardRequest{
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
	return &DeleteResponse{Deleted: resp.Deleted}, nil
}

// GetShard streams a whole shard from the node. The caller must Close the
// returned reader to release the stream.
func (c *Client) GetShard(ctx context.Context, nodeID int, req GetRequest) (io.ReadCloser, error) {
	return c.get(ctx, nodeID, req)
}

// GetShardRange streams the byte range [RangeStart, RangeEnd] of a shard.
func (c *Client) GetShardRange(ctx context.Context, nodeID int, req GetRequest) (io.ReadCloser, error) {
	return c.get(ctx, nodeID, req)
}

func (c *Client) get(ctx context.Context, nodeID int, req GetRequest) (io.ReadCloser, error) {
	stream, err := c.open(ctx, nodeID, OpShardGet, &ShardRequest{
		ObjectHash: req.ObjectHash,
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
