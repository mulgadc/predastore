package distributed

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/mulgadc/predastore/internal/storage"
	"github.com/mulgadc/predastore/quic/quicclient"
	"github.com/mulgadc/predastore/quic/quicserver"
)

// ShardClient performs shard operations against storage nodes, addressed by
// node id. Implementations pick the transport; request and response types
// belong to the storage package so call sites stay transport-agnostic.
type ShardClient interface {
	PutShard(ctx context.Context, nodeID int, req storage.PutRequest, body io.Reader) (*storage.PutResponse, error)
	GetShard(ctx context.Context, nodeID int, req storage.GetRequest) (io.ReadCloser, error)
	GetShardRange(ctx context.Context, nodeID int, req storage.GetRequest) (io.ReadCloser, error)
	DeleteShard(ctx context.Context, nodeID int, req storage.DeleteRequest) (*storage.DeleteResponse, error)
}

var _ ShardClient = (*quicShardClient)(nil)

// quicShardClient reaches storage nodes over pooled QUIC connections using
// per-node addresses: the legacy transport, used when no ShardClient is
// injected.
type quicShardClient struct {
	addr func(nodeID int) string
}

func (c *quicShardClient) PutShard(ctx context.Context, nodeID int, req storage.PutRequest, body io.Reader) (*storage.PutResponse, error) {
	client, err := quicclient.DialPooled(ctx, c.addr(nodeID))
	if err != nil {
		return nil, err
	}
	resp, err := client.Put(ctx, quicserver.PutRequest{
		Bucket:     req.Bucket,
		Object:     req.Object,
		ObjectHash: req.ObjectHash,
		ShardSize:  int(req.ShardSize),
		ShardIndex: req.ShardIndex,
	}, body)
	if err != nil {
		// Restate capacity rejection as the storage sentinel so callers
		// match on one error regardless of transport.
		if errors.Is(err, quicclient.ErrInsufficientStorage) {
			return nil, fmt.Errorf("put shard to node %d: %w", nodeID, storage.ErrStoreFull)
		}
		return nil, err
	}
	return &storage.PutResponse{ShardSize: resp.ShardSize, PoolNearFull: resp.PoolNearFull}, nil
}

func (c *quicShardClient) GetShard(ctx context.Context, nodeID int, req storage.GetRequest) (io.ReadCloser, error) {
	client, err := quicclient.DialPooled(ctx, c.addr(nodeID))
	if err != nil {
		return nil, err
	}
	return client.Get(ctx, objectRequest(req))
}

func (c *quicShardClient) GetShardRange(ctx context.Context, nodeID int, req storage.GetRequest) (io.ReadCloser, error) {
	client, err := quicclient.DialPooled(ctx, c.addr(nodeID))
	if err != nil {
		return nil, err
	}
	return client.GetRange(ctx, objectRequest(req))
}

func (c *quicShardClient) DeleteShard(ctx context.Context, nodeID int, req storage.DeleteRequest) (*storage.DeleteResponse, error) {
	client, err := quicclient.DialPooled(ctx, c.addr(nodeID))
	if err != nil {
		return nil, err
	}
	resp, err := client.Delete(ctx, quicserver.DeleteRequest{
		Bucket:     req.Bucket,
		Object:     req.Object,
		ObjectHash: req.ObjectHash,
		ShardIndex: req.ShardIndex,
	})
	if err != nil {
		return nil, err
	}
	return &storage.DeleteResponse{Deleted: resp.Deleted}, nil
}

// objectRequest maps a shard get onto the quic wire request.
func objectRequest(req storage.GetRequest) quicserver.ObjectRequest {
	return quicserver.ObjectRequest{
		Bucket:     req.Bucket,
		Object:     req.Object,
		RangeStart: req.RangeStart,
		RangeEnd:   req.RangeEnd,
		ShardIndex: req.ShardIndex,
	}
}
