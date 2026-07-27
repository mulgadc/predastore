package distributed

import (
	"context"
	"io"

	"github.com/mulgadc/predastore/quic/quicclient"
	"github.com/mulgadc/predastore/quic/quicserver"
)

// ShardClient performs shard operations against storage nodes, addressed by
// node id. Implementations pick the transport; request and response types
// are shared with the quic packages so call sites stay transport-agnostic.
type ShardClient interface {
	PutShard(ctx context.Context, nodeID int, req quicserver.PutRequest, body io.Reader) (*quicserver.PutResponse, error)
	GetShard(ctx context.Context, nodeID int, req quicserver.ObjectRequest) (io.ReadCloser, error)
	GetShardRange(ctx context.Context, nodeID int, req quicserver.ObjectRequest) (io.ReadCloser, error)
	DeleteShard(ctx context.Context, nodeID int, req quicserver.DeleteRequest) (*quicserver.DeleteResponse, error)
}

var _ ShardClient = (*quicShardClient)(nil)

// quicShardClient reaches storage nodes over pooled QUIC connections using
// per-node addresses: the legacy transport, used when no ShardClient is
// injected.
type quicShardClient struct {
	addr func(nodeID int) string
}

func (c *quicShardClient) PutShard(ctx context.Context, nodeID int, req quicserver.PutRequest, body io.Reader) (*quicserver.PutResponse, error) {
	client, err := quicclient.DialPooled(ctx, c.addr(nodeID))
	if err != nil {
		return nil, err
	}
	return client.Put(ctx, req, body)
}

func (c *quicShardClient) GetShard(ctx context.Context, nodeID int, req quicserver.ObjectRequest) (io.ReadCloser, error) {
	client, err := quicclient.DialPooled(ctx, c.addr(nodeID))
	if err != nil {
		return nil, err
	}
	return client.Get(ctx, req)
}

func (c *quicShardClient) GetShardRange(ctx context.Context, nodeID int, req quicserver.ObjectRequest) (io.ReadCloser, error) {
	client, err := quicclient.DialPooled(ctx, c.addr(nodeID))
	if err != nil {
		return nil, err
	}
	return client.GetRange(ctx, req)
}

func (c *quicShardClient) DeleteShard(ctx context.Context, nodeID int, req quicserver.DeleteRequest) (*quicserver.DeleteResponse, error) {
	client, err := quicclient.DialPooled(ctx, c.addr(nodeID))
	if err != nil {
		return nil, err
	}
	return client.Delete(ctx, req)
}
