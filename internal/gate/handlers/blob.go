package handlers

import (
	"context"
	"io"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
)

// BlobClient is the slice of the blob client the handlers use: the three shard
// operations, addressed by node id. It is an interface only so tests can stand
// in for live nodes; production always holds a *blob.Client.
//
// A get reads the whole shard or a byte range of it depending on the bounds
// its request carries, so there is one method rather than two.
type BlobClient interface {
	Put(ctx context.Context, nodeID config.NodeID, req blob.PutRequest, body io.Reader) (*blob.PutResponse, error)
	Get(ctx context.Context, nodeID config.NodeID, req blob.GetRequest) (io.ReadCloser, error)
	Delete(ctx context.Context, nodeID config.NodeID, req blob.DeleteRequest) (*blob.DeleteResponse, error)
}

var _ BlobClient = (*blob.Client)(nil)
