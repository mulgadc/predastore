package distributed

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"io"
	"log/slog"
	"os"

	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/internal/storage"
	"github.com/mulgadc/predastore/s3/chunked"
)

// arnObjectPrefix is the ARN prefix for object keys
// Format: arn:aws:s3:::<bucket>/<key>.
const arnObjectPrefixPut = "arn:aws:s3:::"

// mapPutErr translates a putObjectViaQUIC error into the S3 error returned
// to the client. A pool-full shard write must surface as 507, not the
// generic 500 other failures get.
func mapPutErr(err error) *backend.S3Error {
	if errors.Is(err, storage.ErrStoreFull) {
		return backend.ErrInsufficientStorageError
	}
	return backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
}

// PutObject stores an object using Reed-Solomon encoding across multiple nodes.
func (b *Backend) PutObject(ctx context.Context, req *backend.PutObjectRequest) (*backend.PutObjectResponse, error) {
	if req.Bucket == "" {
		return nil, backend.ErrNoSuchBucketError.WithResource(req.Bucket)
	}
	if req.Key == "" {
		return nil, backend.ErrNoSuchKeyError.WithResource(req.Key)
	}

	if _, err := b.HeadBucket(ctx, &backend.HeadBucketRequest{Bucket: req.Bucket}); err != nil {
		return nil, err
	}

	objectHash := storage.GenObjectHash(req.Bucket, req.Key)

	objectToShardNodes := ObjectToShardNodes{
		Object:           objectHash,
		DataShardNodes:   make([]uint32, b.rsDataShard),
		ParityShardNodes: make([]uint32, b.rsParityShard),
	}

	// Write object to a temporary file for RS splitting and QUIC distribution
	tmpFile, err := os.CreateTemp("", "distributed-put-*")
	if err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Copy body to temp file, handling chunked encoding if needed
	if req.Body != nil {
		reader := req.Body
		if req.IsChunked && req.ContentEncoding == "aws-chunked" {
			reader = chunked.NewDecoder(req.Body, req.DecodedLength)
		}
		_, err = io.Copy(tmpFile, reader)
		if err != nil {
			slog.Error("distributed.PutObject: copy to temp file failed", "error", err)
			return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
		}
	}
	if closeErr := tmpFile.Close(); closeErr != nil {
		slog.Debug("Failed to close temp file", "path", tmpFile.Name(), "error", closeErr)
	}

	var size int64
	var poolNearFull bool
	size, poolNearFull, err = b.putObjectViaQUIC(ctx, req.Bucket, tmpFile.Name(), objectHash)
	if err != nil {
		slog.Error("distributed.PutObject: shard distribution failed", "error", err)
		return nil, mapPutErr(err)
	}

	objectToShardNodes.Size = size

	// Get hash ring placement using objectHash for consistency with storage and retrieval
	hashRingShards, err := b.hashRing.GetClosestN(objectHash[:], b.rsDataShard+b.rsParityShard)
	if err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	}

	// Record which nodes have data shards
	for i := 0; i < b.rsDataShard; i++ {
		objectToShardNodes.DataShardNodes[i], err = NodeToUint32(hashRingShards[i].String())
		if err != nil {
			return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
		}
	}

	// Record which nodes have parity shards
	for i := 0; i < b.rsParityShard; i++ {
		objectToShardNodes.ParityShardNodes[i], err = NodeToUint32(hashRingShards[b.rsDataShard+i].String())
		if err != nil {
			return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
		}
	}

	// Encode object metadata
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(objectToShardNodes); err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	}

	// Store object hash -> shard metadata (for retrieval)
	if err := b.globalState.Put(TableObjects, string(objectHash[:]), buf.Bytes()); err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	}

	// Store ARN key -> object hash (for listing)
	// Format: arn:aws:s3:::<bucket>/<key>
	arnKey := arnObjectPrefixPut + req.Bucket + "/" + req.Key
	if err := b.globalState.Put(TableObjects, arnKey, objectHash[:]); err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	}

	return &backend.PutObjectResponse{
		ETag:         generateDistributedETag(req.Bucket, req.Key),
		PoolNearFull: poolNearFull,
	}, nil
}
