package distributed

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"

	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/quic/quicclient"
	s3db "github.com/mulgadc/predastore/s3db"
)

// arnObjectPrefix is the ARN prefix for object keys
// Format: arn:aws:s3:::<bucket>/<key>.
const arnObjectPrefixPut = "arn:aws:s3:::"

// mapPutErr translates a putObjectViaQUIC error into the S3 error returned
// to the client. A pool-full shard write must surface as 507, not the
// generic 500 other failures get.
func mapPutErr(err error) *backend.S3Error {
	if errors.Is(err, quicclient.ErrInsufficientStorage) {
		return backend.ErrInsufficientStorageError
	}
	var maxBytesErr *http.MaxBytesError
	if errors.As(err, &maxBytesErr) {
		return backend.NewS3Error(backend.ErrEntityTooLarge, err.Error(), http.StatusBadRequest)
	}
	if errors.Is(err, errObjectBodyShort) || errors.Is(err, errObjectBodyLong) {
		return backend.NewS3Error(backend.ErrInvalidRequest, err.Error(), http.StatusBadRequest)
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

	objectHash := s3db.GenObjectHash(req.Bucket, req.Key)

	objectToShardNodes := ObjectToShardNodes{
		Object:           objectHash,
		DataShardNodes:   make([]uint32, b.rsDataShard),
		ParityShardNodes: make([]uint32, b.rsParityShard),
	}

	poolNearFull, err := b.putObjectViaQUIC(ctx, req.Bucket, req.Key, req.Body, req.ContentLength, objectHash)
	if err != nil {
		slog.Error("distributed.PutObject: shard distribution failed", "error", err)
		return nil, mapPutErr(err)
	}

	objectToShardNodes.Size = req.ContentLength

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
	if err := b.globalState.Set(TableObjects, objectHash[:], buf.Bytes()); err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	}

	// Store ARN key -> object hash (for listing)
	// Format: arn:aws:s3:::<bucket>/<key>
	arnKey := []byte(arnObjectPrefixPut + req.Bucket + "/" + req.Key)
	if err := b.globalState.Set(TableObjects, arnKey, objectHash[:]); err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	}

	return &backend.PutObjectResponse{
		ETag:         generateDistributedETag(req.Bucket, req.Key),
		PoolNearFull: poolNearFull,
	}, nil
}

// PutObjectFromPath stores an object from a file path (used internally and for testing).
func (b *Backend) PutObjectFromPath(ctx context.Context, bucket, objectPath string) error {
	objectHash := s3db.GenObjectHash(bucket, objectPath)

	objectToShardNodes := ObjectToShardNodes{}

	// Check if existing
	data, err := b.globalState.Get(TableObjects, objectHash[:])

	if err != nil {
		// Key not found or other error - treat as new object
		objectToShardNodes = ObjectToShardNodes{
			Object:           objectHash,
			DataShardNodes:   make([]uint32, b.rsDataShard),
			ParityShardNodes: make([]uint32, b.rsParityShard),
		}
	} else {
		// Decode existing metadata
		r := bytes.NewReader(data)
		dec := gob.NewDecoder(r)

		if err := dec.Decode(&objectToShardNodes); err != nil {
			return err
		}
	}

	f, err := os.Open(objectPath)
	if err != nil {
		return fmt.Errorf("open object %s: %w", objectPath, err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat object %s: %w", objectPath, err)
	}

	_, err = b.putObjectViaQUIC(ctx, bucket, objectPath, f, stat.Size(), objectHash)
	if err != nil {
		return err
	}

	objectToShardNodes.Size = stat.Size()

	// Get hash ring placement using objectHash for consistency with putObjectViaQUIC
	hashRingShards, err := b.hashRing.GetClosestN(objectHash[:], b.rsDataShard+b.rsParityShard)
	if err != nil {
		return err
	}

	// Record which nodes have data shards
	for i := 0; i < b.rsDataShard; i++ {
		objectToShardNodes.DataShardNodes[i], err = NodeToUint32(hashRingShards[i].String())
		if err != nil {
			return err
		}
	}

	// Record which nodes have parity shards
	for i := 0; i < b.rsParityShard; i++ {
		objectToShardNodes.ParityShardNodes[i], err = NodeToUint32(hashRingShards[b.rsDataShard+i].String())
		if err != nil {
			return err
		}
	}

	// Encode object metadata
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(objectToShardNodes); err != nil {
		return err
	}

	// Store object metadata using GlobalState
	return b.globalState.Set(TableObjects, objectHash[:], buf.Bytes())
}

// GetFromPath retrieves an object and writes to the provided writer (used for testing).
func (b *Backend) GetFromPath(ctx context.Context, bucket, objectPath string, out *bytes.Buffer) error {
	req := &backend.GetObjectRequest{
		Bucket:     bucket,
		Key:        objectPath,
		RangeStart: -1, // -1 means "not specified" (full object)
		RangeEnd:   -1,
	}

	resp, err := b.GetObject(ctx, req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}
