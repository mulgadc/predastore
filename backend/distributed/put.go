package distributed

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"sync"

	"github.com/klauspost/reedsolomon"
	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/quic/quicclient"
	"github.com/mulgadc/predastore/quic/quicserver"
	"github.com/mulgadc/predastore/s3/chunked"
	s3db "github.com/mulgadc/predastore/s3db"
	"golang.org/x/sync/errgroup"
)

const frameSize = 64 * 1024

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

	totalShards := b.rsDataShard + b.rsParityShard
	shardSize := (frameSize + totalShards - 1) / totalShards

	// Get shard placement set using objectHash.
	placementSet, err := b.hashRing.GetClosestN(objectHash[:], totalShards)
	if err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	}

	// Record which nodes have data shards.
	for i := 0; i < b.rsDataShard; i++ {
		objectToShardNodes.DataShardNodes[i], err = NodeToUint32(placementSet[i].String())
		if err != nil {
			return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
		}
	}

	rs, err := reedsolomon.New(b.rsDataShard, b.rsParityShard)
	if err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	}

	// Record which nodes have parity shards.
	for i := 0; i < b.rsParityShard; i++ {
		objectToShardNodes.ParityShardNodes[i], err = NodeToUint32(placementSet[b.rsDataShard+i].String())
		if err != nil {
			return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
		}
	}

	// Handle aws-chunked encoding if present.
	body := req.Body
	if req.IsChunked && req.ContentEncoding == "aws-chunked" {
		body = chunked.NewDecoder(req.Body, req.DecodedLength)
	}

	free := make(chan []byte, 2)
	for range 2 {
		free <- make([]byte, shardSize*totalShards)
	}

	g, ctx := errgroup.WithContext(ctx)

	collect := func(r io.Reader) <-chan []byte {
		out := make(chan []byte)
		g.Go(func() error {
			defer close(out)
			for {
				var buf []byte
				select {
				case buf = <-free:
				case <-ctx.Done():
					return ctx.Err()
				}

				n, err := io.ReadFull(r, buf[:frameSize])
				switch {
				case err == io.EOF:
					return nil
				case err == io.ErrUnexpectedEOF:
					// Send partial frame, then exit.
				case err != nil:
					return fmt.Errorf("collect frame: %w", err)
				}

				select {
				case out <- buf[:n]:
				case <-ctx.Done():
					return ctx.Err()
				}

				if err == io.ErrUnexpectedEOF {
					return nil
				}
			}
		})
		return out
	}

	shard := func(in <-chan []byte) <-chan [][]byte {
		out := make(chan [][]byte)
		g.Go(func() error {
			defer close(out)
			for frame := range in {
				shards, err := rs.Split(frame)
				if err != nil {
					return fmt.Errorf("split frame: %w", err)
				}

				err = rs.Encode(shards)
				if err != nil {
					return fmt.Errorf("encode parity: %w", err)
				}

				out <- shards
			}
			return nil
		})
		return out
	}

	put := func(in <-chan []byte) <-chan 

	var wg sync.WaitGroup
	for idx, node := range placementSet {
		id, err := NodeToUint32(node.String())
		if err != nil {
			return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
		}

		putReq := quicserver.PutRequest{
			ObjectHash: objectHash,
			ShardIndex: uint32(idx), //nolint:gosec // G115: idx bounded by rsDataShard+rsParityShard.
			ShardSize:  len(shardData),
		}

		r, w := io.Pipe()

		wg.Go(func() {
			res, err := b.shards.PutShard(ctx, int(id), putReq, r)
			if err != nil {
				slog.Error("put shar failed", "node", id, "error", err)
				return
			}
		})
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

	var size int64
	size, _, err = b.putObjectViaQUIC(ctx, bucket, objectPath, objectHash)
	if err != nil {
		return err
	}

	objectToShardNodes.Size = size

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
