package distributed

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/klauspost/reedsolomon"
	"github.com/mulgadc/predastore/backend"
	s3db "github.com/mulgadc/predastore/s3db"
)

// GetObject retrieves an object using Reed-Solomon decoding
func (b *Backend) GetObject(ctx context.Context, req *backend.GetObjectRequest) (*backend.GetObjectResponse, error) {
	// Create RS decoder
	enc, err := reedsolomon.NewStream(b.rsDataShard, b.rsParityShard)
	if err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	}

	// Query which nodes have our object shards
	shards, size, err := b.openInput(req.Bucket, req.Key)
	if err != nil {
		return nil, backend.ErrNoSuchKeyError.WithResource(req.Key)
	}

	// First try with data shards only
	shardReaders, err := b.shardReaders(req.Bucket, req.Key, shards, false)
	if err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	}

	// Buffer to hold the reconstructed object
	var out bytes.Buffer

	// Attempt to join shards
	err = enc.Join(&out, shardReaders, size)
	if err != nil {
		slog.Warn("Initial join failed, attempting reconstruction", "err", err)

		// Try with parity shards for reconstruction
		out.Reset()
		reconstructed, err := b.reconstructObject(ctx, req.Bucket, req.Key, shards, enc, size)
		if err != nil {
			return nil, backend.NewS3Error(backend.ErrInternalError,
				fmt.Sprintf("reconstruction failed: %v", err), 500)
		}
		out = *reconstructed
	}

	// Create response with io.ReadCloser wrapper
	return &backend.GetObjectResponse{
		Body:         io.NopCloser(bytes.NewReader(out.Bytes())),
		ContentType:  "application/octet-stream",
		Size:         int64(out.Len()),
		ETag:         generateDistributedETag(req.Bucket, req.Key),
		StatusCode:   200,
	}, nil
}

// HeadObject returns object metadata
func (b *Backend) HeadObject(ctx context.Context, bucket, key string) (*backend.HeadObjectResponse, error) {
	shards, size, err := b.openInput(bucket, key)
	if err != nil {
		return nil, backend.ErrNoSuchKeyError.WithResource(key)
	}

	_ = shards // Used for validation

	return &backend.HeadObjectResponse{
		ContentType:   "application/octet-stream",
		ContentLength: size,
		ETag:          generateDistributedETag(bucket, key),
	}, nil
}

// reconstructObject attempts to rebuild an object using parity shards
func (b *Backend) reconstructObject(ctx context.Context, bucket, key string, shards ObjectToShardNodes, enc reedsolomon.StreamEncoder, size int64) (*bytes.Buffer, error) {
	// Get all shard readers including parity
	shardReaders, err := b.shardReaders(bucket, key, shards, true)
	if err != nil {
		return nil, err
	}

	// Create reconstruction writers for missing shards
	reconstruction := make([]io.Writer, len(shardReaders))
	files := make([]*os.File, len(shardReaders))

	for i := range reconstruction {
		if shardReaders[i] == nil {
			objHash := s3db.GenObjectHash(bucket, key)
			filename := fmt.Sprintf("%s.%d", hex.EncodeToString(objHash[:]), i)
			outfn := filepath.Join(os.TempDir(), filename)

			files[i], err = os.Create(outfn)
			if err != nil {
				return nil, err
			}
			defer os.Remove(outfn)
			defer files[i].Close()

			slog.Info("Creating temporary file for reconstruction", "filename", outfn)
			reconstruction[i] = files[i]
		}
	}

	// Reconstruct missing shards
	err = enc.Reconstruct(shardReaders, reconstruction)
	if err != nil {
		return nil, fmt.Errorf("reconstruction failed: %w", err)
	}

	// Close reconstruction writers
	for i := range files {
		if files[i] != nil {
			files[i].Close()
		}
	}

	// Re-read shards with reconstructed data
	shardReaders, err = b.shardReaders(bucket, key, shards, true)
	if err != nil {
		return nil, err
	}

	// Fill in reconstructed shards
	for i := range shardReaders {
		if shardReaders[i] == nil && files[i] != nil {
			f, err := os.Open(files[i].Name())
			if err != nil {
				return nil, err
			}
			defer f.Close()
			shardReaders[i] = f
		}
	}

	// Join the shards
	var out bytes.Buffer
	err = enc.Join(&out, shardReaders, size)
	if err != nil {
		return nil, fmt.Errorf("join after reconstruction failed: %w", err)
	}

	return &out, nil
}

// generateDistributedETag creates an ETag for a distributed object
func generateDistributedETag(bucket, key string) string {
	hash := s3db.GenObjectHash(bucket, key)
	return hex.EncodeToString(hash[:16])
}
