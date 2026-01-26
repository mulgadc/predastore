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
	"github.com/mulgadc/predastore/quic/quicclient"
	"github.com/mulgadc/predastore/quic/quicserver"
	s3db "github.com/mulgadc/predastore/s3db"
)

// GetObject retrieves an object using Reed-Solomon decoding.
// Supports byte-range requests for efficient partial reads.
func (b *Backend) GetObject(ctx context.Context, req *backend.GetObjectRequest) (*backend.GetObjectResponse, error) {
	// Query which nodes have our object shards
	shards, size, err := b.openInput(req.Bucket, req.Key)
	if err != nil {
		return nil, backend.ErrNoSuchKeyError.WithResource(req.Key)
	}

	// Check if this is a range request
	// RangeStart/RangeEnd are initialized to -1 by the S3 router when no Range header is present
	// Any value >= 0 indicates a range request (e.g., "bytes=0-511" has RangeStart=0)
	if req.RangeStart >= 0 || req.RangeEnd >= 0 {
		return b.handleRangeRequest(ctx, req, shards, size)
	}

	// Full object retrieval
	return b.getFullObject(ctx, req, shards, size)
}

// handleRangeRequest handles byte-range requests efficiently by targeting specific shards.
// For Reed-Solomon, data is split sequentially across data shards:
// - shardSize = ceil(originalSize / dataShards)
// - shardIndex = offset / shardSize
// - offsetWithinShard = offset % shardSize
func (b *Backend) handleRangeRequest(ctx context.Context, req *backend.GetObjectRequest, shards ObjectToShardNodes, totalSize int64) (*backend.GetObjectResponse, error) {
	// Normalize range values
	start := req.RangeStart
	end := req.RangeEnd

	if start < 0 {
		start = 0
	}
	if end < 0 || end >= totalSize {
		end = totalSize - 1
	}

	// Validate range
	if start > end || start >= totalSize {
		return nil, backend.ErrInvalidRangeError
	}

	// Calculate shard size (how data is split across data shards)
	shardSize := (totalSize + int64(b.rsDataShard) - 1) / int64(b.rsDataShard)

	// Determine which shard(s) contain the requested range
	startShardIdx := int(start / shardSize)
	endShardIdx := int(end / shardSize)

	// Clamp to valid shard indices
	if startShardIdx >= b.rsDataShard {
		startShardIdx = b.rsDataShard - 1
	}
	if endShardIdx >= b.rsDataShard {
		endShardIdx = b.rsDataShard - 1
	}

	slog.Debug("Range request",
		"bucket", req.Bucket,
		"key", req.Key,
		"start", start,
		"end", end,
		"totalSize", totalSize,
		"shardSize", shardSize,
		"startShard", startShardIdx,
		"endShard", endShardIdx,
		"rsDataShard", b.rsDataShard,
	)

	// Optimization: if range is within a single shard, fetch just that shard portion
	if startShardIdx == endShardIdx {
		data, err := b.readRangeFromSingleShard(ctx, req.Bucket, req.Key, shards, startShardIdx, start, end, shardSize, totalSize)
		if err != nil {
			slog.Warn("Single shard range read failed, falling back to full reconstruction", "err", err)
			// Fall back to full reconstruction
			return b.handleRangeWithFullReconstruction(ctx, req, shards, totalSize, start, end)
		}

		contentRange := fmt.Sprintf("bytes %d-%d/%d", start, end, totalSize)
		return &backend.GetObjectResponse{
			Body:         io.NopCloser(bytes.NewReader(data)),
			ContentType:  "application/octet-stream",
			ContentRange: contentRange,
			Size:         int64(len(data)),
			ETag:         generateDistributedETag(req.Bucket, req.Key),
			StatusCode:   206, // Partial Content
		}, nil
	}

	// Range spans multiple shards - reconstruct needed portions
	return b.handleRangeWithFullReconstruction(ctx, req, shards, totalSize, start, end)
}

// readRangeFromSingleShard reads a byte range from a specific data shard.
// This is the optimized path when the range falls entirely within one shard.
func (b *Backend) readRangeFromSingleShard(ctx context.Context, bucket, key string, shards ObjectToShardNodes, shardIdx int, globalStart, globalEnd, shardSize, totalSize int64) ([]byte, error) {
	if shardIdx >= len(shards.DataShardNodes) {
		return nil, fmt.Errorf("shard index %d out of range", shardIdx)
	}

	// Calculate offset within the shard
	shardStart := int64(shardIdx) * shardSize
	offsetInShard := globalStart - shardStart
	endInShard := globalEnd - shardStart

	// Handle last shard which may be smaller
	actualShardSize := shardSize
	if shardIdx == b.rsDataShard-1 {
		// Last shard may be smaller
		actualShardSize = totalSize - shardStart
		if actualShardSize <= 0 {
			return nil, fmt.Errorf("invalid shard size calculation")
		}
	}

	// Clamp end to shard boundaries
	if endInShard >= actualShardSize {
		endInShard = actualShardSize - 1
	}

	slog.Debug("Reading from single shard",
		"shardIdx", shardIdx,
		"offsetInShard", offsetInShard,
		"endInShard", endInShard,
		"actualShardSize", actualShardSize,
	)

	// Request the specific range from the shard via QUIC
	nodeNum := int(shards.DataShardNodes[shardIdx])
	addr := b.getNodeAddr(nodeNum)

	// Use pooled connection to avoid TLS handshake overhead
	client, err := quicclient.DialPooled(ctx, addr)
	if err != nil {
		return nil, fmt.Errorf("dial node %d: %w", nodeNum, err)
	}
	// Don't close - connection stays in pool

	// Request the shard with range
	objectRequest := quicserver.ObjectRequest{
		Bucket:     bucket,
		Object:     key,
		ShardIndex: shardIdx,
		RangeStart: offsetInShard,
		RangeEnd:   endInShard,
	}

	reader, err := client.GetRange(ctx, objectRequest)
	if err != nil {
		return nil, fmt.Errorf("get range from node %d: %w", nodeNum, err)
	}
	defer reader.Close() // CRITICAL: Close to release QUIC stream back to pool

	// Read the response
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read range data: %w", err)
	}

	return data, nil
}

// handleRangeWithFullReconstruction handles range requests by reconstructing
// the full object and then extracting the requested range.
// This is the fallback path when range spans multiple shards or single-shard read fails.
func (b *Backend) handleRangeWithFullReconstruction(ctx context.Context, req *backend.GetObjectRequest, shards ObjectToShardNodes, totalSize, start, end int64) (*backend.GetObjectResponse, error) {
	slog.Info("handleRangeWithFullReconstruction called",
		"bucket", req.Bucket,
		"key", req.Key,
		"start", start,
		"end", end,
		"totalSize", totalSize,
	)

	// Reconstruct full object
	fullReq := &backend.GetObjectRequest{
		Bucket:     req.Bucket,
		Key:        req.Key,
		RangeStart: -1,
		RangeEnd:   -1,
	}

	fullResp, err := b.getFullObject(ctx, fullReq, shards, totalSize)
	if err != nil {
		slog.Error("Full object reconstruction failed", "err", err)
		return nil, err
	}
	defer fullResp.Body.Close()

	// Read full content
	fullData, err := io.ReadAll(fullResp.Body)
	if err != nil {
		slog.Error("Failed to read full content", "err", err)
		return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	}

	slog.Info("Full object reconstructed",
		"fullDataLen", len(fullData),
		"requestedStart", start,
		"requestedEnd", end,
	)

	// Extract range
	if end >= int64(len(fullData)) {
		end = int64(len(fullData)) - 1
	}
	if start >= int64(len(fullData)) {
		slog.Error("Start position beyond data", "start", start, "dataLen", len(fullData))
		return nil, backend.ErrInvalidRangeError
	}

	rangeData := fullData[start : end+1]
	contentRange := fmt.Sprintf("bytes %d-%d/%d", start, end, totalSize)

	slog.Info("Returning range data",
		"rangeDataLen", len(rangeData),
		"contentRange", contentRange,
	)

	return &backend.GetObjectResponse{
		Body:         io.NopCloser(bytes.NewReader(rangeData)),
		ContentType:  "application/octet-stream",
		ContentRange: contentRange,
		Size:         int64(len(rangeData)),
		ETag:         generateDistributedETag(req.Bucket, req.Key),
		StatusCode:   206, // Partial Content
	}, nil
}

// getFullObject retrieves the complete object using Reed-Solomon decoding.
func (b *Backend) getFullObject(ctx context.Context, req *backend.GetObjectRequest, shards ObjectToShardNodes, size int64) (*backend.GetObjectResponse, error) {
	// Create RS decoder
	enc, err := reedsolomon.NewStream(b.rsDataShard, b.rsParityShard)
	if err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
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
		Body:        io.NopCloser(bytes.NewReader(out.Bytes())),
		ContentType: "application/octet-stream",
		Size:        int64(out.Len()),
		ETag:        generateDistributedETag(req.Bucket, req.Key),
		StatusCode:  200,
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
