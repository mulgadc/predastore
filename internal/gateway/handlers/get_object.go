package handlers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/klauspost/reedsolomon"
	"github.com/mulgadc/predastore/internal/gateway/model"
	"github.com/mulgadc/predastore/internal/gateway/placement"
	"github.com/mulgadc/predastore/internal/storage"
)

// GetObject serves GET /{bucket}/{key}, reconstructing the object from its
// shards and honouring a byte range when one is asked for.
func GetObject(st Store, shards *storage.Client, ring *placement.Ring, cache *BucketCache, cfg Config) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		bucket := chi.URLParam(r, "bucket")
		key := chi.URLParam(r, "*")

		// -1 for both ends means "no Range header"; any value >= 0 is a range request.
		rangeStart, rangeEnd := parseRangeHeader(r.Header.Get("Range"))

		if err := requireBucket(st, cache, bucket); err != nil {
			HandleError(w, r, err)
			return
		}

		place, size, err := openInput(st, ring, cfg, bucket, key)
		if err != nil {
			HandleError(w, r, model.ErrNoSuchKeyError.WithResource(key))
			return
		}

		var body []byte
		var contentRange string
		status := http.StatusOK

		if rangeStart >= 0 || rangeEnd >= 0 {
			body, contentRange, err = readRange(ctx, shards, cfg, bucket, key, place, size, rangeStart, rangeEnd)
			status = http.StatusPartialContent
		} else {
			body, err = readObject(ctx, shards, cfg, bucket, key, place, size)
		}
		if err != nil {
			HandleError(w, r, err)
			return
		}

		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Header().Set("ETag", model.ObjectETag(bucket, key))
		w.Header().Set("Last-Modified", time.Time{}.Format(httpTimeFormat))

		if status == http.StatusPartialContent {
			w.Header().Set("Content-Range", contentRange)
		}
		w.WriteHeader(status)

		if _, err := w.Write(body); err != nil {
			slog.DebugContext(ctx, "failed to write response body", "error", err)
		}
	})
}

// parseRangeHeader extracts a single byte range. An absent or unparseable
// header yields (-1, -1), meaning the whole object.
func parseRangeHeader(header string) (start, end int64) {
	start, end = -1, -1
	if !strings.HasPrefix(header, "bytes=") {
		return start, end
	}
	spec := header[len("bytes="):]
	idx := strings.Index(spec, "-")
	if idx < 0 {
		return start, end
	}
	if idx > 0 {
		start, _ = strconv.ParseInt(spec[:idx], 10, 64)
	}
	if idx < len(spec)-1 {
		end, _ = strconv.ParseInt(spec[idx+1:], 10, 64)
	}
	return start, end
}

// readObject reconstructs the complete object from its data shards, falling
// back to parity reconstruction when the data shards alone will not join.
func readObject(ctx context.Context, client *storage.Client, cfg Config, bucket, key string, shards ObjectToShardNodes, size int64) ([]byte, error) {
	// The stream encoder is constructed per request; hoisting it into the
	// gateway belongs with the streaming refactor, not here.
	enc, err := reedsolomon.NewStream(cfg.DataShards, cfg.ParityShards)
	if err != nil {
		return nil, model.NewS3Error(model.ErrInternalError, err.Error(), 500)
	}

	objectHash := model.ObjectHash(bucket, key)

	readers, err := shardReaders(client, objectHash, shards, false)
	if err != nil {
		return nil, model.NewS3Error(model.ErrInternalError, err.Error(), 500)
	}

	var out bytes.Buffer
	if err := enc.Join(&out, readers, size); err != nil {
		slog.WarnContext(ctx, "Initial join failed, attempting reconstruction", "err", err)

		out.Reset()
		reconstructed, err := reconstructObject(ctx, client, objectHash, shards, enc, size)
		if err != nil {
			return nil, model.NewS3Error(model.ErrInternalError,
				fmt.Sprintf("reconstruction failed: %v", err), 500)
		}
		out = *reconstructed
	}

	return out.Bytes(), nil
}

// readRange serves a byte range. Reed-Solomon splits data sequentially across
// the data shards, so a range inside one shard is a single ranged shard read;
// anything wider falls back to reconstructing the object and slicing it.
func readRange(ctx context.Context, client *storage.Client, cfg Config, bucket, key string, shards ObjectToShardNodes, totalSize, reqStart, reqEnd int64) (data []byte, contentRange string, err error) {
	start, end := reqStart, reqEnd
	if start < 0 {
		start = 0
	}
	if end < 0 || end >= totalSize {
		end = totalSize - 1
	}
	if start > end || start >= totalSize {
		return nil, "", model.ErrInvalidRangeError
	}

	shardSize := (totalSize + int64(cfg.DataShards) - 1) / int64(cfg.DataShards)
	startShardIdx := min(int(start/shardSize), cfg.DataShards-1)
	endShardIdx := min(int(end/shardSize), cfg.DataShards-1)

	if startShardIdx == endShardIdx {
		objectHash := model.ObjectHash(bucket, key)
		data, err := readRangeFromSingleShard(ctx, client, cfg, objectHash, shards, startShardIdx, start, end, shardSize, totalSize)
		if err == nil {
			return data, fmt.Sprintf("bytes %d-%d/%d", start, end, totalSize), nil
		}
		slog.WarnContext(ctx, "Single shard range read failed, falling back to full reconstruction", "err", err)
	}

	full, err := readObject(ctx, client, cfg, bucket, key, shards, totalSize)
	if err != nil {
		slog.ErrorContext(ctx, "Full object reconstruction failed", "err", err)
		return nil, "", err
	}

	if end >= int64(len(full)) {
		end = int64(len(full)) - 1
	}
	if start >= int64(len(full)) {
		slog.ErrorContext(ctx, "Start position beyond data", "start", start, "dataLen", len(full))
		return nil, "", model.ErrInvalidRangeError
	}

	return full[start : end+1], fmt.Sprintf("bytes %d-%d/%d", start, end, totalSize), nil
}

// readRangeFromSingleShard reads a byte range from one data shard: the fast
// path when the whole range lands inside it.
func readRangeFromSingleShard(ctx context.Context, client *storage.Client, cfg Config, objectHash [32]byte, shards ObjectToShardNodes, shardIdx int, globalStart, globalEnd, shardSize, totalSize int64) ([]byte, error) {
	if shardIdx >= len(shards.DataShardNodes) {
		return nil, fmt.Errorf("shard index %d out of range", shardIdx)
	}

	shardStart := int64(shardIdx) * shardSize
	offsetInShard := globalStart - shardStart
	endInShard := globalEnd - shardStart

	// The last shard is short whenever the object does not divide evenly.
	actualShardSize := shardSize
	if shardIdx == cfg.DataShards-1 {
		actualShardSize = totalSize - shardStart
		if actualShardSize <= 0 {
			return nil, fmt.Errorf("invalid shard size calculation")
		}
	}
	if endInShard >= actualShardSize {
		endInShard = actualShardSize - 1
	}

	nodeNum := shards.DataShardNodes[shardIdx]
	objectRequest := storage.GetRequest{
		ObjectHash: objectHash,
		ShardIndex: uint32(shardIdx), //nolint:gosec // G115: shardIdx bounded by DataShards (small uint).
		RangeStart: offsetInShard,
		RangeEnd:   endInShard,
	}

	reader, err := client.GetShardRange(ctx, nodeNum, objectRequest)
	if err != nil {
		return nil, fmt.Errorf("get range from node %d: %w", nodeNum, err)
	}
	defer reader.Close() // CRITICAL: Close to release the stream back to the pool

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read range data: %w", err)
	}

	return data, nil
}
