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

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/gate/placement"
)

// GetObject serves GET /{bucket}/{key}, reconstructing the object from its
// shards and honouring a byte range when one is asked for.
func GetObject(mc MetaClient, bc BlobClient, ring *placement.Ring, cache *BucketCache, cfg Config) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		resource, ok := routedObject(w, r)
		if !ok {
			return
		}
		bucket, key := resource.Bucket.Name, resource.Key

		// -1 for both ends means "no Range header"; any value >= 0 is a range request.
		rangeStart, rangeEnd := parseRangeHeader(r.Header.Get("Range"))

		if err := requireBucket(ctx, mc, cache, bucket); err != nil {
			HandleError(w, r, err)
			return
		}

		place, size, err := loadPlacement(ctx, mc, ring, cfg, bucket, key)
		if err != nil {
			HandleError(w, r, model.ErrNoSuchKeyError.WithResource(key))
			return
		}

		handoff := handoffNode(ring, cfg, model.ObjectHash(bucket, key))
		serveObject(ctx, w, r, bc, cfg, bucket, key, place, size, rangeStart, rangeEnd, handoff)
	})
}

// serveObject streams an object, or a range of one, to the client.
//
// The first stripe is read before any header is sent. That bounds nothing --
// it is one stripe either way -- but it is what lets the response report the
// reconstruction it cost: after WriteHeader nothing can be added, and an object
// small enough to be a single stripe is fully known by then. On a longer object
// a shard lost after the header has gone reaches the log and the metric but not
// the header, which is why the count there is a floor rather than a total.
func serveObject(
	ctx context.Context, w http.ResponseWriter, r *http.Request,
	bc BlobClient, cfg Config, bucket, key string,
	place ObjectToShardNodes, size, rangeStart, rangeEnd int64, handoff config.NodeID,
) {
	status := http.StatusOK
	start, end := int64(0), size-1
	var contentRange string
	if rangeStart >= 0 || rangeEnd >= 0 {
		var ok bool
		if start, end, ok = resolveRange(size, rangeStart, rangeEnd); !ok {
			HandleError(w, r, model.ErrInvalidRangeError)
			return
		}
		status = http.StatusPartialContent
		contentRange = fmt.Sprintf("bytes %d-%d/%d", start, end, size)
	}

	header := func(length int64, degraded int) {
		if degraded > 0 {
			w.Header().Set(degradedHeader, strconv.Itoa(degraded))
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
		w.Header().Set("ETag", model.ObjectETag(bucket, key))
		w.Header().Set("Last-Modified", lastModified(place))
		if contentRange != "" {
			w.Header().Set("Content-Range", contentRange)
		}
		w.WriteHeader(status)
	}

	// An empty object has no shards: the write path stores none, because the
	// blob protocol has no zero-length value to store.
	if size == 0 {
		header(0, 0)
		return
	}

	objectHash := model.ObjectHash(bucket, key)

	// A range inside one block is one ranged read of one shard, which is what
	// makes a small read of a large object cost a small read.
	lay := newLayout(cfg.DataShards, size, place.BlockSize)
	if status == http.StatusPartialContent && lay.contiguous(start, end) {
		shardIdx, at := lay.locate(start)
		if data, rErr := readRangeFromSingleShard(ctx, bc, objectHash, place, shardIdx, at, end-start+1); rErr == nil {
			header(int64(len(data)), 0)
			if _, wErr := w.Write(data); wErr != nil {
				slog.DebugContext(ctx, "failed to write response body", "error", wErr)
			}
			return
		} else {
			slog.WarnContext(ctx, "Single shard range read failed, falling back to reconstruction", "err", rErr)
		}
	}

	began := time.Now()
	reader, err := newStripeReader(ctx, bc, cfg, objectHash, place, handoff)
	if err != nil {
		HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
		return
	}
	defer reader.close(ctx)

	first, n, err := reader.next(ctx)
	if err != nil {
		HandleError(w, r, model.NewS3Error(model.ErrInternalError,
			fmt.Sprintf("reconstruction failed: %v", err), 500))
		return
	}

	header(end-start+1, reader.reconstructed)

	out := &windowWriter{dst: w, skip: start, limit: end - start + 1}
	if err := drain(ctx, reader, out, first, n, size); err != nil {
		// The header and part of the body have gone; the only honest signal
		// left is to stop short of Content-Length, which every client treats
		// as the failure it is.
		slog.ErrorContext(ctx, "Object read failed after the response began",
			"bucket", bucket, "key", key, "error", err)
		return
	}
	reportDegradedRead(ctx, bucket, key, reader.failures, reader.reconstructed, time.Since(began))
}

// resolveRange clamps a requested range to the object, reporting whether what
// is left is satisfiable.
func resolveRange(size, reqStart, reqEnd int64) (start, end int64, ok bool) {
	start, end = reqStart, reqEnd
	if start < 0 {
		start = 0
	}
	if end < 0 || end >= size {
		end = size - 1
	}
	if start > end || start >= size {
		return 0, 0, false
	}

	return start, end, true
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

// readObject assembles the complete object in memory. It is the streaming read
// with a buffer on the end of it, kept for the callers that genuinely need the
// bytes in hand -- a multipart part being written into a pipe -- so there is
// one read path rather than two that can disagree about a layout.
func readObject(ctx context.Context, bc BlobClient, cfg Config, bucket, key string, place ObjectToShardNodes, size int64, handoff config.NodeID, opts ...stripeOption) ([]byte, int, error) {
	// An empty object has no shards to read: the write path stores none, since
	// the blob protocol has no zero-length value to store.
	if size == 0 {
		return nil, 0, nil
	}

	start := time.Now()
	reader, err := newStripeReader(ctx, bc, cfg, model.ObjectHash(bucket, key), place, handoff, opts...)
	if err != nil {
		return nil, 0, model.NewS3Error(model.ErrInternalError, err.Error(), 500)
	}
	defer reader.close(ctx)

	out := bytes.NewBuffer(make([]byte, 0, size))
	if err := pipeObject(ctx, reader, out, size); err != nil {
		return nil, 0, model.NewS3Error(model.ErrInternalError,
			fmt.Sprintf("reconstruction failed: %v", err), 500)
	}
	reportDegradedRead(ctx, bucket, key, reader.failures, reader.reconstructed, time.Since(start))

	return out.Bytes(), reader.reconstructed, nil
}

// degradedHeader reports how many shards a GET had to reconstruct. It is not
// an error signal: the response is a complete, correct object either way.
const degradedHeader = "X-Spx-Degraded"

// degradedWriteHeader reports how many shards a PUT could not place. Also not
// an error signal: the object is durable, but it survives fewer further losses
// than a full-width write until repair restores the missing shards.
const degradedWriteHeader = "X-Spx-Degraded-Write"

// handoffHeader reports how many shards a PUT placed away from their owner.
// The stripe is complete and the object is as redundant as a full-width write;
// what is outstanding is only that the shards are not yet where the record
// says, which repair settles.
const handoffHeader = "X-Spx-Handoff"

// readRangeFromSingleShard reads length bytes from one data shard starting at
// at: the fast path when the whole range lands inside one block. The layout
// has already resolved where that is, so there is no shard arithmetic here.
func readRangeFromSingleShard(ctx context.Context, bc BlobClient, objectHash [32]byte, place ObjectToShardNodes, shardIdx int, at, length int64) ([]byte, error) {
	if shardIdx >= len(place.DataShardNodes) {
		return nil, fmt.Errorf("shard index %d out of range", shardIdx)
	}

	nodeNum := place.DataShardNodes[shardIdx]
	objectRequest := blob.GetRequest{
		Key:        objectHash,
		Index:      uint32(shardIdx), //nolint:gosec // G115: shardIdx bounded by DataShards (small uint).
		RangeStart: at,
		RangeEnd:   at + length - 1,
		Epoch:      place.WriteEpoch,
	}

	reader, err := bc.Get(ctx, nodeNum, objectRequest)
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
