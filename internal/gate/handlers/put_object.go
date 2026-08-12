package handlers

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/klauspost/reedsolomon"
	"github.com/mulgadc/predastore/internal/gate/chunked"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/gate/placement"
)

// PutObject serves PUT /{bucket}/{key}: the body is erasure coded across the
// blob nodes as it is read, and its placement recorded in global state under
// both the object hash and the listing key.
func PutObject(mc MetaClient, bc BlobClient, ring *placement.Ring, cache *BucketCache, cfg Config) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		resource, ok := routedObject(w, r)
		if !ok {
			return
		}
		bucket, key := resource.Bucket.Name, resource.Key

		if err := requireBucket(ctx, mc, cache, bucket); err != nil {
			HandleError(w, r, err)
			return
		}

		/* Per-object settings */

		// dataShards is the number of data shards that a stripe is split into before erasure coding.
		dataShards := cfg.DataShards
		// parityShards is the number of parity shards to compute per stripe.
		parityShards := cfg.ParityShards
		// ackThreshold is the minimum number of shards that must be durably written to storage
		// nodes before a stripe is considered durable. Must be >= dataShards and <= dataShards + parityShards.
		ackThreshold := dataShards
		// queueDepth is the maximum number of shards that may be in a stream writer's queue at a
		// time, after which senders block.
		queueDepth := 3
		// maxFrameSize is the maximum size (in bytes) of a single encrypted unit, including overhead bytes.
		maxFrameSize := 64 * 1024
		// maxFramesPerShard is the number of frames that make up each data shard of a stripe. The
		// shards of the last stripe of the object may be shorter in order to keep shards equal in
		// size.
		maxFramesPerShard := 1

		totalShards := dataShards + parityShards
		maxShardSize := maxFrameSize * maxFramesPerShard
		maxStripeSize := maxShardSize * dataShards

		// Compute object eTag.
		// TODO: Decide whether this should be an MD5 of the body bytes, or something random.
		eTag := model.ObjectHash(bucket, key)

		// Use eTag to compute the placement preference list from the hash ring.
		nodes, err := ring.Nodes(eTag, totalShards)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}

		// Initialise RS encoder.
		rs, err := reedsolomon.New(dataShards, parityShards)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}

		body, size := decodeBody(r)

		// Pre-allocate stripe buffers to eliminate copies between stages.
		free := make(chan [][]byte, queueDepth)
		for range queueDepth {
			free <- reedsolomon.AllocAligned(totalShards, maxShardSize)
		}

		read := 0
		for int64(read) < size {
			// Wait for a free stripe buffer.
			var stripe [][]byte
			select {
			case stripe = <-free:
			case <-ctx.Done():
				return
			}

			// Zero the parity shards.
			for _, shard := range stripe[dataShards:] {
				clear(shard)
			}

			// Fill data shards with object plaintext.
		read:
			for i, shard := range stripe[:dataShards] {
				for j := range maxFramesPerShard {
					// Read into the current data shard, leaving space for encryption overhead.
					plaintextStart := j * maxFrameSize
					plaintextEnd := plaintextStart + maxFrameSize - 16 // TODO: Dynamically size encryption overhead.
					n, err := io.ReadFull(body, shard[plaintextStart:plaintextEnd])
					read += n

					switch {
					case err == nil:
						continue
					case errors.Is(err, io.ErrUnexpectedEOF), i == 0:
						// If the first shard is short, slice all shards to the same length to minimize padding.
						for i, shard := range stripe {
							stripe[i] = shard[:plaintextStart+n+16] // TODO: Dynamically size encryption overhead.
						}
						fallthrough
					case errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, io.EOF):
						// Zero the remainder of the current shard.
						clear(shard[plaintextStart+n:])
						// Zero any unused data shards in the stripe.
						for _, shard := range stripe[i+1 : dataShards] {
							clear(shard)
						}
						break read
					default:
						HandleError(w, r, model.NewS3Error(model.ErrInternalError, fmt.Errorf("read plaintext frame: %w", err).Error(), 500))
						return
					}
				}
			}

			// TODO: Seal plaintext frames.

			// Compute parity shards.
			// TODO: Skip encoding parity for the empty shards in the tail stripe.
			for i, shard := range stripe[:dataShards] {
				if err := rs.EncodeIdx(shard, i, stripe[dataShards:]); err != nil {
					HandleError(w, r, model.NewS3Error(model.ErrInternalError, fmt.Errorf("compute parity shards: %w", err).Error(), 500))
					return
				}
			}

			res := make(chan error)
			for _, shard := range stripe {
				go func() {

				}()
			}
		}
	})
}

// decodeBody unwraps aws-chunked framing when the client used it, so the rest
// of the write path only ever sees object bytes, and reports how many of those
// bytes to expect. The count is negative when the request declared no length.
func decodeBody(r *http.Request) (io.Reader, int64) {
	if r.Body == nil {
		return http.NoBody, 0
	}
	if r.Header.Get("Content-Encoding") != "aws-chunked" {
		return r.Body, r.ContentLength
	}
	// Content-Length on a chunked request measures the framing, not the object,
	// so the decoded length is the only size the splitter can use. An absent or
	// unparseable header leaves the object size undeclared.
	decodedLen, err := strconv.ParseInt(r.Header.Get("X-Amz-Decoded-Content-Length"), 10, 64)
	if err != nil || decodedLen < 0 {
		return chunked.NewDecoder(r.Body, 0), -1
	}
	return chunked.NewDecoder(r.Body, decodedLen), decodedLen
}
