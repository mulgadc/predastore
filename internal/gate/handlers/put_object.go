package handlers

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/mulgadc/bluebottle/pkg/sigv4"
	"github.com/mulgadc/predastore/internal/gate/chunked"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/gate/placement"
	"github.com/mulgadc/predastore/internal/telemetry"
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

		phase := time.Now()
		if err := requireBucket(ctx, mc, cache, bucket); err != nil {
			HandleError(w, r, err)
			return
		}
		phase = recordPhase(ctx, telemetry.GateOpPut, telemetry.PhaseBucketCheck, phase)

		objectHash := model.ObjectHash(bucket, key)

		body, size := decodeBody(r)
		if size < 0 {
			HandleError(w, r, model.ErrMissingContentLengthError)
			return
		}

		// Placement, and the write epoch every shard is stamped with, are fixed
		// before the first byte moves: the record the read path will dial has to
		// be the same list the shards were written to.
		place, err := placeShards(ring, cfg, objectHash, size)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}

		written, err := writeObject(ctx, bc, cfg, ring, body, size, objectHash, place)
		recordPhase(ctx, telemetry.GateOpPut, telemetry.PhaseShardFanout, phase)
		if err != nil {
			slog.ErrorContext(ctx, "putObject: shard distribution failed", "error", err)
			telemetry.RecordObjectWrite(ctx, telemetry.WriteOutcomeFailed, writeFailureReason(err))
			abortShards(ctx, bc, objectHash, place, written)
			HandleError(w, r, mapPutErr(err))
			return
		}

		// Shards are prepared but invisible until the placement lands below, so a
		// body that fails its payload check leaves the previous object intact.
		if err := finishPayload(r); err != nil {
			abortShards(ctx, bc, objectHash, place, written)
			HandleError(w, r, err)
			return
		}

		record, err := EncodePlacement(place)
		if err != nil {
			abortShards(ctx, bc, objectHash, place, written)
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}

		// Object hash -> shard placement, for retrieval. This is the commit
		// point: before it the write is invisible, after it the epoch it names
		// is what every read will demand, and the shards already carry it.
		phase = time.Now()
		if err := metaPut(ctx, mc, model.TableObjects, string(objectHash[:]), record); err != nil {
			telemetry.RecordObjectWrite(ctx, telemetry.WriteOutcomeFailed, telemetry.WriteReasonMeta)
			abortShards(ctx, bc, objectHash, place, written)
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}
		phase = recordPhase(ctx, telemetry.GateOpPut, telemetry.PhaseMetaPlacement, phase)

		commitShards(ctx, bc, objectHash, place, written)

		// Listing key -> object hash, for ListObjects.
		if err := metaPut(ctx, mc, model.TableObjects, objectARN(bucket, key), objectHash[:]); err != nil {
			telemetry.RecordObjectWrite(ctx, telemetry.WriteOutcomeFailed, telemetry.WriteReasonMeta)
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}
		recordPhase(ctx, telemetry.GateOpPut, telemetry.PhaseMetaListing, phase)

		// Counted only once both keys have landed: until then the shards exist
		// but nothing references them, which is not an object.
		telemetry.RecordObjectWrite(ctx, telemetry.WriteOutcomeSuccess, "")

		// Nearfull writes still succeed; the header lets clients back off before
		// hitting the hard 507 rejection.
		if written.poolNearFull {
			w.Header().Set("X-Predastore-Pool-Pressure", "nearfull")
		}
		// The write is durable and correct; what it is short of is redundancy,
		// until repair restores the shards that did not land. A caller writing
		// something it cannot reproduce deserves to know that.
		if written.degraded() {
			w.Header().Set(degradedWriteHeader, strconv.Itoa(len(written.missing)))
		}
		if len(written.handoff) > 0 {
			w.Header().Set(handoffHeader, strconv.Itoa(len(written.handoff)))
		}
		w.Header().Set("ETag", model.ObjectETag(bucket, key))
		w.WriteHeader(http.StatusOK)
	})
}

// recordPhase records the time since start as one phase of an object request
// and returns the instant the next phase begins, so chained phases share a
// single clock read at each boundary and cannot overlap each other.
func recordPhase(ctx context.Context, op, phase string, start time.Time) time.Time {
	now := time.Now()
	telemetry.RecordObjectPhase(ctx, op, phase, now.Sub(start).Seconds())
	return now
}

// finishPayload completes the SigV4 payload check on a body large enough that sigv4
// verifies it as it streams: the signed digest is only compared at EOF, and the write
// path stops at the declared length. Draining the remainder forces the comparison, so a
// rewritten body is caught before the write is committed to global state.
func finishPayload(r *http.Request) error {
	if r.Body == nil {
		return nil
	}

	if _, err := io.Copy(io.Discard, r.Body); err != nil {
		if errors.Is(err, sigv4.ErrContentSHA256Mismatch) {
			return model.ErrContentSHA256MismatchError
		}

		return model.NewS3Error(model.ErrInternalError, "Failed to read the request body", 500)
	}

	return nil
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
