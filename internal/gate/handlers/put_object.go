package handlers

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
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

		body, size, dec := decodeBody(r)
		if size < 0 {
			HandleError(w, r, model.ErrMissingContentLengthError)
			return
		}

		poolNearFull, err := writeObject(ctx, bc, ring, cfg, body, size, objectHash)
		recordPhase(ctx, telemetry.GateOpPut, telemetry.PhaseShardFanout, phase)
		if err != nil {
			slog.ErrorContext(ctx, "putObject: shard distribution failed", "error", err)
			telemetry.RecordObjectWrite(ctx, telemetry.WriteOutcomeFailed, writeFailureReason(err))
			HandleError(w, r, mapPutErr(err))
			return
		}

		// Shards are written but nothing references them until the placement lands
		// below, so a body that fails its payload check leaves no readable object.
		if err := finishPayload(r, dec); err != nil {
			HandleError(w, r, err)
			return
		}

		place, err := placeShards(ring, cfg, objectHash, size)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}

		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(place); err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}

		// Object hash -> shard placement, for retrieval.
		phase = time.Now()
		if err := metaPut(ctx, mc, model.TableObjects, string(objectHash[:]), buf.Bytes()); err != nil {
			telemetry.RecordObjectWrite(ctx, telemetry.WriteOutcomeFailed, telemetry.WriteReasonMeta)
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}
		phase = recordPhase(ctx, telemetry.GateOpPut, telemetry.PhaseMetaPlacement, phase)

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
		if poolNearFull {
			w.Header().Set("X-Predastore-Pool-Pressure", "nearfull")
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
// It also finishes a framed body, where the remainder is the terminating chunk
// and the trailers: the write path stops at the decoded length, so without this
// the chunk signature closing the chain and the trailing checksum are never
// read. Draining has to go through the decoder for that reason — draining
// r.Body would read those bytes around every check the decoder makes on them.
func finishPayload(r *http.Request, dec *chunked.Decoder) error {
	if r.Body == nil {
		return nil
	}

	var rest io.Reader = r.Body
	if dec != nil {
		rest = dec
	}

	if _, err := io.Copy(io.Discard, rest); err != nil {
		if errors.Is(err, sigv4.ErrContentSHA256Mismatch) {
			return model.ErrContentSHA256MismatchError
		}

		return mapChunkedErr(err)
	}

	// Only crc64nvme is understood, so a client checksumming with anything else
	// is left to the chunk signatures and the transport. Verifying what we do
	// understand still covers the aws-cli default path.
	if dec != nil {
		if _, ok := dec.TrailerChecksum(); ok {
			if err := dec.VerifyTrailerChecksum(); err != nil {
				return model.ErrChecksumMismatchError
			}
		}
	}

	return nil
}

// mapChunkedErr turns a framing failure into the response S3 gives for it. A
// broken signature chain is an authentication failure; anything else about the
// framing is a malformed request. Neither is a 500: both are the client's.
func mapChunkedErr(err error) error {
	switch {
	case errors.Is(err, chunked.ErrChunkSignature):
		return model.ErrSignatureDoesNotMatchError
	case errors.Is(err, chunked.ErrMalformedFraming):
		return model.ErrMalformedChunkedBodyError
	default:
		return model.NewS3Error(model.ErrInternalError, "Failed to read the request body", 500)
	}
}

// decodeBody unwraps aws-chunked framing when the client used it, so the rest
// of the write path only ever sees object bytes, and reports how many of those
// bytes to expect. The count is negative when the request declared no length.
// The decoder comes back with them so the caller can finish the body through
// it, and is nil when the body carries no framing.
func decodeBody(r *http.Request) (io.Reader, int64, *chunked.Decoder) {
	if r.Body == nil {
		return http.NoBody, 0, nil
	}
	if !bodyIsFramed(r) {
		return r.Body, r.ContentLength, nil
	}

	var opts []chunked.Option
	if chain := SignedPayloadFrom(r.Context()).Chain; chain != nil {
		opts = append(opts, chunked.WithChain(chain))
	}

	// Content-Length on a chunked request measures the framing, not the object,
	// so the decoded length is the only size the splitter can use. An absent or
	// unparseable header leaves the object size undeclared.
	decodedLen, err := strconv.ParseInt(r.Header.Get("X-Amz-Decoded-Content-Length"), 10, 64)
	if err != nil || decodedLen < 0 {
		dec := chunked.NewDecoder(r.Body, 0, opts...)
		return dec, -1, dec
	}
	dec := chunked.NewDecoder(r.Body, decodedLen, opts...)
	return dec, decodedLen, dec
}

// bodyIsFramed reports whether the body carries aws-chunked framing.
//
// The sentinel the client signed decides it, not Content-Encoding: AWS
// documents that header as optional on a chunked upload and permits
// "aws-chunked, gzip", and it is not a signed header, so anything on the path
// can add or remove it. Getting this wrong stores the framing as object data
// and answers 200.
//
// An unauthenticated request signs nothing, so there the header is all there is.
func bodyIsFramed(r *http.Request) bool {
	payload := SignedPayloadFrom(r.Context())
	if payload.Signed {
		return payload.Framed()
	}
	for enc := range strings.SplitSeq(r.Header.Get("Content-Encoding"), ",") {
		if strings.EqualFold(strings.TrimSpace(enc), "aws-chunked") {
			return true
		}
	}
	return false
}
