package handlers

import (
	"context"
	"errors"
	"fmt"
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

		// Placement, and the write epoch every shard is stamped with, are fixed
		// before the first byte moves: the record the read path will dial has to
		// be the same list the shards were written to.
		place, err := placeShards(ring, cfg, objectHash, size)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}

		// The ETag is MD5 over the body, teed off the read the write path
		// already performs rather than a second pass over a buffered copy.
		digest := model.NewPartETagHasher()

		written, err := writeObject(ctx, bc, cfg, ring, io.TeeReader(body, digest), size, objectHash, place)
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
		if err := finishPayload(r, dec); err != nil {
			abortShards(ctx, bc, objectHash, place, written)
			HandleError(w, r, err)
			return
		}

		// The record must already carry the digest here: after the commit
		// point below, it is what every read will demand.
		place.Digest = digest.Sum(nil)
		place.DigestPresent = true

		record, err := EncodePlacement(place)
		if err != nil {
			abortShards(ctx, bc, objectHash, place, written)
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}

		// Publish shards before the placement record, so the record can never
		// name a generation the shards have not reached. It may name one they
		// have already passed, and the blob store retains superseded
		// generations for exactly that reason.
		if overtaken := commitShards(ctx, bc, objectHash, place, written); overtaken > 0 {
			slog.InfoContext(ctx, "A newer write overtook this one",
				"shards_superseded", overtaken,
				"epoch", fmt.Sprintf("%016x", place.WriteEpoch))
		}

		// Object hash -> shard placement is the visibility point. Raft applies
		// this max-epoch update atomically, so a delayed older writer cannot move
		// the record backwards.
		phase = time.Now()
		if err := metaPutMax(ctx, mc, model.TableObjects, string(objectHash[:]), record, place.WriteEpoch); err != nil {
			telemetry.RecordObjectWrite(ctx, telemetry.WriteOutcomeFailed, telemetry.WriteReasonMeta)
			abortShards(ctx, bc, objectHash, place, written)
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
		if etag, ok := place.ETag(); ok {
			w.Header().Set("ETag", etag)
		}
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

	if dec == nil {
		return nil
	}

	// Two independent promises: the sentinel the client signed, and X-Amz-Trailer
	// on the request. The header binds an unauthenticated write, which signs no
	// sentinel, so neither alone is enough to decide the trailer was optional.
	_, _, sent := dec.ChecksumTrailer()
	promised := dec.PromisesChecksum() || SignedPayloadFrom(r.Context()).PromisesTrailer()
	if !sent && !promised {
		return nil
	}
	if err := dec.VerifyTrailerChecksum(); err != nil {
		return mapChecksumErr(err)
	}

	return nil
}

// mapChecksumErr separates a body that failed its checksum from one that never
// supplied a usable one. The first is a mismatch; the second is a request whose
// integrity promise cannot be honoured, which is malformed.
func mapChecksumErr(err error) error {
	switch {
	case errors.Is(err, chunked.ErrChecksumMissing),
		errors.Is(err, chunked.ErrChecksumUndeclared):
		return model.ErrMalformedChunkedBodyError
	default:
		return model.ErrChecksumMismatchError
	}
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
	// X-Amz-Trailer names the trailers to come, and hashing has to start before
	// the body is read, so the promise is what selects the algorithm.
	if promised := r.Header.Values("X-Amz-Trailer"); len(promised) > 0 {
		opts = append(opts, chunked.WithTrailerChecksums(splitHeaderList(promised)))
	}

	// Content-Length on a chunked request measures the framing, not the object,
	// so the decoded length is the only size the splitter can use. An absent or
	// unparseable header leaves the object size undeclared.
	decodedLen, err := strconv.ParseInt(r.Header.Get("X-Amz-Decoded-Content-Length"), 10, 64)
	if err != nil || decodedLen < 0 {
		dec := chunked.NewDecoder(r.Body, 0, opts...)
		return dec, -1, dec
	}
	// The write path stops at the declared length, so the decoder has to hold
	// the body to it as well; otherwise a short declaration stores a truncated
	// object whose checksum still verifies over everything that was sent.
	opts = append(opts, chunked.WithDeclaredLength(decodedLen))
	dec := chunked.NewDecoder(r.Body, decodedLen, opts...)
	return dec, decodedLen, dec
}

// splitHeaderList flattens a header that may repeat and may also carry a
// comma-separated list in one value.
func splitHeaderList(values []string) []string {
	var out []string
	for _, v := range values {
		for part := range strings.SplitSeq(v, ",") {
			if part = strings.TrimSpace(part); part != "" {
				out = append(out, part)
			}
		}
	}
	return out
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
