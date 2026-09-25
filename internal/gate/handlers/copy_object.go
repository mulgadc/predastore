package handlers

import (
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/gate/placement"
	"github.com/mulgadc/predastore/internal/telemetry"
)

// copySourceConditionHeaders are the S3 conditional-copy headers this gate
// does not evaluate. Ignoring one silently would answer 200 to a client that
// asked for a precondition to gate the copy, so a request carrying any of
// them is refused instead.
var copySourceConditionHeaders = []string{
	"X-Amz-Copy-Source-If-Match",
	"X-Amz-Copy-Source-If-None-Match",
	"X-Amz-Copy-Source-If-Modified-Since",
	"X-Amz-Copy-Source-If-Unmodified-Since",
}

// CopyObject serves PUT /{bucket}/{key} carrying x-amz-copy-source. The
// source is streamed stripe by stripe into the same streaming write
// PutObject uses, so the destination gets its own placement record, its own
// epoch and its own content digest rather than a copy of the source's.
func CopyObject(mc MetaClient, bc BlobClient, ring *placement.Ring, cache *BucketCache, cfg Config) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		resource, ok := routedObject(w, r)
		if !ok {
			return
		}
		destBucket, destKey := resource.Bucket.Name, resource.Key

		srcBucket, srcKey, versionID, err := parseCopySource(r.Header.Get("X-Amz-Copy-Source"))
		if err != nil {
			HandleError(w, r, err)
			return
		}
		if err := (model.Object{Bucket: model.Bucket{Name: srcBucket}, Key: srcKey}).Validate(); err != nil {
			HandleError(w, r, err)
			return
		}

		for _, h := range copySourceConditionHeaders {
			if r.Header.Get(h) != "" {
				WriteS3Error(w, r, http.StatusNotImplemented, "NotImplemented",
					"Conditional CopyObject requests are not implemented")
				return
			}
		}
		// COPY, the default, keeps the source's attributes; REPLACE takes the
		// request's. Anything else is refused rather than read as either.
		directive := r.Header.Get("X-Amz-Metadata-Directive")
		replace := strings.EqualFold(directive, "REPLACE")
		if directive != "" && !replace && !strings.EqualFold(directive, "COPY") {
			HandleError(w, r, model.NewS3Error(model.ErrInvalidArgument, "Unknown metadata directive.", 400))
			return
		}
		var attrs ObjectAttributes
		if replace {
			if attrs, err = attributesFromRequest(r.Header); err != nil {
				HandleError(w, r, err)
				return
			}
		}

		// A copy to itself changes nothing unless the caller is replacing its
		// metadata. Real S3 refuses this rather than silently answering 200
		// for a no-op.
		if srcBucket == destBucket && srcKey == destKey && !replace {
			WriteS3Error(w, r, http.StatusBadRequest, "InvalidRequest",
				"This copy request is illegal because it is trying to copy an object to itself "+
					"without changing the object's metadata, storage class, website redirect "+
					"location or encryption attributes.")
			return
		}

		if err := requireBucket(ctx, mc, cache, srcBucket); err != nil {
			HandleError(w, r, err)
			return
		}
		if err := requireBucket(ctx, mc, cache, destBucket); err != nil {
			HandleError(w, r, err)
			return
		}

		// x-amz-copy-source carries its own versionId, so a copy can name a
		// version of the source rather than only its current one.
		srcTarget, err := resolveReadTarget(ctx, mc, cache, srcBucket, srcKey, versionID)
		if err != nil {
			handleVersionedReadErr(w, r, srcKey, srcTarget, err)
			return
		}

		srcPlace, srcSize, err := loadPlacementByHash(ctx, mc, ring, cfg, srcTarget.hash)
		if err != nil {
			HandleError(w, r, model.ErrNoSuchKeyError.WithResource(srcKey))
			return
		}

		srcHandoff := handoffNode(ring, cfg, srcTarget.hash)
		if !replace {
			attrs = srcPlace.Attributes
		}

		destTarget, err := resolveWriteTarget(ctx, mc, cache, destBucket, destKey)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}
		destHash := destTarget.hash

		place, err := placeShards(ring, cfg, destHash, srcSize)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}

		// The source is streamed and teed into the digest, so the destination
		// gets its own content digest and the copy's memory footprint is a
		// stripe rather than the source object.
		digest := model.NewPartETagHasher()
		stream, err := openCopyStream(ctx, bc, cfg,
			srcTarget.hash, srcPlace, srcHandoff, srcSize, 0, srcSize)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}

		written, err := writeObject(ctx, bc, cfg, ring, io.TeeReader(stream, digest), srcSize, destHash, place)
		stream.close(ctx)
		if err != nil {
			slog.ErrorContext(ctx, "copyObject: shard distribution failed", "error", err)
			telemetry.RecordObjectWrite(ctx, telemetry.WriteOutcomeFailed, writeFailureReason(err))
			abortShards(ctx, bc, destHash, place, written)
			HandleError(w, r, mapPutErr(err))
			return
		}

		place.Digest = digest.Sum(nil)
		place.Attributes = attrs

		record, err := EncodePlacement(place)
		if err != nil {
			abortShards(ctx, bc, destHash, place, written)
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}
		previous, err := metaSwap(ctx, mc, model.TableObjects, string(destHash[:]), record)
		if err != nil {
			telemetry.RecordObjectWrite(ctx, telemetry.WriteOutcomeFailed, telemetry.WriteReasonMeta)
			abortShards(ctx, bc, destHash, place, written)
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}
		commitShards(ctx, bc, destHash, place, written)
		if !destTarget.versioned {
			releaseSuperseded(ctx, bc, destHash, previous, place.WriteEpoch)
		}

		if err := indexWrite(ctx, mc, destBucket, destKey, destTarget, place); err != nil {
			telemetry.RecordObjectWrite(ctx, telemetry.WriteOutcomeFailed, telemetry.WriteReasonMeta)
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}

		if err := metaPut(ctx, mc, model.TableObjects, objectARN(destBucket, destKey), destHash[:]); err != nil {
			telemetry.RecordObjectWrite(ctx, telemetry.WriteOutcomeFailed, telemetry.WriteReasonMeta)
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}
		telemetry.RecordObjectWrite(ctx, telemetry.WriteOutcomeSuccess, "")

		if written.poolNearFull {
			w.Header().Set("X-Predastore-Pool-Pressure", "nearfull")
		}
		if written.degraded() {
			w.Header().Set(degradedWriteHeader, strconv.Itoa(len(written.missing)))
		}
		if len(written.handoff) > 0 {
			w.Header().Set(handoffHeader, strconv.Itoa(len(written.handoff)))
		}
		setVersionIDHeader(w.Header(), destTarget.versionID)

		etag, _ := place.ETag()
		modified, _ := place.ModifiedAt()
		if err := writeXML(w, http.StatusOK, CopyObjectResult{ETag: etag, LastModified: modified}); err != nil {
			slog.DebugContext(ctx, "failed to write XML response", "error", err)
		}
	})
}

// parseCopySource splits and URL-decodes an x-amz-copy-source header into the
// bucket and key it names, and the version id if one was given. The query is
// cut off before decoding, so an encoded ? in the key stays part of the key.
func parseCopySource(raw string) (bucket, key, versionID string, err error) {
	if raw == "" {
		return "", "", "", model.NewS3Error(model.ErrInvalidArgument, "x-amz-copy-source header is required", 400)
	}

	path, rawQuery, _ := strings.Cut(raw, "?")
	decoded, decErr := url.PathUnescape(path)
	query, queryErr := url.ParseQuery(rawQuery)
	if decErr != nil || queryErr != nil {
		return "", "", "", model.NewS3Error(model.ErrInvalidArgument, "x-amz-copy-source is not valid URL encoding", 400)
	}
	versionID = query.Get("versionId")

	decoded = strings.TrimPrefix(decoded, "/")
	bucket, key, ok := strings.Cut(decoded, "/")
	if !ok || bucket == "" || key == "" {
		return "", "", "", model.NewS3Error(model.ErrInvalidArgument,
			"x-amz-copy-source must be of the form /bucket/key", 400)
	}

	return bucket, key, versionID, nil
}
