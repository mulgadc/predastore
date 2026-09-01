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

		// Versioning is not implemented anywhere in this store, so a request
		// naming a specific source version cannot be honoured: copying the
		// current version instead would silently ignore what was asked for.
		if versionID != "" && versionID != "null" {
			WriteS3Error(w, r, http.StatusNotImplemented, "NotImplemented",
				"Copying a specific object version is not implemented")
			return
		}
		for _, h := range copySourceConditionHeaders {
			if r.Header.Get(h) != "" {
				WriteS3Error(w, r, http.StatusNotImplemented, "NotImplemented",
					"Conditional CopyObject requests are not implemented")
				return
			}
		}
		// A copy to itself changes nothing unless the caller is explicitly
		// replacing metadata, storage class or similar, none of which this
		// store tracks per object. Real S3 refuses this rather than silently
		// answering 200 for a no-op.
		if srcBucket == destBucket && srcKey == destKey &&
			!strings.EqualFold(r.Header.Get("X-Amz-Metadata-Directive"), "REPLACE") {
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

		srcPlace, srcSize, err := loadPlacement(ctx, mc, ring, cfg, srcBucket, srcKey)
		if err != nil {
			HandleError(w, r, model.ErrNoSuchKeyError.WithResource(srcKey))
			return
		}

		srcHandoff := handoffNode(ring, cfg, model.ObjectHash(srcBucket, srcKey))

		destHash := model.ObjectHash(destBucket, destKey)
		place, err := placeShards(ring, cfg, destHash, srcSize)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}

		// The source is read stripe by stripe into a pipe and teed into the
		// digest, the same shape GetObject streams a response body: the
		// copy's memory footprint is a stripe, not the source object, no
		// matter how large that object is.
		digest := model.NewPartETagHasher()
		var written writeResult
		if srcSize == 0 {
			// An empty object has no shards to read back, only a placement
			// record, so there is nothing to stream and no stripe reader to open.
			written, err = writeObject(ctx, bc, cfg, ring, io.TeeReader(http.NoBody, digest), 0, destHash, place)
		} else {
			srcReader, rErr := newStripeReader(ctx, bc, cfg, model.ObjectHash(srcBucket, srcKey), srcPlace, srcHandoff)
			if rErr != nil {
				HandleError(w, r, model.NewS3Error(model.ErrInternalError, rErr.Error(), 500))
				return
			}

			pr, pw := io.Pipe()
			done := make(chan struct{})
			go func() {
				defer close(done)
				pw.CloseWithError(pipeObject(ctx, srcReader, pw, srcSize))
			}()

			written, err = writeObject(ctx, bc, cfg, ring, io.TeeReader(pr, digest), srcSize, destHash, place)

			// Closing the read end unblocks the goroutine if writeObject stopped
			// short, and waiting for it means the stripe reader is closed by
			// nobody else while it is still being read.
			_ = pr.Close()
			<-done
			srcReader.close(ctx)
		}
		if err != nil {
			slog.ErrorContext(ctx, "copyObject: shard distribution failed", "error", err)
			telemetry.RecordObjectWrite(ctx, telemetry.WriteOutcomeFailed, writeFailureReason(err))
			abortShards(ctx, bc, destHash, place, written)
			HandleError(w, r, mapPutErr(err))
			return
		}

		place.Digest = digest.Sum(nil)

		record, err := EncodePlacement(place)
		if err != nil {
			abortShards(ctx, bc, destHash, place, written)
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}
		if err := metaPut(ctx, mc, model.TableObjects, string(destHash[:]), record); err != nil {
			telemetry.RecordObjectWrite(ctx, telemetry.WriteOutcomeFailed, telemetry.WriteReasonMeta)
			abortShards(ctx, bc, destHash, place, written)
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}
		commitShards(ctx, bc, destHash, place, written)

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

		// The digest was set above, so the record always has an ETag to give.
		etag, _ := place.ETag()
		if err := writeXML(w, http.StatusOK,
			CopyObjectResult{ETag: etag, LastModified: place.ModifiedAt()}); err != nil {
			slog.DebugContext(ctx, "failed to write XML response", "error", err)
		}
	})
}

// parseCopySource splits and URL-decodes an x-amz-copy-source header into the
// bucket and key it names, and the version id if one was given. S3 accepts
// both a leading slash and a bare bucket/key form, and clients percent-encode
// the key the same way they would for the request path itself.
func parseCopySource(raw string) (bucket, key, versionID string, err error) {
	if raw == "" {
		return "", "", "", model.NewS3Error(model.ErrInvalidArgument, "x-amz-copy-source header is required", 400)
	}

	decoded, decErr := url.PathUnescape(raw)
	if decErr != nil {
		return "", "", "", model.NewS3Error(model.ErrInvalidArgument, "x-amz-copy-source is not valid URL encoding", 400)
	}

	if idx := strings.IndexByte(decoded, '?'); idx >= 0 {
		versionID = strings.TrimPrefix(decoded[idx:], "?versionId=")
		decoded = decoded[:idx]
	}

	decoded = strings.TrimPrefix(decoded, "/")
	bucket, key, ok := strings.Cut(decoded, "/")
	if !ok || bucket == "" || key == "" {
		return "", "", "", model.NewS3Error(model.ErrInvalidArgument,
			"x-amz-copy-source must be of the form /bucket/key", 400)
	}

	return bucket, key, versionID, nil
}
