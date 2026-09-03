package handlers

import (
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/gate/placement"
)

// UploadPartCopy serves PUT /{bucket}/{key}?partNumber=N&uploadId=X carrying
// x-amz-copy-source: a part whose content is a byte range of an object already
// stored, rather than a request body.
//
// The docker registry's S3 driver finishes every resumed blob upload this way,
// so without it a push of anything past one request fails.
func UploadPartCopy(mc MetaClient, bc BlobClient, ring *placement.Ring, cache *BucketCache, cfg Config) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		resource, ok := routedObject(w, r)
		if !ok {
			return
		}
		bucket, key := resource.Bucket.Name, resource.Key
		uploadID := r.URL.Query().Get("uploadId")
		partNumber, _ := strconv.Atoi(r.URL.Query().Get("partNumber"))

		srcBucket, srcKey, versionID, err := parseCopySource(r.Header.Get("X-Amz-Copy-Source"))
		if err != nil {
			HandleError(w, r, err)
			return
		}
		if err := (model.Object{Bucket: model.Bucket{Name: srcBucket}, Key: srcKey}).Validate(); err != nil {
			HandleError(w, r, err)
			return
		}

		// Same two refusals CopyObject makes, for the same reason: honouring
		// neither silently would answer 200 to a request whose terms were
		// ignored.
		if versionID != "" && versionID != "null" {
			WriteS3Error(w, r, http.StatusNotImplemented, "NotImplemented",
				"Copying a specific object version is not implemented")
			return
		}
		for _, h := range copySourceConditionHeaders {
			if r.Header.Get(h) != "" {
				WriteS3Error(w, r, http.StatusNotImplemented, "NotImplemented",
					"Conditional copy requests are not implemented")
				return
			}
		}

		if err := model.ValidatePartNumber(partNumber); err != nil {
			HandleError(w, r, err)
			return
		}
		if err := requireBucket(ctx, mc, cache, bucket); err != nil {
			HandleError(w, r, err)
			return
		}
		if err := requireBucket(ctx, mc, cache, srcBucket); err != nil {
			HandleError(w, r, err)
			return
		}
		if err := requireUpload(ctx, mc, bucket, key, uploadID); err != nil {
			HandleError(w, r, err)
			return
		}

		srcPlace, srcSize, err := loadPlacement(ctx, mc, ring, cfg, srcBucket, srcKey)
		if err != nil {
			HandleError(w, r, model.ErrNoSuchKeyError.WithResource(srcKey))
			return
		}

		start, end, err := copySourceRange(r.Header.Get("X-Amz-Copy-Source-Range"), srcSize)
		if err != nil {
			HandleError(w, r, err)
			return
		}
		partSize := end - start + 1
		if partSize > model.MaxPartSize {
			HandleError(w, r, model.NewS3Error(model.ErrEntityTooLarge, "Part exceeds maximum size", 400))
			return
		}

		stream, err := openCopyStream(ctx, bc, cfg,
			model.ObjectHash(srcBucket, srcKey), srcPlace,
			handoffNode(ring, cfg, model.ObjectHash(srcBucket, srcKey)),
			srcSize, start, partSize)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}

		// A copied part has no client payload to check, so no verify step: the
		// source is content this gate already accepted and digested.
		partMeta, written, err := storePart(ctx, mc, bc, ring, cfg,
			bucket, key, uploadID, partNumber, stream, partSize, nil)
		stream.close(ctx)
		if err != nil {
			HandleError(w, r, err)
			return
		}

		writePartHeaders(w, written)
		w.Header().Set("X-Amz-Server-Side-Encryption", "AES256")
		if err := writeXML(w, http.StatusOK,
			CopyPartResult{ETag: partMeta.ETag, LastModified: partMeta.LastModified}); err != nil {
			slog.DebugContext(ctx, "failed to write XML response", "error", err)
		}
	})
}

// copySourceRange resolves x-amz-copy-source-range against the source size. An
// absent header copies the whole object; S3 requires both ends when one is
// given, so a half-open or unparseable spec is refused rather than widened into
// a copy the caller did not ask for.
func copySourceRange(header string, size int64) (start, end int64, err error) {
	if header == "" {
		if size == 0 {
			return 0, -1, nil
		}
		return 0, size - 1, nil
	}

	start, end = parseRangeHeader(header)
	if !strings.HasPrefix(header, "bytes=") || start < 0 || end < 0 {
		return 0, 0, model.NewS3Error(model.ErrInvalidArgument,
			"The x-amz-copy-source-range value must be of the form bytes=first-last "+
				"where first and last are the zero-based offsets of the first and last bytes to copy", 400)
	}

	start, end, ok := resolveRange(size, start, end)
	if !ok {
		return 0, 0, model.ErrInvalidRangeError
	}

	return start, end, nil
}
