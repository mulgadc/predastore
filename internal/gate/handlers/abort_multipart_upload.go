package handlers

import (
	"log/slog"
	"net/http"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/telemetry"
)

// AbortMultipartUpload serves DELETE /{bucket}/{key}?uploadId=X: it discards an
// upload's parts and metadata. The object itself is untouched — an abort of an
// overwrite must leave the existing object in place.
func AbortMultipartUpload(mc MetaClient, bc BlobClient, cache *BucketCache) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		resource, ok := routedObject(w, r)
		if !ok {
			return
		}
		bucket, key := resource.Bucket.Name, resource.Key
		uploadID := r.URL.Query().Get("uploadId")

		if err := requireBucket(ctx, mc, cache, bucket); err != nil {
			HandleError(w, r, err)
			return
		}
		if err := requireUpload(ctx, mc, bucket, key, uploadID); err != nil {
			HandleError(w, r, err)
			return
		}

		// Parts that cannot be listed cannot be cleaned up, but the upload must
		// still be closed out, so an unreadable part index degrades to dropping
		// the upload metadata alone.
		storedParts, err := getStoredParts(ctx, mc, uploadID)
		if err != nil {
			slog.WarnContext(ctx, "Failed to get stored parts for cleanup", "uploadID", uploadID, "error", err)
			storedParts = nil
		}
		parts := make([]model.CompletedPart, len(storedParts))
		for i, p := range storedParts {
			parts[i] = model.CompletedPart{PartNumber: p.PartNumber, ETag: p.ETag}
		}

		if err := cleanupMultipartUpload(ctx, mc, bc, bucket, key, uploadID, parts); err != nil {
			slog.WarnContext(ctx, "Failed to cleanup multipart upload", "uploadID", uploadID, "error", err)
		}

		telemetry.RecordMultipartUpload(ctx, telemetry.UploadAborted)
		slog.DebugContext(ctx, "Multipart upload aborted", "bucket", bucket, "key", key, "uploadID", uploadID)

		w.WriteHeader(http.StatusNoContent)
	})
}
