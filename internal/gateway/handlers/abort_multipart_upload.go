package handlers

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/mulgadc/predastore/internal/gateway/model"
	"github.com/mulgadc/predastore/internal/storage"
)

// AbortMultipartUpload serves DELETE /{bucket}/{key}?uploadId=X: it discards an
// upload's parts and metadata. The object itself is untouched — an abort of an
// overwrite must leave the existing object in place.
func AbortMultipartUpload(st Store, shards *storage.Client, cache *BucketCache) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		bucket := chi.URLParam(r, "bucket")
		key := chi.URLParam(r, "*")
		uploadID := r.URL.Query().Get("uploadId")

		if bucket == "" {
			HandleError(w, r, model.ErrNoSuchBucketError.WithResource(bucket))
			return
		}
		if key == "" {
			HandleError(w, r, model.ErrNoSuchKeyError.WithResource(key))
			return
		}
		if err := requireBucket(st, cache, bucket); err != nil {
			HandleError(w, r, err)
			return
		}
		if err := requireUpload(st, bucket, key, uploadID); err != nil {
			HandleError(w, r, err)
			return
		}

		// Parts that cannot be listed cannot be cleaned up, but the upload must
		// still be closed out, so an unreadable part index degrades to dropping
		// the upload metadata alone.
		storedParts, err := getStoredParts(st, uploadID)
		if err != nil {
			slog.WarnContext(ctx, "Failed to get stored parts for cleanup", "uploadID", uploadID, "error", err)
			storedParts = nil
		}
		parts := make([]model.CompletedPart, len(storedParts))
		for i, p := range storedParts {
			parts[i] = model.CompletedPart{PartNumber: p.PartNumber, ETag: p.ETag}
		}

		if err := cleanupMultipartUpload(ctx, st, shards, bucket, key, uploadID, parts); err != nil {
			slog.WarnContext(ctx, "Failed to cleanup multipart upload", "uploadID", uploadID, "error", err)
		}

		slog.DebugContext(ctx, "Multipart upload aborted", "bucket", bucket, "key", key, "uploadID", uploadID)

		w.WriteHeader(http.StatusNoContent)
	})
}
