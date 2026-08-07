package s3

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/mulgadc/predastore/internal/gateway/model"
)

// abortMultipartUpload serves DELETE /{bucket}/{key}?uploadId=X: it discards an
// upload's parts and metadata. The object itself is untouched — an abort of an
// overwrite must leave the existing object in place.
func (s *HTTP2Server) abortMultipartUpload(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucket := chi.URLParam(r, "bucket")
	key := chi.URLParam(r, "*")
	uploadID := r.URL.Query().Get("uploadId")

	if bucket == "" {
		s.handleError(w, r, model.ErrNoSuchBucketError.WithResource(bucket))
		return
	}
	if key == "" {
		s.handleError(w, r, model.ErrNoSuchKeyError.WithResource(key))
		return
	}
	if err := s.requireBucket(bucket); err != nil {
		s.handleError(w, r, err)
		return
	}
	if err := s.requireUpload(bucket, key, uploadID); err != nil {
		s.handleError(w, r, err)
		return
	}

	// Parts that cannot be listed cannot be cleaned up, but the upload must
	// still be closed out, so an unreadable part index degrades to dropping
	// the upload metadata alone.
	storedParts, err := s.getStoredParts(uploadID)
	if err != nil {
		slog.WarnContext(ctx, "Failed to get stored parts for cleanup", "uploadID", uploadID, "error", err)
		storedParts = nil
	}
	parts := make([]model.CompletedPart, len(storedParts))
	for i, p := range storedParts {
		parts[i] = model.CompletedPart{PartNumber: p.PartNumber, ETag: p.ETag}
	}

	if err := s.cleanupMultipartUpload(ctx, bucket, key, uploadID, parts); err != nil {
		slog.WarnContext(ctx, "Failed to cleanup multipart upload", "uploadID", uploadID, "error", err)
	}

	slog.DebugContext(ctx, "Multipart upload aborted", "bucket", bucket, "key", key, "uploadID", uploadID)

	w.WriteHeader(http.StatusNoContent)
}
