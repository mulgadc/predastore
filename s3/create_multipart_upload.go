package s3

import (
	"bytes"
	"encoding/gob"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/mulgadc/predastore/internal/gateway/model"
)

// createMultipartUpload serves POST /{bucket}/{key} without an uploadId: it
// registers an upload the client then sends parts against.
func (s *HTTP2Server) createMultipartUpload(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucket := chi.URLParam(r, "bucket")
	key := chi.URLParam(r, "*")

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

	uploadID := uuid.New().String()
	metadata := model.UploadMetadata{
		UploadID:    uploadID,
		Bucket:      bucket,
		Key:         key,
		ContentType: r.Header.Get("Content-Type"),
		CreatedAt:   time.Now(),
		Parts:       []model.PartMetadata{},
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(metadata); err != nil {
		s.handleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to encode upload metadata", 500))
		return
	}

	if err := s.statePut(model.TableMultipart, uploadID, buf.Bytes()); err != nil {
		slog.ErrorContext(ctx, "Failed to store multipart upload metadata", "uploadID", uploadID, "error", err)
		s.handleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to create multipart upload", 500))
		return
	}

	slog.DebugContext(ctx, "Multipart upload created", "bucket", bucket, "key", key, "uploadID", uploadID)

	w.Header().Set("X-Amz-Server-Side-Encryption", "AES256")
	if err := s.writeXML(w, http.StatusOK, InitiateMultipartUploadResult{
		Bucket:   bucket,
		Key:      key,
		UploadId: uploadID,
	}); err != nil {
		slog.DebugContext(ctx, "failed to write XML response", "error", err)
	}
}
