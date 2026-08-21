package handlers

import (
	"bytes"
	"encoding/gob"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/telemetry"
)

// CreateMultipartUpload serves POST /{bucket}/{key} without an uploadId: it
// registers an upload the client then sends parts against.
func CreateMultipartUpload(mc MetaClient, cache *BucketCache) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		resource, ok := routedObject(w, r)
		if !ok {
			return
		}
		bucket, key := resource.Bucket.Name, resource.Key

		if err := requireBucket(ctx, mc, cache, bucket); err != nil {
			HandleError(w, r, err)
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
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to encode upload metadata", 500))
			return
		}

		if err := metaPut(ctx, mc, model.TableMultipart, uploadID, buf.Bytes()); err != nil {
			slog.ErrorContext(ctx, "Failed to store multipart upload metadata", "uploadID", uploadID, "error", err)
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to create multipart upload", 500))
			return
		}

		telemetry.RecordMultipartUpload(ctx, telemetry.UploadCreated)
		slog.DebugContext(ctx, "Multipart upload created", "bucket", bucket, "key", key, "uploadID", uploadID)

		w.Header().Set("X-Amz-Server-Side-Encryption", "AES256")
		if err := writeXML(w, http.StatusOK, InitiateMultipartUploadResult{
			Bucket:   bucket,
			Key:      key,
			UploadId: uploadID,
		}); err != nil {
			slog.DebugContext(ctx, "failed to write XML response", "error", err)
		}
	})
}
