package handlers

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/gate/placement"
)

// UploadPart serves PUT /{bucket}/{key}?partNumber=N&uploadId=X. A part is
// stored as an object in its own right, under a hidden key, so completion is
// just a read-back and concatenation.
func UploadPart(mc MetaClient, bc BlobClient, ring *placement.Ring, cache *BucketCache, cfg Config) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		bucket := chi.URLParam(r, "bucket")
		key := chi.URLParam(r, "*")
		uploadID := r.URL.Query().Get("uploadId")
		partNumber, _ := strconv.Atoi(r.URL.Query().Get("partNumber"))

		if bucket == "" {
			HandleError(w, r, model.ErrNoSuchBucketError.WithResource(bucket))
			return
		}
		if key == "" {
			HandleError(w, r, model.ErrNoSuchKeyError.WithResource(key))
			return
		}
		if err := requireBucket(mc, cache, bucket); err != nil {
			HandleError(w, r, err)
			return
		}
		if err := model.ValidatePartNumber(partNumber); err != nil {
			HandleError(w, r, err)
			return
		}
		if err := requireUpload(mc, bucket, key, uploadID); err != nil {
			HandleError(w, r, err)
			return
		}

		etag, data, err := model.CalculatePartETagFromReader(decodeBody(r))
		if err != nil {
			slog.ErrorContext(ctx, "Failed to read part data", "uploadID", uploadID, "part", partNumber, "error", err)
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to read part data", 500))
			return
		}

		// The last part may be arbitrarily small, so only the upper bound is
		// enforceable here.
		partSize := int64(len(data))
		if partSize > model.MaxPartSize {
			HandleError(w, r, model.NewS3Error(model.ErrEntityTooLarge, "Part exceeds maximum size", 400))
			return
		}

		partKey := partObjectKey(key, uploadID, partNumber)
		objectHash := model.ObjectHash(bucket, partKey)

		// A deterministic temp path keeps a retried part on the same placement.
		tmpPath := filepath.Join(os.TempDir(), fmt.Sprintf("multipart-%s-%05d.tmp", uploadID, partNumber))
		tmpFile, err := os.Create(tmpPath)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to create temp file", 500))
			return
		}
		defer os.Remove(tmpPath)
		defer tmpFile.Close()

		if _, err := tmpFile.Write(data); err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to write temp file", 500))
			return
		}
		if closeErr := tmpFile.Close(); closeErr != nil {
			slog.DebugContext(ctx, "Failed to close temp file", "path", tmpPath, "error", closeErr)
		}

		if _, _, err := writeObject(ctx, bc, ring, cfg, tmpPath, objectHash); err != nil {
			slog.ErrorContext(ctx, "Failed to store part", "uploadID", uploadID, "part", partNumber, "error", err)
			HandleError(w, r, mapPutErr(err))
			return
		}

		partMeta := model.PartMetadata{
			PartNumber:   partNumber,
			Size:         partSize,
			ETag:         etag,
			LastModified: time.Now(),
		}
		var partBuf bytes.Buffer
		if err := gob.NewEncoder(&partBuf).Encode(partMeta); err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to encode part metadata", 500))
			return
		}
		if err := metaPut(mc, model.TableParts, multipartPartKey(uploadID, partNumber), partBuf.Bytes()); err != nil {
			slog.ErrorContext(ctx, "Failed to store part metadata", "uploadID", uploadID, "part", partNumber, "error", err)
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to store part metadata", 500))
			return
		}

		place, err := placeShards(ring, cfg, objectHash, partSize)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to get shard placement", 500))
			return
		}
		var shardBuf bytes.Buffer
		if err := gob.NewEncoder(&shardBuf).Encode(place); err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to encode shard metadata", 500))
			return
		}
		if err := metaPut(mc, model.TableObjects, partShardKey(uploadID, partNumber), shardBuf.Bytes()); err != nil {
			slog.ErrorContext(ctx, "Failed to store part shard metadata", "uploadID", uploadID, "part", partNumber, "error", err)
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to store part shard metadata", 500))
			return
		}

		slog.DebugContext(ctx, "Part uploaded", "uploadID", uploadID, "partNumber", partNumber, "size", partSize, "etag", etag)

		w.Header().Set("ETag", etag)
		w.Header().Set("X-Amz-Server-Side-Encryption", "AES256")
		w.WriteHeader(http.StatusOK)
	})
}
