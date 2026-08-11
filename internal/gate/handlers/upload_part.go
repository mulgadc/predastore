package handlers

import (
	"bytes"
	"encoding/gob"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/gate/placement"
)

// UploadPart serves PUT /{bucket}/{key}?partNumber=N&uploadId=X. A part is
// stored as an object in its own right, under a hidden key, so completion is
// just a read-back and concatenation.
func UploadPart(mc MetaClient, bc BlobClient, ring *placement.Ring, cache *BucketCache, cfg Config) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		resource, ok := routedObject(w, r)
		if !ok {
			return
		}
		bucket, key := resource.Bucket.Name, resource.Key
		uploadID := r.URL.Query().Get("uploadId")
		partNumber, _ := strconv.Atoi(r.URL.Query().Get("partNumber"))

		if err := requireBucket(ctx, mc, cache, bucket); err != nil {
			HandleError(w, r, err)
			return
		}
		if err := model.ValidatePartNumber(partNumber); err != nil {
			HandleError(w, r, err)
			return
		}
		if err := requireUpload(ctx, mc, bucket, key, uploadID); err != nil {
			HandleError(w, r, err)
			return
		}

		// The part is written as it arrives, so its length has to be known
		// before the body is read rather than measured from what was buffered.
		body, partSize := decodeBody(r)
		if partSize < 0 {
			HandleError(w, r, model.ErrMissingContentLengthError)
			return
		}
		// The last part may be arbitrarily small, so only the upper bound is
		// enforceable here.
		if partSize > model.MaxPartSize {
			HandleError(w, r, model.NewS3Error(model.ErrEntityTooLarge, "Part exceeds maximum size", 400))
			return
		}

		partKey := partObjectKey(key, uploadID, partNumber)
		objectHash := model.ObjectHash(bucket, partKey)

		// The ETag is MD5 over the part, and the write path already reads the
		// part end to end, so the hash is teed off that read instead of costing
		// a second pass over a buffered copy.
		digest := model.NewPartETagHasher()

		// Placement comes from the part's own object hash, so a retried part
		// lands on the same nodes without anything deterministic on disk.
		if _, err := writeObject(ctx, bc, ring, cfg, io.TeeReader(body, digest), partSize, objectHash); err != nil {
			slog.ErrorContext(ctx, "Failed to store part", "uploadID", uploadID, "part", partNumber, "error", err)
			HandleError(w, r, mapPutErr(err))
			return
		}
		etag := model.PartETagFrom(digest)

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
		if err := metaPut(ctx, mc, model.TableParts, multipartPartKey(uploadID, partNumber), partBuf.Bytes()); err != nil {
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
		if err := metaPut(ctx, mc, model.TableObjects, partShardKey(uploadID, partNumber), shardBuf.Bytes()); err != nil {
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
