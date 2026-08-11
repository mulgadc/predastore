package handlers

import (
	"log/slog"
	"net/http"
	"sort"
	"strconv"

	"github.com/mulgadc/predastore/internal/gate/model"
)

// defaultMaxParts is the page size S3 uses when a request names none.
const defaultMaxParts = 1000

// ListParts serves GET /{bucket}/{key}?uploadId=X. Clients call this to learn
// which parts the server holds before completing an upload, so without it a
// completion arrives with an empty part list and is rejected.
func ListParts(mc MetaClient, cache *BucketCache) http.Handler {
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

		maxParts := queryInt(r, "max-parts", defaultMaxParts)
		if maxParts < 0 || maxParts > defaultMaxParts {
			maxParts = defaultMaxParts
		}
		marker := queryInt(r, "part-number-marker", 0)

		storedParts, err := getStoredParts(ctx, mc, uploadID)
		if err != nil {
			slog.ErrorContext(ctx, "Failed to list parts", "uploadID", uploadID, "error", err)
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to list parts", 500))
			return
		}
		sort.Slice(storedParts, func(i, j int) bool {
			return storedParts[i].PartNumber < storedParts[j].PartNumber
		})

		// The marker is exclusive: a page resumes after the part it names.
		result := ListPartsResult{
			Bucket:           bucket,
			Key:              key,
			UploadId:         uploadID,
			StorageClass:     "STANDARD",
			PartNumberMarker: marker,
			MaxParts:         maxParts,
		}
		for _, p := range storedParts {
			if p.PartNumber <= marker {
				continue
			}
			if len(result.Parts) == maxParts {
				result.IsTruncated = true
				break
			}
			result.Parts = append(result.Parts, ListPart{
				PartNumber:   p.PartNumber,
				LastModified: p.LastModified,
				ETag:         p.ETag,
				Size:         p.Size,
			})
		}
		if n := len(result.Parts); n > 0 {
			result.NextPartNumberMarker = result.Parts[n-1].PartNumber
		}

		if err := writeXML(w, http.StatusOK, result); err != nil {
			slog.DebugContext(ctx, "failed to write XML response", "error", err)
		}
	})
}

// queryInt reads a non-negative integer query parameter, falling back to the
// default when it is absent or unparseable.
func queryInt(r *http.Request, name string, fallback int) int {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return fallback
	}
	return n
}
