package handlers

import (
	"bytes"
	"encoding/gob"
	"log/slog"
	"net/http"
	"sort"

	"github.com/mulgadc/predastore/internal/gate/model"
)

// defaultMaxUploads is the page size S3 reports when the client asks for none.
const defaultMaxUploads = 1000

// ListMultipartUploads serves GET /{bucket}?uploads: the uploads started
// against this bucket and not yet completed or aborted.
//
// Without it an abandoned upload cannot be found — aborting one needs its
// bucket, key and uploadId — so its parts hold storage that nothing can
// attribute to anyone and no teardown can reach.
func ListMultipartUploads(mc MetaClient, cache *BucketCache) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		resource, ok := routedBucket(w, r)
		if !ok {
			return
		}
		bucket := resource.Name

		if err := requireBucket(ctx, mc, cache, bucket); err != nil {
			HandleError(w, r, err)
			return
		}

		// Uploads are keyed by upload id alone, so there is no per-bucket
		// prefix to scan and the whole table is read and filtered. In-flight
		// uploads are few and short-lived, which is what makes that affordable.
		items, err := metaScan(ctx, mc, model.TableMultipart, "", 0)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "failed to list multipart uploads: "+err.Error(), 500))
			return
		}

		result := ListMultipartUploadsResult{
			Bucket:     bucket,
			MaxUploads: defaultMaxUploads,
		}

		// A corrupt row is skipped rather than failing the listing: one bad
		// upload must not hide every other upload in the bucket.
		for _, item := range items {
			var metadata model.UploadMetadata
			if err := gob.NewDecoder(bytes.NewReader(item.Value)).Decode(&metadata); err != nil {
				slog.WarnContext(ctx, "Skipping corrupt multipart entry during scan", "key", item.Key, "error", err)
				continue
			}
			if metadata.Bucket != bucket {
				continue
			}
			result.Uploads = append(result.Uploads, MultipartUpload{
				Key:       metadata.Key,
				UploadId:  metadata.UploadID,
				Initiated: metadata.CreatedAt,
			})
		}

		// S3 orders by key then upload id. The scan has no order of its own,
		// so a caller paging or diffing two listings needs one imposed here.
		sort.Slice(result.Uploads, func(i, j int) bool {
			if result.Uploads[i].Key != result.Uploads[j].Key {
				return result.Uploads[i].Key < result.Uploads[j].Key
			}
			return result.Uploads[i].UploadId < result.Uploads[j].UploadId
		})

		if err := writeXML(w, http.StatusOK, result); err != nil {
			slog.DebugContext(ctx, "failed to write XML response", "error", err)
		}
	})
}
