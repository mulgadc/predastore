package handlers

import (
	"net/http"
	"strconv"

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
		body, partSize, dec := decodeBody(r)
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

		// The payload check runs after the part has landed but before it is
		// reachable, so a failed check leaves nothing CompleteMultipartUpload
		// can assemble.
		partMeta, written, err := storePart(ctx, mc, bc, ring, cfg,
			bucket, key, uploadID, partNumber, body, partSize,
			func() error { return finishPayload(r, dec) })
		if err != nil {
			HandleError(w, r, err)
			return
		}

		writePartHeaders(w, written)
		w.Header().Set("ETag", partMeta.ETag)
		w.Header().Set("X-Amz-Server-Side-Encryption", "AES256")
		w.WriteHeader(http.StatusOK)
	})
}
