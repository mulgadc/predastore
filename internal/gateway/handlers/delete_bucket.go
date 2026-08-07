package handlers

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/mulgadc/predastore/internal/gateway/auth"
	"github.com/mulgadc/predastore/internal/gateway/model"
)

// DeleteBucket serves DELETE /{bucket}.
func DeleteBucket(st Store, cache *BucketCache) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bucket := chi.URLParam(r, "bucket")

		// DELETE /{bucket}?policy — no-op, bucket policies are not supported
		if r.URL.Query().Has("policy") {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		ownerID := auth.AccessKeyID(r.Context())

		exists, bucketOwner, err := bucketExists(st, cache, bucket)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}
		if !exists {
			HandleError(w, r, model.ErrNoSuchBucketError.WithResource(bucket))
			return
		}
		if ownerID != "" && bucketOwner != ownerID {
			HandleError(w, r, model.ErrAccessDeniedError.WithResource(bucket))
			return
		}

		// One object is enough to reject the delete, so the scan stops at the first.
		objects, err := stateScan(st, model.TableObjects, objectARN(bucket, ""), 1)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}
		if len(objects) > 0 {
			HandleError(w, r, model.ErrBucketNotEmptyError.WithResource(bucket))
			return
		}

		if err := stateDelete(st, model.TableBuckets, bucket); err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "failed to delete bucket: "+err.Error(), 500))
			return
		}

		cache.remove(bucket)

		w.WriteHeader(http.StatusNoContent)
	})
}
