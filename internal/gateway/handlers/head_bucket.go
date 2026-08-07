package handlers

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// HeadBucket serves HEAD /{bucket}: existence plus the bucket's region.
func HeadBucket(st Store, cache *BucketCache) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bucket := chi.URLParam(r, "bucket")

		meta, err := lookupBucket(st, cache, bucket)
		if err != nil {
			HandleError(w, r, err)
			return
		}

		w.Header().Set("X-Amz-Bucket-Region", meta.Region)
		w.WriteHeader(http.StatusOK)
	})
}
