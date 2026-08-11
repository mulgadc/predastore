package handlers

import (
	"net/http"
)

// HeadBucket serves HEAD /{bucket}: existence plus the bucket's region.
func HeadBucket(mc MetaClient, cache *BucketCache) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		resource, ok := routedBucket(w, r)
		if !ok {
			return
		}
		bucket := resource.Name

		meta, err := lookupBucket(ctx, mc, cache, bucket)
		if err != nil {
			HandleError(w, r, err)
			return
		}

		w.Header().Set("X-Amz-Bucket-Region", meta.Region)
		w.WriteHeader(http.StatusOK)
	})
}
