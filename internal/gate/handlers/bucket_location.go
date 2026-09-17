package handlers

import (
	"log/slog"
	"net/http"
)

// GetBucketLocation serves GET /{bucket}?location.
//
// A null LocationConstraint means us-east-1 to the S3 specification, so an SDK
// that resolves a bucket's region this way would sign for the wrong one. The
// region is the same value HeadBucket reports in X-Amz-Bucket-Region.
func GetBucketLocation(mc MetaClient, cache *BucketCache) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		resource, ok := routedBucket(w, r)
		if !ok {
			return
		}

		meta, err := lookupBucket(ctx, mc, cache, resource.Name)
		if err != nil {
			HandleError(w, r, err)
			return
		}

		// S3 answers an empty constraint for us-east-1 and only for us-east-1,
		// which is the one case where the legacy null reading is correct.
		constraint := meta.Region
		if constraint == "us-east-1" {
			constraint = ""
		}

		if err := writeXML(w, http.StatusOK, LocationConstraint{Value: constraint}); err != nil {
			slog.DebugContext(ctx, "failed to write XML response", "error", err)
		}
	})
}
