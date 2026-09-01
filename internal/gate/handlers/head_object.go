package handlers

import (
	"net/http"
	"strconv"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/gate/placement"
)

// httpTimeFormat is the RFC 1123 form S3 dates its responses with.
const httpTimeFormat = "Mon, 02 Jan 2006 15:04:05 GMT"

// lastModified dates a response from the placement record.
func lastModified(p ObjectToShardNodes) string {
	return p.ModifiedAt().Format(httpTimeFormat)
}

// HeadObject serves HEAD /{bucket}/{key}: size and entity tag, no body.
func HeadObject(mc MetaClient, ring *placement.Ring, cache *BucketCache, cfg Config) http.Handler {
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

		place, size, err := loadPlacement(ctx, mc, ring, cfg, bucket, key)
		if err != nil {
			HandleError(w, r, model.ErrNoSuchKeyError.WithResource(key))
			return
		}

		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		// A whole object's record always carries a digest; the guard covers the
		// part records that share this type and do not.
		if etag, ok := place.ETag(); ok {
			w.Header().Set("ETag", etag)
		}
		w.Header().Set("Last-Modified", lastModified(place))
		w.WriteHeader(http.StatusOK)
	})
}
