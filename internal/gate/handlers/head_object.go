package handlers

import (
	"net/http"
	"strconv"
	"time"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/gate/placement"
)

// httpTimeFormat is the RFC 1123 form S3 dates its responses with.
const httpTimeFormat = "Mon, 02 Jan 2006 15:04:05 GMT"

// lastModified dates a response from the placement record. A record too old to
// carry a time keeps the zero date it has always served: omitting the header
// breaks clients that require one, and a fresh time is the lie this replaced.
func lastModified(p ObjectToShardNodes) string {
	at, ok := p.ModifiedAt()
	if !ok {
		return time.Time{}.Format(httpTimeFormat)
	}

	return at.Format(httpTimeFormat)
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
		// A record with no stored digest omits the ETag rather than serving the
		// old name-derived value; see the same choice in GetObject.
		if etag, ok := place.ETag(); ok {
			w.Header().Set("ETag", etag)
		}
		w.Header().Set("Last-Modified", lastModified(place))
		w.WriteHeader(http.StatusOK)
	})
}
