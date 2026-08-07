package s3

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// headBucket serves HEAD /{bucket}: existence plus the bucket's region.
func (s *HTTP2Server) headBucket(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")

	meta, err := s.lookupBucket(bucket)
	if err != nil {
		s.handleError(w, r, err)
		return
	}

	w.Header().Set("X-Amz-Bucket-Region", meta.Region)
	w.WriteHeader(http.StatusOK)
}
