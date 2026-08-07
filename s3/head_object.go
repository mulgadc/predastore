package s3

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/mulgadc/predastore/internal/gateway/model"
)

// httpTimeFormat is the RFC 1123 form S3 dates its responses with.
const httpTimeFormat = "Mon, 02 Jan 2006 15:04:05 GMT"

// headObject serves HEAD /{bucket}/{key}: size and entity tag, no body.
func (s *HTTP2Server) headObject(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	key := chi.URLParam(r, "*")

	if err := s.requireBucket(bucket); err != nil {
		s.handleError(w, r, err)
		return
	}

	_, size, err := s.openInput(bucket, key)
	if err != nil {
		s.handleError(w, r, model.ErrNoSuchKeyError.WithResource(key))
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.Header().Set("ETag", model.ObjectETag(bucket, key))
	w.Header().Set("Last-Modified", time.Time{}.Format(httpTimeFormat))
	w.WriteHeader(http.StatusOK)
}
