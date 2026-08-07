package gateway

import (
	"net/http"

	"github.com/mulgadc/predastore/internal/gateway/handlers"
	"github.com/mulgadc/predastore/internal/gateway/placement"
	"github.com/mulgadc/predastore/internal/storage"
)

// byQuery routes on the presence of a query parameter. S3 overloads one method
// and path across several operations and distinguishes them by query string,
// which chi cannot match on, so the split is made explicit here rather than
// buried in the handler it dispatches to.
func byQuery(param string, with, without http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has(param) {
			with.ServeHTTP(w, r)
			return
		}
		without.ServeHTTP(w, r)
	})
}

// setupRoutes maps the S3 REST API onto the handlers, constructing each one
// over the dependencies it needs. It runs after the middleware chain is
// installed, since chi requires all middleware to be registered before the
// first route.
func (s *Server) setupRoutes(shards *storage.Client, ring *placement.Ring) {
	r := s.router
	st, cache, cfg := s.state, s.buckets, s.handlerCfg

	r.Method(http.MethodGet, "/", handlers.ListBuckets(st))

	// Bucket operations (no key).
	r.Method(http.MethodPut, "/{bucket}", handlers.CreateBucket(st, cache, cfg))
	r.Method(http.MethodHead, "/{bucket}", handlers.HeadBucket(st, cache))
	r.Method(http.MethodDelete, "/{bucket}", handlers.DeleteBucket(st, cache))
	r.Method(http.MethodGet, "/{bucket}", handlers.ListObjects(st, cache))

	// Object operations (with key).
	r.Method(http.MethodHead, "/{bucket}/*", handlers.HeadObject(st, ring, cache, cfg))
	r.Method(http.MethodGet, "/{bucket}/*", handlers.GetObject(st, shards, ring, cache, cfg))
	r.Method(http.MethodPut, "/{bucket}/*", byQuery("partNumber",
		handlers.UploadPart(st, shards, ring, cache, cfg),
		handlers.PutObject(st, shards, ring, cache, cfg)))
	r.Method(http.MethodPost, "/{bucket}/*", byQuery("uploadId",
		handlers.CompleteMultipartUpload(st, shards, ring, cache, cfg),
		handlers.CreateMultipartUpload(st, cache)))
	r.Method(http.MethodDelete, "/{bucket}/*", byQuery("uploadId",
		handlers.AbortMultipartUpload(st, shards, cache),
		handlers.DeleteObject(st, shards, cache)))
}
