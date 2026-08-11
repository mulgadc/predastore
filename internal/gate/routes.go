package gate

import (
	"net/http"

	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/placement"
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
func (s *Server) setupRoutes(bc handlers.BlobClient, ring *placement.Ring) {
	r := s.router
	mc, cache, cfg := s.meta, s.buckets, s.handlerCfg

	r.Method(http.MethodGet, "/", handlers.ListBuckets(mc))

	// Bucket operations (no key).
	r.Method(http.MethodPut, "/{bucket}", handlers.CreateBucket(mc, cache, cfg))
	r.Method(http.MethodHead, "/{bucket}", handlers.HeadBucket(mc, cache))
	r.Method(http.MethodDelete, "/{bucket}", handlers.DeleteBucket(mc, cache))
	r.Method(http.MethodGet, "/{bucket}", handlers.ListObjects(mc, cache))

	// Object operations (with key).
	r.Method(http.MethodHead, "/{bucket}/*", handlers.HeadObject(mc, ring, cache, cfg))
	r.Method(http.MethodGet, "/{bucket}/*", handlers.GetObject(mc, bc, ring, cache, cfg))
	r.Method(http.MethodPut, "/{bucket}/*", byQuery("partNumber",
		handlers.UploadPart(mc, bc, ring, cache, cfg),
		handlers.PutObject(mc, bc, ring, cache, cfg)))
	r.Method(http.MethodPost, "/{bucket}/*", byQuery("uploadId",
		handlers.CompleteMultipartUpload(mc, bc, ring, cache, cfg),
		handlers.CreateMultipartUpload(mc, cache)))
	r.Method(http.MethodDelete, "/{bucket}/*", byQuery("uploadId",
		handlers.AbortMultipartUpload(mc, bc, cache),
		handlers.DeleteObject(mc, bc, cache)))
}
