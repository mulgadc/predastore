package gate

import (
	"net/http"

	"github.com/go-chi/chi/v5"
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

// methodNotAllowed answers the requests a route matches on method and pattern
// but not on the sub-resource that selects the operation.
func methodNotAllowed() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlers.WriteS3Error(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed",
			"The specified method is not allowed against this resource")
	})
}

// setupRoutes maps the S3 REST API onto the handlers, constructing each one
// over the dependencies it needs. It runs after the middleware chain is
// installed, since chi requires all middleware to be registered before the
// first route.
//
// The three route groups are the three shapes an S3 request addresses: no
// resource, a bucket, or an object. Middleware registered inside a group is
// inline, so it runs after chi has matched and can resolve the resource from
// the same URL parameters the handler reads.
func (s *Server) setupRoutes(ring *placement.Ring) {
	r := s.router
	mc, bc := s.cfg.Meta, s.cfg.Blob
	cache, cfg := s.buckets, s.handlerCfg

	// Service operations address no resource.
	r.Group(func(r chi.Router) {
		s.useRequestChain(r)
		r.Method(http.MethodGet, "/", handlers.ListBuckets(mc))
	})

	// Bucket operations (no key).
	r.Group(func(r chi.Router) {
		r.Use(resolveBucket)
		s.useRequestChain(r)

		r.Method(http.MethodPut, "/{bucket}", handlers.CreateBucket(mc, cache, cfg))
		r.Method(http.MethodHead, "/{bucket}", handlers.HeadBucket(mc, cache))
		r.Method(http.MethodDelete, "/{bucket}", handlers.DeleteBucket(mc, cache))
		r.Method(http.MethodGet, "/{bucket}", byQuery("uploads",
			handlers.ListMultipartUploads(mc, cache),
			handlers.ListObjects(mc, cache)))
		// A POST at a bucket is the batch delete and nothing else, so without
		// ?delete the route answers what an unmatched POST answered before it.
		r.Method(http.MethodPost, "/{bucket}", byQuery("delete",
			handlers.DeleteObjects(mc, cache),
			methodNotAllowed()))
	})

	// Object operations (with key).
	r.Group(func(r chi.Router) {
		r.Use(resolveObject)
		s.useRequestChain(r)

		// bulkBody marks the four handlers that move object data. They share a
		// method and pattern with cheap ones, so the choice is made here rather
		// than by route: a request deadline that applies to a body caps the
		// object at whatever fits in it.
		r.Method(http.MethodHead, "/{bucket}/*", handlers.HeadObject(mc, ring, cache, cfg))
		r.Method(http.MethodGet, "/{bucket}/*", byQuery("uploadId",
			handlers.ListParts(mc, cache),
			bulkBody(handlers.GetObject(mc, bc, ring, cache, cfg))))
		r.Method(http.MethodPut, "/{bucket}/*", byQuery("partNumber",
			bulkBody(handlers.UploadPart(mc, bc, ring, cache, cfg)),
			bulkBody(handlers.PutObject(mc, bc, ring, cache, cfg))))
		r.Method(http.MethodPost, "/{bucket}/*", byQuery("uploadId",
			bulkBody(handlers.CompleteMultipartUpload(mc, bc, ring, cache, cfg)),
			handlers.CreateMultipartUpload(mc, cache)))
		r.Method(http.MethodDelete, "/{bucket}/*", byQuery("uploadId",
			handlers.AbortMultipartUpload(mc, bc, cache),
			handlers.DeleteObject(mc, cache)))
	})
}

// useRequestChain installs the middleware every route shares once its resource
// is resolved: span naming, authentication and authorization, then throttling.
// Ordering matters — throttling counts against the authenticated account.
func (s *Server) useRequestChain(r chi.Router) {
	r.Use(s3SpanMiddleware)
	r.Use(s.sigV4AuthMiddleware)
	if throttle := s.throttleMiddleware(); throttle != nil {
		r.Use(throttle)
	}
}
