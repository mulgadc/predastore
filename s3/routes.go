package s3

import "net/http"

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

// setupRoutes maps the S3 REST API onto the handlers. It runs after the
// middleware chain is installed, since chi requires all middleware to be
// registered before the first route.
func (s *HTTP2Server) setupRoutes() {
	r := s.router

	r.Get("/", s.listBuckets)

	// Bucket operations (no key).
	r.Put("/{bucket}", s.createBucket)
	r.Head("/{bucket}", s.headBucket)
	r.Delete("/{bucket}", s.deleteBucket)
	r.Get("/{bucket}", s.listObjects)

	// Object operations (with key).
	r.Head("/{bucket}/*", s.headObject)
	r.Get("/{bucket}/*", s.getObject)
	r.Method(http.MethodPut, "/{bucket}/*", byQuery("partNumber",
		http.HandlerFunc(s.uploadPart),
		http.HandlerFunc(s.putObject)))
	r.Method(http.MethodPost, "/{bucket}/*", byQuery("uploadId",
		http.HandlerFunc(s.completeMultipartUpload),
		http.HandlerFunc(s.createMultipartUpload)))
	r.Method(http.MethodDelete, "/{bucket}/*", byQuery("uploadId",
		http.HandlerFunc(s.abortMultipartUpload),
		http.HandlerFunc(s.deleteObject)))
}
