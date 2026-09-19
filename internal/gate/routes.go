package gate

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/placement"
	"github.com/mulgadc/predastore/s3api"
)

// selectRoute answers a method and pattern that several operations share. S3
// distinguishes them by query parameter or header, which chi cannot match on,
// so the first route the request selects wins and the table's order decides.
func selectRoute(routes []s3api.Route, handlers map[string]http.Handler) http.Handler {
	var scope s3api.Scope
	if len(routes) > 0 {
		scope = routes[0].Scope
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		for _, route := range routes {
			if route.Selects(r, query) {
				handlers[route.ID].ServeHTTP(w, r)
				return
			}
		}
		if sub := s3api.SubResource(query); sub != "" {
			unservedSubResource(sub, r.Method, scope).ServeHTTP(w, r)
			return
		}
		methodNotAllowed().ServeHTTP(w, r)
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

// unsetControlCodes names, per sub-resource, the error S3 answers a read of a
// control that is not configured. Reading one of these from predastore is that
// case permanently: the control is not set, and no write can set it, so the
// not-configured code is the true answer and the one a client can carry on
// from. The Terraform AWS provider reads these during refresh and takes the
// code as "absent"; a 501 is an error that fails the plan instead.
//
// A sub-resource is absent from this table when AWS answers an unset read with
// a populated 200 -- notification, logging, accelerate and requestPayment --
// because synthesising that success would claim a control predastore does not
// have. acl is absent for the same reason: every bucket has one, so there is no
// not-configured code to return.
var unsetControlCodes = map[string]string{
	"cors":              "NoSuchCORSConfiguration",
	"encryption":        "ServerSideEncryptionConfigurationNotFoundError",
	"lifecycle":         "NoSuchLifecycleConfiguration",
	"object-lock":       "ObjectLockConfigurationNotFoundError",
	"ownershipControls": "OwnershipControlsNotFoundError",
	"policy":            "NoSuchBucketPolicy",
	"publicAccessBlock": "NoSuchPublicAccessBlockConfiguration",
	"replication":       "ReplicationConfigurationNotFoundError",
	"website":           "NoSuchWebsiteConfiguration",
}

// unservedSubResource answers a sub-resource no route serves, naming it. The
// alternative is the fallback route for the method, which on an object path
// writes, reads or deletes the object itself.
//
// A read of an unset bucket control answers the code S3 answers for that;
// everything else, including every write and every object sub-resource,
// answers NotImplemented. This is the one place that answer is decided.
//
// The codes are bucket controls, so scope gates them: ?policy on an object is
// not an unset bucket policy, it is a sub-resource S3 has no object form of.
func unservedSubResource(sub, method string, scope s3api.Scope) http.Handler {
	if method == http.MethodGet && scope == s3api.ScopeBucket {
		if code, ok := unsetControlCodes[sub]; ok {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				handlers.WriteS3Error(w, r, http.StatusNotFound, code,
					fmt.Sprintf("The %s configuration does not exist", sub))
			})
		}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlers.WriteS3Error(w, r, http.StatusNotImplemented, "NotImplemented",
			fmt.Sprintf("The %s sub-resource is not implemented", sub))
	})
}

// setupRoutes builds the router from the declared S3 route table, so a route
// exists only where s3api names the operation it answers. It runs after the
// middleware chain is installed, since chi requires all middleware to be
// registered before the first route.
//
// The three route groups are the three shapes an S3 request addresses: no
// resource, a bucket, or an object. Middleware registered inside a group is
// inline, so it runs after chi has matched and can resolve the resource from
// the same URL parameters the handler reads.
func (s *Server) setupRoutes(ring *placement.Ring) error {
	built := s.s3Handlers(ring)
	if err := checkHandlers(built); err != nil {
		return err
	}

	s.mountScope(s3api.ScopeService, nil, built)
	s.mountScope(s3api.ScopeBucket, resolveBucket, built)
	s.mountScope(s3api.ScopeObject, resolveObject, built)
	return nil
}

// s3Handlers constructs each handler over the dependencies it needs, keyed by
// the route ID that binds it. bulkBody marks the five that move object data: a
// request deadline that applies to a body caps the object at whatever fits.
func (s *Server) s3Handlers(ring *placement.Ring) map[string]http.Handler {
	mc, bc := s.cfg.Meta, s.cfg.Blob
	cache, cfg := s.buckets, s.handlerCfg

	return map[string]http.Handler{
		"ListBuckets":             handlers.ListBuckets(mc),
		"CreateBucket":            handlers.CreateBucket(mc, cache, cfg),
		"HeadBucket":              handlers.HeadBucket(mc, cache),
		"DeleteBucket":            handlers.DeleteBucket(mc, cache),
		"GetBucketLocation":       handlers.GetBucketLocation(mc, cache),
		"GetBucketTagging":        handlers.GetBucketTagging(mc, cache),
		"PutBucketTagging":        handlers.PutBucketTagging(mc, cache),
		"GetBucketVersioning":     handlers.GetBucketVersioning(mc, cache),
		"PutBucketVersioning":     handlers.PutBucketVersioning(mc, cache),
		"ListObjectVersions":      handlers.ListObjectVersions(mc, cache),
		"DeleteBucketTagging":     handlers.DeleteBucketTagging(mc, cache),
		"ListMultipartUploads":    handlers.ListMultipartUploads(mc, cache),
		"ListObjects":             handlers.ListObjects(mc, cache),
		"DeleteObjects":           handlers.DeleteObjects(mc, bc, cache, cfg),
		"HeadObject":              handlers.HeadObject(mc, ring, cache, cfg),
		"ListParts":               handlers.ListParts(mc, cache),
		"GetObject":               bulkBody(handlers.GetObject(mc, bc, ring, cache, cfg)),
		"UploadPartCopy":          bulkBody(handlers.UploadPartCopy(mc, bc, ring, cache, cfg)),
		"UploadPart":              bulkBody(handlers.UploadPart(mc, bc, ring, cache, cfg)),
		"CopyObject":              bulkBody(handlers.CopyObject(mc, bc, ring, cache, cfg)),
		"PutObject":               bulkBody(handlers.PutObject(mc, bc, ring, cache, cfg)),
		"CompleteMultipartUpload": bulkBody(handlers.CompleteMultipartUpload(mc, bc, ring, cache, cfg)),
		"CreateMultipartUpload":   handlers.CreateMultipartUpload(mc, cache),
		"AbortMultipartUpload":    handlers.AbortMultipartUpload(mc, bc, cache),
		"DeleteObject":            handlers.DeleteObject(mc, bc, cache, cfg),
	}
}

// checkHandlers refuses a table and a handler set that have drifted apart, so a
// declared operation cannot be unserved and a handler cannot be unreachable.
func checkHandlers(built map[string]http.Handler) error {
	declared := map[string]bool{}
	for _, route := range s3api.Routes() {
		declared[route.ID] = true
		if built[route.ID] == nil {
			return fmt.Errorf("s3 route %q has no handler", route.ID)
		}
	}
	for id := range built {
		if !declared[id] {
			return fmt.Errorf("s3 handler %q answers no declared route", id)
		}
	}
	return nil
}

// mountScope registers one route group: the declared routes that address this
// kind of resource, behind the middleware that resolves it.
func (s *Server) mountScope(scope s3api.Scope, resolve func(http.Handler) http.Handler, built map[string]http.Handler) {
	s.router.Group(func(r chi.Router) {
		if resolve != nil {
			r.Use(resolve)
		}
		s.useRequestChain(r)

		for _, group := range groupRoutes(scope) {
			r.Method(group[0].Method, group[0].Pattern, selectRoute(group, built))
		}
	})
}

// groupRoutes returns the scope's routes grouped by method and pattern, in
// declaration order: chi matches those, and the group decides between them.
func groupRoutes(scope s3api.Scope) [][]s3api.Route {
	var groups [][]s3api.Route
	index := map[string]int{}
	for _, route := range s3api.Routes() {
		if route.Scope != scope {
			continue
		}
		key := route.Method + " " + route.Pattern
		if at, ok := index[key]; ok {
			groups[at] = append(groups[at], route)
			continue
		}
		index[key] = len(groups)
		groups = append(groups, []s3api.Route{route})
	}
	return groups
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
