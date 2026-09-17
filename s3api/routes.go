// Package s3api names the S3 REST operations predastore serves, and the request
// shape that selects each one. The gate builds its router from this table, so
// an operation is served only if it is named here.
package s3api

import (
	"net/http"
	"slices"
)

// Scope is the kind of resource a route addresses, which decides the middleware
// that resolves it before the handler runs.
type Scope string

const (
	ScopeService Scope = "service"
	ScopeBucket  Scope = "bucket"
	ScopeObject  Scope = "object"
)

// Route is one leaf of the S3 dispatch tree. S3 overloads a method and path
// across several operations and picks between them on a query parameter or a
// header, so Query and Header carry that choice rather than the handler.
type Route struct {
	// ID is the stable key the gate binds a handler to.
	ID string

	// Names are the AWS S3 model operations this leaf answers. It is usually
	// one; ListObjects answers both listing versions off the same handler.
	Names []string

	Scope   Scope
	Method  string
	Pattern string

	// Query is the sub-resource query parameter that must be present, and
	// Header the request header that must be set. Empty means unconstrained.
	Query  string
	Header string
}

// Selects reports whether a request chooses this route, given that its method
// and pattern have already matched.
func (r Route) Selects(req *http.Request) bool {
	if r.Query != "" && !req.URL.Query().Has(r.Query) {
		return false
	}
	return r.Header == "" || req.Header.Get(r.Header) != ""
}

// copySourceHeader selects the server-side copy at both levels: with a part
// number it is UploadPartCopy, without it CopyObject.
const copySourceHeader = "X-Amz-Copy-Source"

// routes is ordered most-selective-first within each method and pattern: the
// gate takes the first route a request selects, so an unconstrained route is
// the fallback and must come last.
var routes = []Route{
	{ID: "ListBuckets", Names: []string{"ListBuckets"}, Scope: ScopeService, Method: http.MethodGet, Pattern: "/"},

	{ID: "PutBucketTagging", Names: []string{"PutBucketTagging"}, Scope: ScopeBucket, Method: http.MethodPut, Pattern: "/{bucket}", Query: "tagging"},
	{ID: "PutBucketVersioning", Names: []string{"PutBucketVersioning"}, Scope: ScopeBucket, Method: http.MethodPut, Pattern: "/{bucket}", Query: "versioning"},
	{ID: "CreateBucket", Names: []string{"CreateBucket"}, Scope: ScopeBucket, Method: http.MethodPut, Pattern: "/{bucket}"},
	{ID: "HeadBucket", Names: []string{"HeadBucket"}, Scope: ScopeBucket, Method: http.MethodHead, Pattern: "/{bucket}"},
	{ID: "DeleteBucketTagging", Names: []string{"DeleteBucketTagging"}, Scope: ScopeBucket, Method: http.MethodDelete, Pattern: "/{bucket}", Query: "tagging"},
	{ID: "DeleteBucket", Names: []string{"DeleteBucket"}, Scope: ScopeBucket, Method: http.MethodDelete, Pattern: "/{bucket}"},
	{ID: "GetBucketLocation", Names: []string{"GetBucketLocation"}, Scope: ScopeBucket, Method: http.MethodGet, Pattern: "/{bucket}", Query: "location"},
	{ID: "GetBucketTagging", Names: []string{"GetBucketTagging"}, Scope: ScopeBucket, Method: http.MethodGet, Pattern: "/{bucket}", Query: "tagging"},
	{ID: "GetBucketVersioning", Names: []string{"GetBucketVersioning"}, Scope: ScopeBucket, Method: http.MethodGet, Pattern: "/{bucket}", Query: "versioning"},
	{ID: "ListObjectVersions", Names: []string{"ListObjectVersions"}, Scope: ScopeBucket, Method: http.MethodGet, Pattern: "/{bucket}", Query: "versions"},
	{ID: "ListMultipartUploads", Names: []string{"ListMultipartUploads"}, Scope: ScopeBucket, Method: http.MethodGet, Pattern: "/{bucket}", Query: "uploads"},
	{ID: "ListObjects", Names: []string{"ListObjects", "ListObjectsV2"}, Scope: ScopeBucket, Method: http.MethodGet, Pattern: "/{bucket}"},
	// A POST at a bucket is the batch delete and nothing else. There is no
	// fallback route, so without ?delete the method is not allowed.
	{ID: "DeleteObjects", Names: []string{"DeleteObjects"}, Scope: ScopeBucket, Method: http.MethodPost, Pattern: "/{bucket}", Query: "delete"},

	{ID: "HeadObject", Names: []string{"HeadObject"}, Scope: ScopeObject, Method: http.MethodHead, Pattern: "/{bucket}/*"},
	{ID: "ListParts", Names: []string{"ListParts"}, Scope: ScopeObject, Method: http.MethodGet, Pattern: "/{bucket}/*", Query: "uploadId"},
	{ID: "GetObject", Names: []string{"GetObject"}, Scope: ScopeObject, Method: http.MethodGet, Pattern: "/{bucket}/*"},
	{ID: "UploadPartCopy", Names: []string{"UploadPartCopy"}, Scope: ScopeObject, Method: http.MethodPut, Pattern: "/{bucket}/*", Query: "partNumber", Header: copySourceHeader},
	{ID: "UploadPart", Names: []string{"UploadPart"}, Scope: ScopeObject, Method: http.MethodPut, Pattern: "/{bucket}/*", Query: "partNumber"},
	{ID: "CopyObject", Names: []string{"CopyObject"}, Scope: ScopeObject, Method: http.MethodPut, Pattern: "/{bucket}/*", Header: copySourceHeader},
	{ID: "PutObject", Names: []string{"PutObject"}, Scope: ScopeObject, Method: http.MethodPut, Pattern: "/{bucket}/*"},
	{ID: "CompleteMultipartUpload", Names: []string{"CompleteMultipartUpload"}, Scope: ScopeObject, Method: http.MethodPost, Pattern: "/{bucket}/*", Query: "uploadId"},
	{ID: "CreateMultipartUpload", Names: []string{"CreateMultipartUpload"}, Scope: ScopeObject, Method: http.MethodPost, Pattern: "/{bucket}/*"},
	{ID: "AbortMultipartUpload", Names: []string{"AbortMultipartUpload"}, Scope: ScopeObject, Method: http.MethodDelete, Pattern: "/{bucket}/*", Query: "uploadId"},
	{ID: "DeleteObject", Names: []string{"DeleteObject"}, Scope: ScopeObject, Method: http.MethodDelete, Pattern: "/{bucket}/*"},
}

// Routes returns the declared table in dispatch order.
func Routes() []Route {
	return slices.Clone(routes)
}

// Operations returns the distinct S3 model operations predastore serves, sorted
// for a stable comparison against the AWS service model.
func Operations() []string {
	names := make([]string, 0, len(routes))
	for _, route := range routes {
		names = append(names, route.Names...)
	}
	slices.Sort(names)
	return slices.Compact(names)
}
