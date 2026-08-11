package handlers

import (
	"context"
	"net/http"

	"github.com/mulgadc/predastore/internal/gate/model"
)

// contextKey namespaces the resource a route resolved before its handler ran.
type contextKey string

const (
	// contextKeyBucket carries the bucket for every routed request that names
	// one, object routes included.
	contextKeyBucket contextKey = "bucket"
	// contextKeyObject carries the object for object routes only.
	contextKeyObject contextKey = "object"
)

// errNoResource is returned when a handler runs without the resource its route
// resolves. It cannot happen through the router, so it surfaces as
// InternalError rather than a client error.
var errNoResource = model.NewS3Error(model.ErrInternalError, "request resource was not resolved", 500)

// WithBucket attaches a resolved bucket to the request context.
func WithBucket(ctx context.Context, bucket model.Bucket) context.Context {
	return context.WithValue(ctx, contextKeyBucket, bucket)
}

// WithObject attaches a resolved object to the request context.
func WithObject(ctx context.Context, object model.Object) context.Context {
	return context.WithValue(ctx, contextKeyObject, object)
}

// BucketFrom returns the bucket the route resolved. A false second result means
// the request addressed no bucket, which is only ListBuckets.
func BucketFrom(ctx context.Context) (model.Bucket, bool) {
	bucket, ok := ctx.Value(contextKeyBucket).(model.Bucket)
	return bucket, ok
}

// ObjectFrom returns the object the route resolved. A false second result means
// the request addressed a bucket or nothing at all.
func ObjectFrom(ctx context.Context) (model.Object, bool) {
	object, ok := ctx.Value(contextKeyObject).(model.Object)
	return object, ok
}

// routedBucket returns the bucket a bucket-level handler's route resolved,
// answering the error itself when the resolver did not run.
func routedBucket(w http.ResponseWriter, r *http.Request) (model.Bucket, bool) {
	bucket, ok := BucketFrom(r.Context())
	if !ok {
		HandleError(w, r, errNoResource)
	}
	return bucket, ok
}

// routedObject returns the object an object-level handler's route resolved,
// answering the error itself when the resolver did not run.
func routedObject(w http.ResponseWriter, r *http.Request) (model.Object, bool) {
	object, ok := ObjectFrom(r.Context())
	if !ok {
		HandleError(w, r, errNoResource)
	}
	return object, ok
}
