package gate

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/model"
)

// resolveBucket puts the bucket a bucket-level route matched onto the context.
// It is inline middleware, so it runs after chi has dispatched and reads the
// same URL parameters the handler would.
func resolveBucket(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bucket := model.Bucket{Name: chi.URLParam(r, "bucket")}
		if err := bucket.Validate(); err != nil {
			rejectResource(w, r, err)
			return
		}
		next.ServeHTTP(w, r.WithContext(handlers.WithBucket(r.Context(), bucket)))
	})
}

// resolveObject puts the object an object-level route matched onto the context,
// along with the bucket holding it: the ownership check and requireBucket want
// the bucket alone, and deriving it twice is how the two drift apart.
func resolveObject(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		object := model.Object{
			Bucket: model.Bucket{Name: chi.URLParam(r, "bucket")},
			Key:    chi.URLParam(r, "*"),
		}
		if err := object.Validate(); err != nil {
			rejectResource(w, r, err)
			return
		}
		ctx := handlers.WithBucket(r.Context(), object.Bucket)
		next.ServeHTTP(w, r.WithContext(handlers.WithObject(ctx, object)))
	})
}

// requestBucketKey flattens whichever resource the route resolved into the
// bucket and key names, empty for what the request did not address. The IAM
// action and resource mapping wants one pair across all three route shapes.
func requestBucketKey(ctx context.Context) (bucket, key string) {
	if object, ok := handlers.ObjectFrom(ctx); ok {
		return object.Bucket.Name, object.Key
	}
	if b, ok := handlers.BucketFrom(ctx); ok {
		return b.Name, ""
	}
	return "", ""
}

// rejectResource answers a malformed resource with the error the model named:
// InvalidBucketName or InvalidKey, never AccessDenied. This runs before
// authentication, so a policy outcome would send operators to audit IAM over
// what is a bad request.
func rejectResource(w http.ResponseWriter, r *http.Request, err error) {
	slog.WarnContext(r.Context(), "Rejected malformed S3 request path",
		"path", r.URL.Path, "rawPath", r.URL.RawPath, "error", err, "remoteAddr", r.RemoteAddr)
	handlers.HandleError(w, r, err)
}
