package s3

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
)

// requestTarget is the bucket and object key a request resolves to, computed
// from the same string chi dispatches on so the authorization subject and the
// value the handlers act on cannot diverge.
type requestTarget struct {
	bucket string
	key    string
}

var (
	errDotSegment       = errors.New("path contains a . or .. segment")
	errKeyTrailingSlash = errors.New("object key has a trailing slash")
)

// s3TargetMiddleware resolves the request's bucket and key onto the context.
// It runs after StripSlashes and before authorization, so policy evaluation,
// the ownership check, throttling and the route handlers all read one pair.
func (s *HTTP2Server) s3TargetMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := validateRequestPath(r); err != nil {
			slog.DebugContext(r.Context(), "Rejected malformed S3 request path",
				"path", r.URL.Path, "rawPath", r.URL.RawPath, "error", err, "remoteAddr", r.RemoteAddr)
			s.writeS3Error(w, r, http.StatusForbidden, "AccessDenied", "Access Denied")
			return
		}

		bucket, key := parseS3Path(resolveRoutePath(r))
		ctx := context.WithValue(r.Context(), contextKeyTarget, requestTarget{bucket: bucket, key: key})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// resolveRoutePath mirrors chi's own routing-path resolution: the StripSlashes
// rewrite wins, then the raw (still percent-encoded) path, then the decoded
// path. Anything else would authorize a different string than chi routes on.
func resolveRoutePath(r *http.Request) string {
	if rctx := chi.RouteContext(r.Context()); rctx != nil && rctx.RoutePath != "" {
		return rctx.RoutePath
	}
	if r.URL.RawPath != "" {
		return r.URL.RawPath
	}
	return r.URL.Path
}

// validateRequestPath rejects paths that only normalise into their dispatched
// form, which is what let an exact-ARN Deny be sidestepped. Both the raw and
// the decoded path are checked, since either can be the routing subject. A
// trailing slash directly after the bucket stays legal — AWS accepts
// `PUT /bucket/` as CreateBucket, and StripSlashes exists to normalise it.
func validateRequestPath(r *http.Request) error {
	for _, path := range [2]string{r.URL.RawPath, r.URL.Path} {
		trimmed := strings.TrimPrefix(path, "/")
		if trimmed == "" {
			continue
		}
		for segment := range strings.SplitSeq(trimmed, "/") {
			if segment == "." || segment == ".." {
				return errDotSegment
			}
		}
		if strings.HasSuffix(trimmed, "/") && strings.Contains(strings.TrimSuffix(trimmed, "/"), "/") {
			return errKeyTrailingSlash
		}
	}
	return nil
}

// requestTargetFrom returns the pair resolved by s3TargetMiddleware. A false
// second result means the middleware did not run and must be treated as a
// failure — an empty target would authorize as ListAllMyBuckets.
func requestTargetFrom(ctx context.Context) (requestTarget, bool) {
	target, ok := ctx.Value(contextKeyTarget).(requestTarget)
	return target, ok
}

// requestBucketKey returns the resolved bucket and key for a routed request.
func requestBucketKey(ctx context.Context) (bucket, key string) {
	target, _ := requestTargetFrom(ctx)
	return target.bucket, target.key
}
