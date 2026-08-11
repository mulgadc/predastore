package gate

import (
	"net/http"

	"github.com/mulgadc/predastore/pkg/otelsetup"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// s3SpanMiddleware renames the request span to the resolved S3 action,
// records bucket/key attributes once per request, and names the request
// for metrics. Span work is a no-op when tracing is not exporting.
func s3SpanMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bucket, key := requestBucketKey(r.Context())
		action := s3Action(r.Method, bucket, key)
		if action != "" {
			otelsetup.SetRequestAction(r.Context(), action)
		}
		span := trace.SpanFromContext(r.Context())
		if span.IsRecording() {
			if action != "" {
				span.SetName(action)
			}
			if bucket != "" {
				span.SetAttributes(attribute.String("s3.bucket", bucket))
			}
			if key != "" {
				span.SetAttributes(attribute.String("s3.key", key))
			}
		}
		next.ServeHTTP(w, r)
	})
}
