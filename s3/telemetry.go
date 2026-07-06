package s3

import (
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// s3SpanMiddleware renames the request span to the resolved S3 action and
// records bucket/key attributes once per request. No-op when tracing is not
// exporting (the span is non-recording).
func s3SpanMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		span := trace.SpanFromContext(r.Context())
		if span.IsRecording() {
			if action := s3Action(r.Method, r.URL.Path); action != "" {
				span.SetName(action)
			}
			bucket, key := parseS3Path(r.URL.Path)
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
