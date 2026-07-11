package otelsetup

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const httpTracerName = "github.com/mulgadc/predastore/otelsetup"

// statusRecorder captures the response status and body size for span and
// metric attributes.
type statusRecorder struct {
	http.ResponseWriter

	status  int
	written int64
}

func (w *statusRecorder) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// Write tracks bytes actually written to the client, not a forced read of
// the response body.
func (w *statusRecorder) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.written += int64(n)
	return n, err
}

// HTTPMiddleware opens a server span per request, honoring an inbound W3C
// traceparent header, and records request count/duration metrics. Handlers
// rename the span (and SetRequestAction) once they resolve a logical
// operation (e.g. the S3 action). No-op unless Init configured export.
func HTTPMiddleware(serverName string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
			action := &requestAction{name: r.Method}
			ctx = context.WithValue(ctx, requestActionKey{}, action)
			ctx, span := otel.Tracer(httpTracerName).Start(ctx, r.Method+" "+r.URL.Path,
				trace.WithSpanKind(trace.SpanKindServer),
				trace.WithAttributes(
					semconv.HTTPRequestMethodKey.String(r.Method),
					semconv.URLPath(r.URL.Path),
					attribute.String("server.name", serverName),
				))
			defer span.End()

			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r.WithContext(ctx))

			span.SetAttributes(semconv.HTTPResponseStatusCode(rec.status))
			outcome := "success"
			if rec.status >= http.StatusInternalServerError {
				span.SetStatus(codes.Error, fmt.Sprintf("HTTP %d", rec.status))
				outcome = "error"
			}

			// Content-Length is read from the header set before the body was
			// consumed; -1 (unknown/chunked) is left unrecorded rather than
			// forcing a body read to measure it.
			reqBytes := max(r.ContentLength, 0)
			RecordRequest(ctx, RequestMetric{
				Action:     action.name,
				Outcome:    outcome,
				StatusCode: rec.status,
				ErrorCode:  action.errorCode,
				ReqBytes:   reqBytes,
				RespBytes:  rec.written,
				Elapsed:    time.Since(start),
			})
		})
	}
}
