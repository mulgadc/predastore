package gate

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestS3SpanMiddlewareRenamesSpan(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	tests := []struct {
		method     string
		path       string
		wantName   string
		wantBucket string
		wantKey    string
	}{
		{http.MethodGet, "/", "s3:ListAllMyBuckets", "", ""},
		{http.MethodGet, "/my-bucket", "s3:ListBucket", "my-bucket", ""},
		{http.MethodPut, "/my-bucket/some/key.txt", "s3:PutObject", "my-bucket", "some/key.txt"},
		{http.MethodDelete, "/my-bucket/obj", "s3:DeleteObject", "my-bucket", "obj"},
	}

	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			handler := s3SpanMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
			req := httptest.NewRequest(tt.method, tt.path, nil)
			ctx, span := tp.Tracer("test").Start(req.Context(), "HTTP")
			handler.ServeHTTP(httptest.NewRecorder(), req.WithContext(ctx))
			span.End()

			spans := sr.Ended()
			got := spans[len(spans)-1]
			if got.Name() != tt.wantName {
				t.Errorf("span name = %q, want %q", got.Name(), tt.wantName)
			}
			attrs := map[string]string{}
			for _, kv := range got.Attributes() {
				attrs[string(kv.Key)] = kv.Value.String()
			}
			if attrs["s3.bucket"] != tt.wantBucket {
				t.Errorf("s3.bucket = %q, want %q", attrs["s3.bucket"], tt.wantBucket)
			}
			if attrs["s3.key"] != tt.wantKey {
				t.Errorf("s3.key = %q, want %q", attrs["s3.key"], tt.wantKey)
			}
		})
	}
}

func TestS3SpanMiddlewareNoSpanIsNoop(t *testing.T) {
	called := false
	handler := s3SpanMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/b/k", nil))
	if !called {
		t.Error("next handler not called without a recording span")
	}
}
