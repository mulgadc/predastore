package gate

import (
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// spanAccount returns the account attribute of the last recorded span, or "".
func spanAccount(t *testing.T, sr *tracetest.SpanRecorder) string {
	t.Helper()
	spans := sr.Ended()
	if len(spans) == 0 {
		t.Fatal("no span recorded")
	}
	for _, kv := range spans[len(spans)-1].Attributes() {
		if string(kv.Key) == attrAccountID {
			return kv.Value.AsString()
		}
	}
	return ""
}

// S3 is where a tenant's data lives, and predastore produces more spans than
// any other service in the fleet. Unattributed, none of them can be tied to
// the account that caused them.
func TestAnnotateSpanAccountNamesTheCaller(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	ctx, span := tp.Tracer("test").Start(t.Context(), "HTTP")
	annotateSpanAccount(ctx, "000000000042")
	span.End()

	if got := spanAccount(t, sr); got != "000000000042" {
		t.Errorf("%s = %q, want %q", attrAccountID, got, "000000000042")
	}
}

// A request that never authenticated belongs to nobody. Recording a blank
// account would read as a tenant whose id went missing.
func TestAnnotateSpanAccountSkipsAnEmptyAccount(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	ctx, span := tp.Tracer("test").Start(t.Context(), "HTTP")
	annotateSpanAccount(ctx, "")
	span.End()

	if got := spanAccount(t, sr); got != "" {
		t.Errorf("%s = %q, want it absent", attrAccountID, got)
	}
}

// A context with no span must not panic: handlers run without tracing in
// tests and whenever the SDK is disabled.
func TestAnnotateSpanAccountWithoutASpan(t *testing.T) {
	annotateSpanAccount(t.Context(), "000000000042")
}
