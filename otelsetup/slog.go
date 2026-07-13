package otelsetup

import (
	"context"
	"errors"
	"log/slog"
	"os"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	loggerglobal "go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/trace"
)

// SetDefaultJSONLogger installs the process-wide slog default: JSON to
// stdout (journald) at the given level, with trace_id/span_id stamping. If a
// real OTLP LoggerProvider is already installed (via Init), the default also
// fans out to it, so repeated calls (once per HTTP server setup) re-establish
// stdout-at-level without ever clobbering the OTLP bridge. Both sinks are
// gated at the same level: the OTLP bridge has no severity filter of its
// own, so without gating it every record (including Debug) would ship to
// OTLP regardless of the configured level.
func SetDefaultJSONLogger(level slog.Level) {
	stdout := NewSlogHandler(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))

	lp, ok := loggerglobal.GetLoggerProvider().(*sdklog.LoggerProvider)
	if !ok {
		slog.SetDefault(slog.New(stdout))
		return
	}
	bridge := otelslog.NewHandler("predastore", otelslog.WithLoggerProvider(lp))
	slog.SetDefault(slog.New(newFanoutHandler(stdout, newLevelHandler(level, bridge))))
}

var _ slog.Handler = (*traceHandler)(nil)

// traceHandler stamps trace_id/span_id from the record's context onto every
// log line so any log can be pivoted to its trace in the backend.
type traceHandler struct {
	inner slog.Handler
}

// NewSlogHandler wraps inner so records logged with a context carrying an
// active span gain trace_id and span_id attributes. Records without a span
// pass through unchanged.
func NewSlogHandler(inner slog.Handler) slog.Handler {
	return &traceHandler{inner: inner}
}

func (h *traceHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.inner.Handle(ctx, r)
}

func (h *traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *traceHandler) WithGroup(name string) slog.Handler {
	return &traceHandler{inner: h.inner.WithGroup(name)}
}

var _ slog.Handler = (*levelHandler)(nil)

// levelHandler gates inner at a minimum level. The otelslog bridge reports
// Enabled==true for every level (its BatchProcessor has no severity filter),
// so without this wrapper every Debug record would be exported to OTLP
// regardless of the configured level — and slog would never short-circuit
// Debug calls at the call site.
type levelHandler struct {
	level slog.Level
	inner slog.Handler
}

// newLevelHandler returns a handler that only forwards records at or above
// level to inner, regardless of what inner.Enabled reports.
func newLevelHandler(level slog.Level, inner slog.Handler) slog.Handler {
	return &levelHandler{level: level, inner: inner}
}

func (h *levelHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= h.level }

func (h *levelHandler) Handle(ctx context.Context, r slog.Record) error {
	return h.inner.Handle(ctx, r)
}

func (h *levelHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &levelHandler{level: h.level, inner: h.inner.WithAttrs(attrs)}
}

func (h *levelHandler) WithGroup(name string) slog.Handler {
	return &levelHandler{level: h.level, inner: h.inner.WithGroup(name)}
}

var _ slog.Handler = (*fanoutHandler)(nil)

// fanoutHandler writes every record to all inner handlers, e.g. so OTLP
// export is additive to the existing stdout/journald handler rather than
// replacing it.
type fanoutHandler struct {
	handlers []slog.Handler
}

// newFanoutHandler returns a handler that fans out to all of handlers.
func newFanoutHandler(handlers ...slog.Handler) slog.Handler {
	return &fanoutHandler{handlers: handlers}
}

func (h *fanoutHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, inner := range h.handlers {
		if inner.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (h *fanoutHandler) Handle(ctx context.Context, r slog.Record) error {
	var errs error
	for _, inner := range h.handlers {
		if inner.Enabled(ctx, r.Level) {
			errs = errors.Join(errs, inner.Handle(ctx, r.Clone()))
		}
	}
	return errs
}

func (h *fanoutHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := make([]slog.Handler, len(h.handlers))
	for i, inner := range h.handlers {
		next[i] = inner.WithAttrs(attrs)
	}
	return &fanoutHandler{handlers: next}
}

func (h *fanoutHandler) WithGroup(name string) slog.Handler {
	next := make([]slog.Handler, len(h.handlers))
	for i, inner := range h.handlers {
		next[i] = inner.WithGroup(name)
	}
	return &fanoutHandler{handlers: next}
}
