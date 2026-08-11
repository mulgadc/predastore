package otelsetup

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// actionAttrKey names the logical operation on request metrics. Values must
// stay low-cardinality: resolved S3 action names only, never bucket/key IDs.
const actionAttrKey = "s3.action"

// errorCodeAttrKey carries the real S3 error code (e.g. "NoSuchBucket") when
// the request failed. Bounded set: only predastore's own S3Error codes.
const errorCodeAttrKey = "s3.error_code"

// directionAttrKey distinguishes bytes read from the request body ("in")
// from bytes written to the response body ("out") on the bytes counter.
const directionAttrKey = "direction"

var (
	instrumentsOnce sync.Once
	requestCounter  metric.Int64Counter
	requestDuration metric.Float64Histogram
	requestBytes    metric.Int64Counter
)

// requestInstruments lazily creates the shared request instruments. The
// global meter delegates to the real provider once Init installs it.
func requestInstruments() (metric.Int64Counter, metric.Float64Histogram, metric.Int64Counter) {
	instrumentsOnce.Do(func() {
		m := otel.Meter(httpTracerName)
		var err error
		requestCounter, err = m.Int64Counter("mulga.requests",
			metric.WithDescription("Count of service requests handled."),
			metric.WithUnit("{request}"))
		if err != nil {
			otel.Handle(err)
		}
		requestDuration, err = m.Float64Histogram("mulga.request.duration",
			metric.WithDescription("Duration of handled service requests."),
			metric.WithUnit("s"))
		if err != nil {
			otel.Handle(err)
		}
		requestBytes, err = m.Int64Counter("mulga.request.bytes",
			metric.WithDescription("Bytes transferred handling service requests, by direction."),
			metric.WithUnit("By"))
		if err != nil {
			otel.Handle(err)
		}
	})
	return requestCounter, requestDuration, requestBytes
}

// RequestMetric is the set of attributes recorded for one handled HTTP
// request. Outcome is "success"/"error", or empty when not observable at the
// instrumentation point. ErrorCode is the real S3 error code (e.g.
// "NoSuchBucket") and is empty on success. ReqBytes/RespBytes are the
// request/response body sizes; zero means not known and is not recorded.
type RequestMetric struct {
	Action     string
	Outcome    string
	StatusCode int
	ErrorCode  string
	ReqBytes   int64
	RespBytes  int64
	Elapsed    time.Duration
}

// RecordRequest records one handled request on the shared counter, duration
// histogram, and bytes-transferred counter.
func RecordRequest(ctx context.Context, m RequestMetric) {
	counter, duration, bytesCounter := requestInstruments()

	attrs := []attribute.KeyValue{attribute.String(actionAttrKey, m.Action)}
	if m.Outcome != "" {
		attrs = append(attrs, attribute.String("outcome", m.Outcome))
	}
	if m.StatusCode > 0 {
		attrs = append(attrs, semconv.HTTPResponseStatusCode(m.StatusCode))
	}
	if m.ErrorCode != "" {
		attrs = append(attrs, attribute.String(errorCodeAttrKey, m.ErrorCode))
	}
	opt := metric.WithAttributeSet(attribute.NewSet(attrs...))

	if counter != nil {
		counter.Add(ctx, 1, opt)
	}
	if duration != nil {
		duration.Record(ctx, m.Elapsed.Seconds(), opt)
	}
	if bytesCounter == nil {
		return
	}
	actionAttr := attribute.String(actionAttrKey, m.Action)
	if m.ReqBytes > 0 {
		bytesCounter.Add(ctx, m.ReqBytes, metric.WithAttributeSet(
			attribute.NewSet(actionAttr, attribute.String(directionAttrKey, "in"))))
	}
	if m.RespBytes > 0 {
		bytesCounter.Add(ctx, m.RespBytes, metric.WithAttributeSet(
			attribute.NewSet(actionAttr, attribute.String(directionAttrKey, "out"))))
	}
}

// requestActionKey carries a mutable per-request holder so downstream
// middleware can name the request, and record the S3 error code, after
// routing resolves them.
type requestActionKey struct{}

type requestAction struct {
	name      string
	errorCode string
}

// SetRequestAction sets the logical action recorded on request metrics for
// the in-flight request. No-op when the request did not pass through
// HTTPMiddleware.
func SetRequestAction(ctx context.Context, action string) {
	if h, ok := ctx.Value(requestActionKey{}).(*requestAction); ok && action != "" {
		h.name = action
	}
}

// SetRequestErrorCode records the real S3 error code (e.g. "NoSuchBucket")
// for the in-flight request's metrics. No-op when the request did not pass
// through HTTPMiddleware.
func SetRequestErrorCode(ctx context.Context, code string) {
	if h, ok := ctx.Value(requestActionKey{}).(*requestAction); ok && code != "" {
		h.errorCode = code
	}
}
