package otelsetup

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// resetRequestInstruments clears the cached request instruments so a test
// can rebind them to its own reader. The sync.Once in requestInstruments
// only ever fires once per process, so tests that need to observe recorded
// values must reset it after installing their own MeterProvider.
func resetRequestInstruments() {
	instrumentsOnce = sync.Once{}
	requestCounter = nil
	requestDuration = nil
	requestBytes = nil
}

// setupTestMeterProvider installs an SDK MeterProvider backed by a
// ManualReader for the duration of the test, and resets the request
// instrument cache so RecordRequest binds to it.
func setupTestMeterProvider(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	resetRequestInstruments()
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		resetRequestInstruments()
	})
	return reader
}

// collectMetric returns the collected data for the named instrument, or nil
// if it was not recorded.
func collectMetric(t *testing.T, reader *sdkmetric.ManualReader, name string) *metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for i := range sm.Metrics {
			if sm.Metrics[i].Name == name {
				return &sm.Metrics[i]
			}
		}
	}
	return nil
}

// attrsOf returns the string/int-stringified attribute map for the sole
// data point of an int64 Sum metric.
func attrsOf(t *testing.T, m *metricdata.Metrics) map[string]string {
	t.Helper()
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok || len(sum.DataPoints) == 0 {
		t.Fatalf("metric %s has no int64 sum data points", m.Name)
	}
	got := map[string]string{}
	for _, kv := range sum.DataPoints[0].Attributes.ToSlice() {
		got[string(kv.Key)] = kv.Value.Emit()
	}
	return got
}

func TestRecordRequestSuccess2xx(t *testing.T) {
	reader := setupTestMeterProvider(t)

	RecordRequest(context.Background(), RequestMetric{
		Action:     "GetObject",
		Outcome:    "success",
		StatusCode: http.StatusOK,
		ReqBytes:   0,
		RespBytes:  4096,
		Elapsed:    10 * time.Millisecond,
	})

	reqs := collectMetric(t, reader, "mulga.requests")
	if reqs == nil {
		t.Fatal("mulga.requests not recorded")
	}
	attrs := attrsOf(t, reqs)
	if attrs["s3.action"] != "GetObject" {
		t.Errorf("s3.action = %q, want GetObject", attrs["s3.action"])
	}
	if attrs["outcome"] != "success" {
		t.Errorf("outcome = %q, want success", attrs["outcome"])
	}
	if attrs["http.response.status_code"] != "200" {
		t.Errorf("http.response.status_code = %q, want 200", attrs["http.response.status_code"])
	}
	if _, ok := attrs["s3.error_code"]; ok {
		t.Errorf("s3.error_code should be absent on success, got %q", attrs["s3.error_code"])
	}

	bytes := collectMetric(t, reader, "mulga.request.bytes")
	if bytes == nil {
		t.Fatal("mulga.request.bytes not recorded")
	}
	sum, ok := bytes.Data.(metricdata.Sum[int64])
	if !ok || len(sum.DataPoints) != 1 {
		t.Fatalf("mulga.request.bytes data points = %+v, want 1 (out only)", bytes.Data)
	}
	dp := sum.DataPoints[0]
	if dp.Value != 4096 {
		t.Errorf("bytes value = %d, want 4096", dp.Value)
	}
	direction, _ := dp.Attributes.Value(attribute.Key(directionAttrKey))
	if direction.AsString() != "out" {
		t.Errorf("direction = %q, want out", direction.AsString())
	}
}

func TestRecordRequestClientError4xx(t *testing.T) {
	reader := setupTestMeterProvider(t)

	RecordRequest(context.Background(), RequestMetric{
		Action:     "GetObject",
		Outcome:    "error",
		StatusCode: http.StatusNotFound,
		ErrorCode:  "NoSuchKey",
		Elapsed:    time.Millisecond,
	})

	reqs := collectMetric(t, reader, "mulga.requests")
	if reqs == nil {
		t.Fatal("mulga.requests not recorded")
	}
	attrs := attrsOf(t, reqs)
	if attrs["http.response.status_code"] != "404" {
		t.Errorf("http.response.status_code = %q, want 404", attrs["http.response.status_code"])
	}
	if attrs["s3.error_code"] != "NoSuchKey" {
		t.Errorf("s3.error_code = %q, want NoSuchKey", attrs["s3.error_code"])
	}
	// outcome stays whatever the middleware bucketed (4xx isn't forced to
	// "error" here since HTTPMiddleware only flips outcome on >=500).
	if attrs["outcome"] != "error" {
		t.Errorf("outcome = %q, want error", attrs["outcome"])
	}
}

func TestRecordRequestServerError5xx(t *testing.T) {
	reader := setupTestMeterProvider(t)

	RecordRequest(context.Background(), RequestMetric{
		Action:     "PutObject",
		Outcome:    "error",
		StatusCode: http.StatusInternalServerError,
		ErrorCode:  "InternalError",
		ReqBytes:   1024,
		Elapsed:    5 * time.Millisecond,
	})

	reqs := collectMetric(t, reader, "mulga.requests")
	if reqs == nil {
		t.Fatal("mulga.requests not recorded")
	}
	attrs := attrsOf(t, reqs)
	if attrs["http.response.status_code"] != "500" {
		t.Errorf("http.response.status_code = %q, want 500", attrs["http.response.status_code"])
	}
	if attrs["s3.error_code"] != "InternalError" {
		t.Errorf("s3.error_code = %q, want InternalError", attrs["s3.error_code"])
	}

	bytes := collectMetric(t, reader, "mulga.request.bytes")
	if bytes == nil {
		t.Fatal("mulga.request.bytes not recorded")
	}
	sum, ok := bytes.Data.(metricdata.Sum[int64])
	if !ok || len(sum.DataPoints) != 1 {
		t.Fatalf("mulga.request.bytes data points = %+v, want 1 (in only)", bytes.Data)
	}
	dp := sum.DataPoints[0]
	if dp.Value != 1024 {
		t.Errorf("bytes value = %d, want 1024", dp.Value)
	}
	direction, _ := dp.Attributes.Value(attribute.Key(directionAttrKey))
	if direction.AsString() != "in" {
		t.Errorf("direction = %q, want in", direction.AsString())
	}
}

func TestRecordRequestZeroBytesNotRecorded(t *testing.T) {
	reader := setupTestMeterProvider(t)

	RecordRequest(context.Background(), RequestMetric{
		Action:     "HeadObject",
		Outcome:    "success",
		StatusCode: http.StatusOK,
		Elapsed:    time.Millisecond,
	})

	if m := collectMetric(t, reader, "mulga.request.bytes"); m != nil {
		if sum, ok := m.Data.(metricdata.Sum[int64]); ok && len(sum.DataPoints) != 0 {
			t.Errorf("expected no bytes data points when ReqBytes/RespBytes are 0, got %+v", sum.DataPoints)
		}
	}
}
