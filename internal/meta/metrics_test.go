package meta

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// metricReader is installed before any test runs: the telemetry package binds
// its instruments to whichever provider is global the first time one is used.
var metricReader *sdkmetric.ManualReader

func TestMain(m *testing.M) {
	metricReader = sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader)))
	os.Exit(m.Run())
}

func opCount(t *testing.T, op, outcome string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := metricReader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	want := map[string]string{"op": op, "outcome": outcome}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "predastore.meta.client.ops" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("meta client ops is %T, want Sum[int64]", m.Data)
			}
			for _, dp := range sum.DataPoints {
				matched := true
				for k, v := range want {
					got, has := dp.Attributes.Value(attribute.Key(k))
					if !has || got.AsString() != v {
						matched = false
						break
					}
				}
				if matched {
					return dp.Value
				}
			}
		}
	}
	return 0
}

// A key that is not there is an answer, not a fault. Folding it into the error
// count would make every HEAD of a missing object look like a broken meta plane
// and drown the failures that matter.
func TestObserveOpKeepsNotFoundOutOfTheErrorCount(t *testing.T) {
	ctx := context.Background()

	before := map[string]int64{
		"success":   opCount(t, "get", "success"),
		"not_found": opCount(t, "get", "not_found"),
		"error":     opCount(t, "get", "error"),
	}

	for _, tc := range []struct {
		outcome string
		err     error
	}{
		{outcome: "success", err: nil},
		{outcome: "not_found", err: ErrNotFound},
		{outcome: "not_found", err: fmt.Errorf("get key: %w", ErrNotFound)},
		{outcome: "error", err: errors.New("connection refused")},
	} {
		err := tc.err
		observeOp(ctx, "get", time.Now(), &err)
	}

	for outcome, want := range map[string]int64{"success": 1, "not_found": 2, "error": 1} {
		if got := opCount(t, "get", outcome) - before[outcome]; got != want {
			t.Errorf("%s operations recorded = %d, want %d", outcome, got, want)
		}
	}
}

// The indices come out of raft as strings, and a node that has never snapshotted
// reports one that will not parse. Zero is the honest reading: no snapshot.
func TestParseRaftStatTreatsUnreadableValuesAsZero(t *testing.T) {
	for in, want := range map[string]int64{
		"42":    42,
		"0":     0,
		"":      0,
		"n/a":   0,
		"1.5":   0,
		" 7 ":   0,
		"-3":    -3,
		"99999": 99999,
	} {
		if got := parseRaftStat(in); got != want {
			t.Errorf("parseRaftStat(%q) = %d, want %d", in, got, want)
		}
	}
}
