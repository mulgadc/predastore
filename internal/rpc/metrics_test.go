package rpc

import (
	"context"
	"os"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// metricReader is installed before any test runs, because the telemetry package
// binds its instruments to whichever provider is global on first use. Every test
// in this binary shares it, so the assertions below compare before and after
// rather than absolute values, and each uses node ids of its own.
var metricReader *sdkmetric.ManualReader

func TestMain(m *testing.M) {
	metricReader = sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader)))
	os.Exit(m.Run())
}

// metricValue is the sum or gauge value carrying attrs, or 0 when nothing has
// been recorded for them yet. An absent series and a zero one mean the same
// thing here: nothing has happened to that node.
func metricValue(t *testing.T, name string, attrs map[string]string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := metricReader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					if matches(dp.Attributes, attrs) {
						return dp.Value
					}
				}
			case metricdata.Gauge[int64]:
				for _, dp := range data.DataPoints {
					if matches(dp.Attributes, attrs) {
						return dp.Value
					}
				}
			default:
				t.Fatalf("%s is %T, want a sum or a gauge", name, m.Data)
			}
		}
	}
	return 0
}

func matches(set attribute.Set, want map[string]string) bool {
	for k, v := range want {
		got, ok := set.Value(attribute.Key(k))
		if !ok || got.AsString() != v {
			return false
		}
	}
	return true
}

// A stream that has been closed is no longer open, and one that was opened and
// abandoned still is: the gauge is there to tell those apart.
func TestOpenStreamsReturnToZeroWhenClosed(t *testing.T) {
	nodes := newTestCluster(t, 21, 22)
	ctx := context.Background()
	peer := map[string]string{"node": "22"}

	before := metricValue(t, "predastore.rpc.streams.open", peer)

	stream, err := OpenStream(ctx, nodes[21].client, 22, opPing, &pingHeader{Name: "hello"})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if got := metricValue(t, "predastore.rpc.streams.open", peer); got != before+1 {
		t.Errorf("open streams with one held = %d, want %d", got, before+1)
	}

	if err := stream.Close(); err != nil {
		t.Fatalf("close stream: %v", err)
	}
	if got := metricValue(t, "predastore.rpc.streams.open", peer); got != before {
		t.Errorf("open streams after close = %d, want %d", got, before)
	}

	// The peer answered a request nothing read, so the read side is abandoned
	// and has to be cancelled for the node to drain at teardown.
	stream.CancelRead(0)
}

// Teardown is not always a Close: an abandoned read is cancelled instead, and a
// caller that does both must not have the stream counted out twice.
func TestOpenStreamsReleaseOnceAcrossTeardownPaths(t *testing.T) {
	nodes := newTestCluster(t, 23, 24)
	ctx := context.Background()
	peer := map[string]string{"node": "24"}

	before := metricValue(t, "predastore.rpc.streams.open", peer)

	stream, err := OpenStream(ctx, nodes[23].client, 24, opPing, &pingHeader{Name: "hello"})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	stream.CancelWrite(0)
	stream.CancelRead(0)
	if err := stream.Close(); err != nil {
		t.Logf("close after cancel: %v", err)
	}

	if got := metricValue(t, "predastore.rpc.streams.open", peer); got != before {
		t.Errorf("open streams after three teardown calls = %d, want %d", got, before)
	}
}

func TestConnectionGaugeFollowsHeldConnections(t *testing.T) {
	nodes := newTestCluster(t, 25, 26)
	ctx := context.Background()
	self := map[string]string{"node": "25"}

	if got := metricValue(t, "predastore.rpc.connections", self); got != 0 {
		t.Fatalf("connections before dialling = %d, want 0", got)
	}

	conn, err := nodes[25].pool.Dial(ctx, 26)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if got := metricValue(t, "predastore.rpc.connections", self); got != 1 {
		t.Errorf("connections after dialling one peer = %d, want 1", got)
	}

	nodes[25].pool.Evict(conn)
	if got := metricValue(t, "predastore.rpc.connections", self); got != 0 {
		t.Errorf("connections after eviction = %d, want 0", got)
	}
}

// Closing a connection the pool never held is the caller tidying up. Counting
// it would report peers dropping that were never in contact.
func TestOnlyHeldConnectionsCountAsEvictions(t *testing.T) {
	nodes := newTestCluster(t, 27, 28)
	ctx := context.Background()
	dropped := map[string]string{"node": "28", "reason": "closed"}

	before := metricValue(t, "predastore.rpc.evictions", dropped)

	conn, err := nodes[27].pool.Dial(ctx, 28)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	nodes[27].pool.Evict(conn)
	if got := metricValue(t, "predastore.rpc.evictions", dropped); got != before+1 {
		t.Errorf("evictions after dropping a held connection = %d, want %d", got, before+1)
	}

	nodes[27].pool.Evict(conn)
	if got := metricValue(t, "predastore.rpc.evictions", dropped); got != before+1 {
		t.Errorf("evictions after re-evicting the same connection = %d, want %d", got, before+1)
	}
}

// A connection that answers nothing is evicted by the stall path, and that is a
// different fault from a peer that closed: the reason has to say which.
func TestStallEvictionIsCountedAsAStall(t *testing.T) {
	nodes := newTestCluster(t, 29, 30)
	stalled := map[string]string{"node": "30", "reason": "stall"}

	before := metricValue(t, "predastore.rpc.evictions", stalled)

	conn, err := nodes[29].pool.Dial(context.Background(), 30)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	for range maxStreamStalls {
		nodes[29].pool.noteStall(conn)
	}

	waitFor(t, "the stalled connection to be evicted", func() bool {
		return metricValue(t, "predastore.rpc.evictions", stalled) == before+1
	})
	// The pool must also have let it go, or the count would describe a
	// connection it is still handing out.
	if got := metricValue(t, "predastore.rpc.connections", map[string]string{"node": "29"}); got != 0 {
		t.Errorf("connections held after a stall eviction = %d, want 0", got)
	}
}
