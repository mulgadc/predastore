package blob_test

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestObservedShardWritePhases(t *testing.T) {
	// The child owns the process-wide meter provider, keeping other blob
	// tests independent of the instrumentation already registered here.
	const collect = "PREDASTORE_SHARD_PHASE_TEST"
	if os.Getenv(collect) != "1" {
		cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestObservedShardWritePhases$", "-test.count=1")
		cmd.Env = append(os.Environ(), collect+"=1")
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", output)
		return
	}

	ctx := t.Context()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(provider)
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	delay := 120 * time.Millisecond
	client, _ := startSlowCommitNode(t, delay, 0, time.Second, time.Second, time.Second)
	payload := bytes.Repeat([]byte("p"), 4096)
	key := [32]byte{9}
	_, err := client.Put(ctx, serverNode, blob.PutRequest{
		Key: key, Index: 0, Size: int64(len(payload)), Epoch: slowEpoch,
	}, bytes.NewReader(payload))
	require.NoError(t, err)
	_, err = client.Commit(ctx, serverNode, blob.CommitRequest{Key: key, Index: 0, Epoch: slowEpoch})
	require.NoError(t, err)

	var snapshot metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &snapshot))
	counts := make(map[string]uint64)
	durations := make(map[string]float64)
	for _, scope := range snapshot.ScopeMetrics {
		for _, observation := range scope.Metrics {
			if observation.Name != "predastore.shard.duration" {
				continue
			}
			histogram, ok := observation.Data.(metricdata.Histogram[float64])
			require.True(t, ok)
			for _, datum := range histogram.DataPoints {
				op, _ := datum.Attributes.Value(attribute.Key("op"))
				counts[op.AsString()] += datum.Count
				durations[op.AsString()] += datum.Sum
			}
		}
	}
	for _, op := range []string{"write_body", "prepare_reply", "commit"} {
		require.EqualValues(t, 1, counts[op], "one timing for %s", op)
	}
	require.GreaterOrEqual(t, durations["prepare_reply"], delay.Seconds())
}
