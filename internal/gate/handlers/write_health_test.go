package handlers

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestAcknowledgedObjectShardMargin(t *testing.T) {
	// The child owns the process-wide meter provider.
	const collect = "PREDASTORE_WRITE_HEALTH_TEST"
	if os.Getenv(collect) != "1" {
		cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestAcknowledgedObjectShardMargin$")
		cmd.Env = append(os.Environ(), collect+"=1")
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", output)
		return
	}

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(provider)
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	g := newRealStoreGate(t, rsProfile{2, 1, 4})
	payload := bytes.Repeat([]byte("d"), 64<<10)
	require.Equal(t, http.StatusOK, g.put("healthy", payload).Code)
	require.Equal(t, http.StatusOK, g.put("empty", nil).Code)
	g.bc.arm(shardFaults{dropCommit: []uint32{0}})
	require.Equal(t, http.StatusOK, g.put("degraded", payload).Code)
	require.Equal(t, payload, g.get("degraded").Body.Bytes())
	g.bc.arm(shardFaults{dropCommit: []uint32{0, 1}})
	require.Equal(t, http.StatusInternalServerError, g.put("rejected", payload).Code)

	var snapshot metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &snapshot))
	for _, scope := range snapshot.ScopeMetrics {
		for _, observation := range scope.Metrics {
			if observation.Name != "predastore.object.shard_margin" {
				continue
			}
			data, ok := observation.Data.(metricdata.Histogram[int64])
			require.True(t, ok)
			require.Len(t, data.DataPoints, 1)
			point := data.DataPoints[0]
			require.EqualValues(t, 2, point.Count)
			require.EqualValues(t, 1, point.Sum)
			require.EqualValues(t, 1, point.BucketCounts[0])
			minimum, ok := point.Min.Value()
			require.True(t, ok)
			require.Zero(t, minimum)
			return
		}
	}
	t.Fatal("acknowledged object shard margin was not exported")
}
