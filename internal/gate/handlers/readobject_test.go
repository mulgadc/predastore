package handlers

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/telemetry"
)

// readObject assembles the complete object in memory. The serving paths all
// stream, so this is a test-only buffered read the assertions compare against.
func readObject(ctx context.Context, bc BlobClient, cfg Config, bucket, key string, place ObjectToShardNodes, size int64, handoff config.NodeID, opts ...stripeOption) ([]byte, int, error) {
	// An empty object has no shards to read: the write path stores none, since
	// the blob protocol has no zero-length value to store.
	if size == 0 {
		telemetry.RecordObjectRead(ctx, telemetry.ReadPathDirect)

		return nil, 0, nil
	}

	defer telemetry.EnterGateInflight(ctx, telemetry.GateOpGet, size)()

	start := time.Now()
	reader, err := newStripeReader(ctx, bc, cfg, model.ObjectHash(bucket, key), place, handoff, opts...)
	if err != nil {
		return nil, 0, model.NewS3Error(model.ErrInternalError, err.Error(), 500)
	}
	defer reader.close(ctx)

	out := bytes.NewBuffer(make([]byte, 0, size))
	if err := pipeObject(ctx, reader, out, size); err != nil {
		return nil, 0, model.NewS3Error(model.ErrInternalError,
			fmt.Sprintf("reconstruction failed: %v", err), 500)
	}
	reportDegradedRead(ctx, bucket, key, reader.failures, reader.reconstructed, time.Since(start))
	if reader.reconstructed > 0 {
		telemetry.RecordObjectRead(ctx, telemetry.ReadPathReconstructed)
	} else {
		telemetry.RecordObjectRead(ctx, telemetry.ReadPathDirect)
	}

	return out.Bytes(), reader.reconstructed, nil
}
