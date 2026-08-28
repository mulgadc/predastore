package handlers

import (
	"context"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/telemetry"
)

// Phases are chained by handing the previous one's end to the next, so the sum
// of a request's phases is its duration and no two phases can cover the same
// instant twice. A helper that returned the old start would double-count every
// phase after the first.
func TestRecordPhaseReturnsTheNextPhaseStart(t *testing.T) {
	ctx := context.Background()

	start := time.Now()
	first := recordPhase(ctx, telemetry.GateOpPut, telemetry.PhaseBucketCheck, start)
	if !first.After(start) && !first.Equal(start) {
		t.Fatalf("next phase starts at %v, before the previous one at %v", first, start)
	}

	time.Sleep(time.Millisecond)
	second := recordPhase(ctx, telemetry.GateOpPut, telemetry.PhaseShardFanout, first)
	if !second.After(first) {
		t.Errorf("second phase ends at %v, at or before the first at %v", second, first)
	}
}
