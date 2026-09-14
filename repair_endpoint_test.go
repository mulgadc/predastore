package predastore

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mulgadc/predastore/internal/gate/repair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeRepairReporter struct {
	stats repair.Stats
	ok    bool
}

func (f fakeRepairReporter) RepairStats() (repair.Stats, bool) { return f.stats, f.ok }

func serveRepair(t *testing.T, gw repairReporter) map[string]any {
	t.Helper()

	ep := repairEndpoint(gw)
	require.Equal(t, "GET /repair", ep.Pattern)

	rec := httptest.NewRecorder()
	ep.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/repair", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))

	return body
}

func TestRepairEndpointServesTheCounters(t *testing.T) {
	body := serveRepair(t, fakeRepairReporter{ok: true, stats: repair.Stats{
		Passes: 3, Pending: 1, FailedPeerMissing: 1, RepairedRebuild: 4,
		LastPass: &repair.PassSummary{Outcome: repair.OutcomeComplete, Owed: 1, DurationMs: 42},
	}})

	assert.Equal(t, true, body["enabled"])
	stats, ok := body["stats"].(map[string]any)
	require.True(t, ok, "stats must be an object: %v", body)
	assert.InDelta(t, 3, stats["passes"], 0)
	assert.InDelta(t, 1, stats["pending"], 0)
	assert.InDelta(t, 1, stats["failed_peer_missing"], 0)
	assert.InDelta(t, 4, stats["repaired_rebuild"], 0)

	last, ok := stats["last_pass"].(map[string]any)
	require.True(t, ok, "last_pass must be an object: %v", stats)
	assert.Equal(t, "complete", last["outcome"])
	assert.InDelta(t, 1, last["owed"], 0)
	assert.InDelta(t, 42, last["duration_ms"], 0)
}

func TestRepairEndpointOnAGateThatRunsNoRepair(t *testing.T) {
	body := serveRepair(t, fakeRepairReporter{})

	assert.Equal(t, false, body["enabled"])
	assert.NotContains(t, body, "stats")
}
