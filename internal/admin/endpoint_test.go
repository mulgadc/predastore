package admin_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mulgadc/predastore/internal/admin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEndpointsAreServedBesideTheProbes(t *testing.T) {
	t.Parallel()

	s := admin.New("127.0.0.1:0", nil, admin.Endpoint{
		Pattern: "GET /repair",
		Handler: admin.JSONHandler(func() any { return map[string]int{"passes": 2} }),
	})
	h := s.Handler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/repair", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
	var body map[string]int
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, 2, body["passes"])

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	assert.Equal(t, http.StatusOK, rec.Code, "the probes are still served")

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/repair", nil))
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code, "an endpoint is read-only")
}
