package s3db

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/mulgadc/predastore/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestVerifySigV4Request_Integration wires Parse + secret lookup +
// Verify through a chi middleware and asserts a signed request reaches
// the handler.
func TestVerifySigV4Request_Integration(t *testing.T) {
	credentials := map[string]string{"TESTACCESSKEY": "TESTSECRETKEY"}
	const region = "us-east-1"
	const service = "s3db"

	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sig, err := auth.ParseReq(r)
			if err != nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"error":"AccessDenied","message":"` + err.Error() + `"}`))
				return
			}
			secret, ok := credentials[sig.AccessKeyID]
			if !ok {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			if err := sig.Verify(secret, service, region); err != nil {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			w.Header().Set("X-Access-Key", sig.AccessKeyID)
			next.ServeHTTP(w, r)
		})
	})

	r.Get("/v1/get/{table}/{key}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"success"}`))
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/get/test-table/test-key", nil)
	req.Host = "localhost:6660"
	sum := sha256.Sum256(nil)
	require.NoError(t, auth.SignReq(req, "TESTACCESSKEY", "TESTSECRETKEY",
		hex.EncodeToString(sum[:]), service, region))

	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "TESTACCESSKEY", rr.Header().Get("X-Access-Key"))
}

func TestDefaultConstants(t *testing.T) {
	assert.Equal(t, "us-east-1", DefaultRegion)
	assert.Equal(t, "s3db", DefaultService)
}
