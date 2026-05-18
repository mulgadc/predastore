package s3db

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/mulgadc/predastore/auth"
	"github.com/stretchr/testify/assert"
)

func TestSignRequest(t *testing.T) {
	tests := []struct {
		name      string
		method    string
		path      string
		accessKey string
		secretKey string
		region    string
		service   string
		wantErr   bool
	}{
		{
			name:      "Valid signing",
			method:    "GET",
			path:      "/v1/get/test-table/test-key",
			accessKey: "TESTACCESSKEY",
			secretKey: "TESTSECRETKEY",
			region:    "us-east-1",
			service:   "s3db",
			wantErr:   false,
		},
		{
			name:      "POST request",
			method:    "POST",
			path:      "/v1/put/test-table/test-key",
			accessKey: "TESTACCESSKEY",
			secretKey: "TESTSECRETKEY",
			region:    "us-east-1",
			service:   "s3db",
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(tt.method, "http://localhost:6660"+tt.path, nil)
			assert.NoError(t, err)

			err = SignRequest(req, tt.accessKey, tt.secretKey, tt.region, tt.service)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				// Verify the Authorization header was set
				authHeader := req.Header.Get("Authorization")
				assert.Contains(t, authHeader, "AWS4-HMAC-SHA256")
				assert.Contains(t, authHeader, tt.accessKey)
				// Verify the X-Amz-Date header was set
				dateHeader := req.Header.Get("X-Amz-Date")
				assert.NotEmpty(t, dateHeader)
			}
		})
	}
}

// TestVerifySigV4Request_Integration drives auth.VerifySigV4Request through
// a chi router middleware — proving the verifier behaves correctly when
// composed into the same request pipeline the production authMiddleware
// uses. Verifier-specific unit cases live in predastore/auth.
func TestVerifySigV4Request_Integration(t *testing.T) {
	credentials := map[string]string{"TESTACCESSKEY": "TESTSECRETKEY"}
	const region = "us-east-1"
	const service = "s3db"

	lookup := func(id string) (string, error) {
		secret, ok := credentials[id]
		if !ok {
			return "", fmt.Errorf("invalid access key: %s", id)
		}
		return secret, nil
	}

	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			accessKey, err := auth.VerifySigV4Request(r, region, service, lookup)
			if err != nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"error":"AccessDenied","message":"` + err.Error() + `"}`))
				return
			}
			w.Header().Set("X-Access-Key", accessKey)
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

	timestamp := time.Now().UTC().Format(auth.TimeFormat)
	err := auth.GenerateAuthHeaderReq(
		"TESTACCESSKEY",
		"TESTSECRETKEY",
		timestamp,
		region,
		service,
		req,
	)
	assert.NoError(t, err)

	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "TESTACCESSKEY", rr.Header().Get("X-Access-Key"))
}

func TestDefaultConstants(t *testing.T) {
	assert.Equal(t, "us-east-1", DefaultRegion)
	assert.Equal(t, "s3db", DefaultService)
}
