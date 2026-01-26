package s3db

import (
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

func TestValidateSignatureHTTP(t *testing.T) {
	credentials := map[string]string{
		"TESTACCESSKEY": "TESTSECRETKEY",
	}
	region := "us-east-1"
	service := "s3db"

	tests := []struct {
		name          string
		method        string
		path          string
		setupHeaders  func(req *http.Request)
		wantAccessKey string
		wantErr       bool
	}{
		{
			name:   "Missing Authorization header",
			method: "GET",
			path:   "/v1/get/test-table/test-key",
			setupHeaders: func(req *http.Request) {
				// No auth header
			},
			wantAccessKey: "",
			wantErr:       true,
		},
		{
			name:   "Valid signature",
			method: "GET",
			path:   "/v1/get/test-table/test-key",
			setupHeaders: func(req *http.Request) {
				timestamp := time.Now().UTC().Format(auth.TimeFormat)
				err := auth.GenerateAuthHeaderReq(
					"TESTACCESSKEY",
					"TESTSECRETKEY",
					timestamp,
					region,
					service,
					req,
				)
				if err != nil {
					t.Fatalf("Error generating auth header: %v", err)
				}
			},
			wantAccessKey: "TESTACCESSKEY",
			wantErr:       false,
		},
		{
			name:   "Invalid access key",
			method: "GET",
			path:   "/v1/get/test-table/test-key",
			setupHeaders: func(req *http.Request) {
				timestamp := time.Now().UTC().Format(auth.TimeFormat)
				err := auth.GenerateAuthHeaderReq(
					"INVALIDACCESSKEY",
					"TESTSECRETKEY",
					timestamp,
					region,
					service,
					req,
				)
				if err != nil {
					t.Fatalf("Error generating auth header: %v", err)
				}
			},
			wantAccessKey: "",
			wantErr:       true,
		},
		{
			name:   "Invalid signature (wrong secret)",
			method: "GET",
			path:   "/v1/get/test-table/test-key",
			setupHeaders: func(req *http.Request) {
				timestamp := time.Now().UTC().Format(auth.TimeFormat)
				err := auth.GenerateAuthHeaderReq(
					"TESTACCESSKEY",
					"WRONGSECRETKEY",
					timestamp,
					region,
					service,
					req,
				)
				if err != nil {
					t.Fatalf("Error generating auth header: %v", err)
				}
			},
			wantAccessKey: "",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create request
			req := httptest.NewRequest(tt.method, tt.path, nil)
			req.Host = "localhost:6660"
			if tt.setupHeaders != nil {
				tt.setupHeaders(req)
			}

			// Validate signature
			gotAccessKey, gotErr := ValidateSignatureHTTP(req, credentials, region, service)

			if tt.wantErr {
				assert.Error(t, gotErr)
			} else {
				assert.NoError(t, gotErr)
				assert.Equal(t, tt.wantAccessKey, gotAccessKey)
			}
		})
	}
}

func TestValidateSignatureHTTP_Integration(t *testing.T) {
	// Test full round-trip: sign request on client side, validate on server side
	credentials := map[string]string{
		"TESTACCESSKEY": "TESTSECRETKEY",
	}
	region := "us-east-1"
	service := "s3db"

	// Create chi router with auth middleware
	r := chi.NewRouter()

	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			accessKey, err := ValidateSignatureHTTP(r, credentials, region, service)
			if err != nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				w.Write([]byte(`{"error":"AccessDenied","message":"` + err.Error() + `"}`))
				return
			}
			// Store access key in header (context not needed for this test)
			w.Header().Set("X-Access-Key", accessKey)
			next.ServeHTTP(w, r)
		})
	})

	r.Get("/v1/get/{table}/{key}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"success"}`))
	})

	// Create a signed request
	req := httptest.NewRequest("GET", "/v1/get/test-table/test-key", nil)
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

	// Perform request
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "TESTACCESSKEY", rr.Header().Get("X-Access-Key"))
}

func TestDefaultConstants(t *testing.T) {
	assert.Equal(t, "us-east-1", DefaultRegion)
	assert.Equal(t, "s3db", DefaultService)
}
