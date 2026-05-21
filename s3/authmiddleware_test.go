package s3

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/mulgadc/predastore/auth"
	"github.com/stretchr/testify/assert"
)

func TestSigV4AuthMiddleware(t *testing.T) {
	// Create a test S3 config with known auth credentials
	s3Config := &Config{
		Region: "ap-southeast-2",
		Buckets: []S3_Buckets{
			{
				Name:     "test-bucket01",
				Region:   "ap-southeast-2",
				Type:     "distributed",
				Pathname: "testdata/test-bucket01",
				Public:   false, // Not public - requires authentication
			},
		},
		Auth: []AuthEntry{
			{
				AccessKeyID:     "TESTACCESSKEY",
				SecretAccessKey: "TESTSECRETKEY",
				Policy: []PolicyRule{
					{
						Bucket:  "test-bucket01",
						Actions: []string{"s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:ListObjects"},
					},
				},
			},
		},
	}

	// Setup test cases
	tests := []struct {
		name           string
		method         string
		path           string
		setupHeaders   func(t *testing.T, req *http.Request)
		query          url.Values
		expectStatus   int
		expectResponse string
	}{
		{
			name:   "No Auth Header",
			method: "GET",
			path:   "/test-bucket01",
			setupHeaders: func(t *testing.T, req *http.Request) {
				// No auth header
			},
			expectStatus:   http.StatusForbidden,
			expectResponse: "Missing Authorization header",
			query:          url.Values{}, // add S3 query param
		},

		{
			name:   "Valid Signature",
			method: "GET",
			path:   "/test-bucket01",
			setupHeaders: func(t *testing.T, req *http.Request) {
				signTestReq(t, req, nil, "TESTACCESSKEY", "TESTSECRETKEY", "ap-southeast-2", "s3")
			},
			expectStatus:   -1, // sentinel: assert NOT 403 (auth passed, backend is nil so handler may 500)
			expectResponse: "",
		},

		{
			name:   "Valid Signature Wrong Region",
			method: "GET",
			path:   "/test-bucket01",
			setupHeaders: func(t *testing.T, req *http.Request) {
				signTestReq(t, req, nil, "TESTACCESSKEY", "TESTSECRETKEY", "ap-southeast-1", "s3")
			},
			expectStatus:   http.StatusForbidden,
			expectResponse: "Request authenticated",
		},

		{
			name:   "Invalid Access Key",
			method: "GET",
			path:   "/test-bucket01",
			setupHeaders: func(t *testing.T, req *http.Request) {
				signTestReq(t, req, nil, "INVALIDACCESSKEY", "TESTSECRETKEY", "ap-southeast-2", "s3")
			},
			expectStatus:   http.StatusForbidden,
			expectResponse: "Invalid access key",
		},
		{
			name:   "Invalid Signature",
			method: "GET",
			path:   "/test-bucket01",
			setupHeaders: func(t *testing.T, req *http.Request) {
				signTestReq(t, req, nil, "TESTACCESSKEY", "WRONGSECRETKEY", "ap-southeast-2", "s3")
			},
			expectStatus:   http.StatusForbidden,
			expectResponse: "Invalid signature",
		},
		{
			name:   "Expired Timestamp - Too Old",
			method: "GET",
			path:   "/test-bucket01",
			setupHeaders: func(t *testing.T, req *http.Request) {
				// 6 minutes in the past — exceeds the 5-minute maxClockSkew
				signTestReq(t, req, nil, "TESTACCESSKEY", "TESTSECRETKEY", "ap-southeast-2", "s3",
					auth.WithTime(time.Now().UTC().Add(-6*time.Minute)))
			},
			expectStatus:   http.StatusForbidden,
			expectResponse: "RequestTimeTooSkewed",
		},
		{
			name:   "Expired Timestamp - Too Far In Future",
			method: "GET",
			path:   "/test-bucket01",
			setupHeaders: func(t *testing.T, req *http.Request) {
				// 6 minutes in the future
				signTestReq(t, req, nil, "TESTACCESSKEY", "TESTSECRETKEY", "ap-southeast-2", "s3",
					auth.WithTime(time.Now().UTC().Add(6*time.Minute)))
			},
			expectStatus:   http.StatusForbidden,
			expectResponse: "RequestTimeTooSkewed",
		},
		{
			name:   "Missing X-Amz-Date Header",
			method: "GET",
			path:   "/test-bucket01",
			setupHeaders: func(t *testing.T, req *http.Request) {
				signTestReq(t, req, nil, "TESTACCESSKEY", "TESTSECRETKEY", "ap-southeast-2", "s3")
				// Remove the date header after signing
				req.Header.Del("X-Amz-Date")
			},
			expectStatus:   http.StatusForbidden,
			expectResponse: "Missing required header",
		},
		{
			name:   "Invalid X-Amz-Date Format",
			method: "GET",
			path:   "/test-bucket01",
			setupHeaders: func(t *testing.T, req *http.Request) {
				signTestReq(t, req, nil, "TESTACCESSKEY", "TESTSECRETKEY", "ap-southeast-2", "s3")
				// Replace with malformed date
				req.Header.Set("X-Amz-Date", "not-a-valid-date")
			},
			expectStatus:   http.StatusForbidden,
			expectResponse: "Invalid X-Amz-Date",
		},
	}

	server := NewHTTP2ServerWithBackend(s3Config, nil, NewConfigProvider(s3Config.Auth))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create request
			req := httptest.NewRequest(tt.method, tt.path, nil)
			if tt.setupHeaders != nil {
				tt.setupHeaders(t, req)
			}

			// Wrap a stub next handler so we exercise only the auth middleware
			// and never reach the routed S3 handlers (which need a backend).
			nextCalled := false
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				nextCalled = true
				w.WriteHeader(http.StatusOK)
			})

			rr := httptest.NewRecorder()
			server.sigV4AuthMiddleware(next).ServeHTTP(rr, req)

			if tt.expectStatus == -1 {
				// Sentinel: valid auth should pass through to next.
				assert.True(t, nextCalled, "Valid signature should pass through to next handler")
				assert.Equal(t, http.StatusOK, rr.Code)
			} else {
				assert.False(t, nextCalled, "Failing auth must not invoke next handler")
				assert.Equal(t, tt.expectStatus, rr.Code)
			}
		})
	}
}

// TestSigV4AuthMiddleware_RequireSignedHeaders asserts that requests
// missing "host" or "x-amz-date" from SignedHeaders are rejected with
// AuthorizationHeaderMalformed.
func TestSigV4AuthMiddleware_RequireSignedHeaders(t *testing.T) {
	s3Config := &Config{
		Region: "ap-southeast-2",
		Buckets: []S3_Buckets{{
			Name:     "test-bucket01",
			Region:   "ap-southeast-2",
			Type:     "distributed",
			Pathname: "testdata/test-bucket01",
			Public:   false,
		}},
		Auth: []AuthEntry{{
			AccessKeyID:     "TESTACCESSKEY",
			SecretAccessKey: "TESTSECRETKEY",
			Policy: []PolicyRule{{
				Bucket:  "test-bucket01",
				Actions: []string{"s3:GetObject", "s3:ListBucket"},
			}},
		}},
	}
	server := NewHTTP2ServerWithBackend(s3Config, nil, NewConfigProvider(s3Config.Auth))

	rewriteSignedHeaders := func(req *http.Request, list string) {
		ah := req.Header.Get("Authorization")
		parts := strings.Split(ah, ", ")
		if len(parts) != 3 {
			t.Fatalf("expected 3-part auth header, got %d", len(parts))
		}
		parts[1] = "SignedHeaders=" + list
		req.Header.Set("Authorization", strings.Join(parts, ", "))
	}

	tests := []struct {
		name       string
		signedList string
	}{
		{"missing host", "x-amz-date"},
		{"missing x-amz-date", "host"},
		{"neither present", "content-type"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/test-bucket01", nil)
			signTestReq(t, req, nil, "TESTACCESSKEY", "TESTSECRETKEY", "ap-southeast-2", "s3")
			rewriteSignedHeaders(req, tt.signedList)

			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Fatal("next handler must not be invoked when SignedHeaders guard fires")
			})
			rr := httptest.NewRecorder()
			server.sigV4AuthMiddleware(next).ServeHTTP(rr, req)

			assert.Equal(t, http.StatusForbidden, rr.Code)
			assert.Contains(t, rr.Body.String(), "AuthorizationHeaderMalformed")
		})
	}
}
