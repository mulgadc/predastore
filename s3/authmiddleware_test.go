package s3

import (
	"bytes"
	"encoding/hex"
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
		setupHeaders   func(req *http.Request)
		query          url.Values
		expectStatus   int
		expectResponse string
	}{
		{
			name:   "No Auth Header",
			method: "GET",
			path:   "/test-bucket01",
			setupHeaders: func(req *http.Request) {
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
			setupHeaders: func(req *http.Request) {
				// Use our utility function to generate a valid authorization header
				timestamp := time.Now().UTC().Format("20060102T150405Z")

				// Generate valid auth header
				err := auth.GenerateAuthHeaderReq(
					"TESTACCESSKEY",
					"TESTSECRETKEY",
					timestamp,
					"ap-southeast-2",
					"s3",
					req,
				)

				if err != nil {
					assert.Fail(t, "Error generating auth header: %v", err)
				}
			},
			expectStatus:   -1, // sentinel: assert NOT 403 (auth passed, backend is nil so handler may 500)
			expectResponse: "",
		},

		{
			name:   "Valid Signature Wrong Region",
			method: "GET",
			path:   "/test-bucket01",
			setupHeaders: func(req *http.Request) {
				// Use our utility function to generate a valid authorization header
				timestamp := time.Now().UTC().Format("20060102T150405Z")

				// Generate valid auth header
				err := auth.GenerateAuthHeaderReq(
					"TESTACCESSKEY",
					"TESTSECRETKEY",
					timestamp,
					"ap-southeast-1",
					"s3",
					req,
				)

				if err != nil {
					assert.Fail(t, "Error generating auth header: %v", err)
				}
			},
			expectStatus:   http.StatusForbidden,
			expectResponse: "Request authenticated",
		},

		{
			name:   "Invalid Access Key",
			method: "GET",
			path:   "/test-bucket01",
			setupHeaders: func(req *http.Request) {
				timestamp := time.Now().UTC().Format("20060102T150405Z")

				// Generate auth header with invalid access key
				err := auth.GenerateAuthHeaderReq(
					"INVALIDACCESSKEY",
					"TESTSECRETKEY",
					timestamp,
					"ap-southeast-2",
					"s3",
					req,
				)

				if err != nil {
					assert.Fail(t, "Error generating auth header: %v", err)
				}
			},
			expectStatus:   http.StatusForbidden,
			expectResponse: "Invalid access key",
		},
		{
			name:   "Invalid Signature",
			method: "GET",
			path:   "/test-bucket01",
			setupHeaders: func(req *http.Request) {
				timestamp := time.Now().UTC().Format("20060102T150405Z")

				// Generate valid header first
				err := auth.GenerateAuthHeaderReq(
					"TESTACCESSKEY",
					"WRONGSECRETKEY", // Wrong secret key
					timestamp,
					"ap-southeast-2",
					"s3",
					req,
				)

				if err != nil {
					assert.Fail(t, "Error generating auth header: %v", err)
				}
			},
			expectStatus:   http.StatusForbidden,
			expectResponse: "Invalid signature",
		},
		{
			name:   "Expired Timestamp - Too Old",
			method: "GET",
			path:   "/test-bucket01",
			setupHeaders: func(req *http.Request) {
				// 6 minutes in the past — exceeds the 5-minute maxClockSkew
				timestamp := time.Now().UTC().Add(-6 * time.Minute).Format(auth.TimeFormat)
				err := auth.GenerateAuthHeaderReq(
					"TESTACCESSKEY", "TESTSECRETKEY",
					timestamp, "ap-southeast-2", "s3", req,
				)
				if err != nil {
					assert.Fail(t, "Error generating auth header: %v", err)
				}
			},
			expectStatus:   http.StatusForbidden,
			expectResponse: "RequestTimeTooSkewed",
		},
		{
			name:   "Expired Timestamp - Too Far In Future",
			method: "GET",
			path:   "/test-bucket01",
			setupHeaders: func(req *http.Request) {
				// 6 minutes in the future
				timestamp := time.Now().UTC().Add(6 * time.Minute).Format(auth.TimeFormat)
				err := auth.GenerateAuthHeaderReq(
					"TESTACCESSKEY", "TESTSECRETKEY",
					timestamp, "ap-southeast-2", "s3", req,
				)
				if err != nil {
					assert.Fail(t, "Error generating auth header: %v", err)
				}
			},
			expectStatus:   http.StatusForbidden,
			expectResponse: "RequestTimeTooSkewed",
		},
		{
			name:   "Missing X-Amz-Date Header",
			method: "GET",
			path:   "/test-bucket01",
			setupHeaders: func(req *http.Request) {
				timestamp := time.Now().UTC().Format(auth.TimeFormat)
				err := auth.GenerateAuthHeaderReq(
					"TESTACCESSKEY", "TESTSECRETKEY",
					timestamp, "ap-southeast-2", "s3", req,
				)
				if err != nil {
					assert.Fail(t, "Error generating auth header: %v", err)
				}
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
			setupHeaders: func(req *http.Request) {
				timestamp := time.Now().UTC().Format(auth.TimeFormat)
				err := auth.GenerateAuthHeaderReq(
					"TESTACCESSKEY", "TESTSECRETKEY",
					timestamp, "ap-southeast-2", "s3", req,
				)
				if err != nil {
					assert.Fail(t, "Error generating auth header: %v", err)
				}
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
				tt.setupHeaders(req)
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

// TestSigV4AuthMiddleware_RequireSignedHeaders verifies that requests whose
// SigV4 SignedHeaders list omits "host" or "x-amz-date" are rejected with
// AuthorizationHeaderMalformed. AWS SDKs always sign both; omitting either
// would let a captured Authorization header replay against a different vhost
// or outside the X-Amz-Date skew window.
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
			ts := time.Now().UTC().Format(auth.TimeFormat)
			err := auth.GenerateAuthHeaderReq("TESTACCESSKEY", "TESTSECRETKEY", ts, "ap-southeast-2", "s3", req)
			assert.NoError(t, err)
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

// TestSigV4AuthMiddleware_OversizeSignedBody verifies that a signed request
// whose body exceeds maxAuthBodySize and does not use UNSIGNED-PAYLOAD or a
// precomputed hash is rejected with 413 EntityTooLarge, not buffered and
// OOMed. This is the pre-auth DoS guard.
func TestSigV4AuthMiddleware_OversizeSignedBody(t *testing.T) {
	s3Config := &Config{
		Region: "ap-southeast-2",
		Buckets: []S3_Buckets{{
			Name:     "test-bucket01",
			Region:   "ap-southeast-2",
			Type:     "fs",
			Pathname: "tests/data/test-bucket01",
			Public:   false,
		}},
		Auth: []AuthEntry{{
			AccessKeyID:     "TESTACCESSKEY",
			SecretAccessKey: "TESTSECRETKEY",
			Policy: []PolicyRule{{
				Bucket:  "test-bucket01",
				Actions: []string{"s3:PutObject"},
			}},
		}},
	}

	server := NewHTTP2Server(s3Config)

	// Body 1 byte over the cap — exercises the MaxBytesReader reject path
	// without allocating an unreasonable amount of memory in tests.
	body := strings.Repeat("A", maxAuthBodySize+1)
	req := httptest.NewRequest(http.MethodPut, "/test-bucket01/big-object", bytes.NewReader([]byte(body)))

	timestamp := time.Now().UTC().Format(auth.TimeFormat)
	err := auth.GenerateAuthHeaderReq(
		"TESTACCESSKEY", "TESTSECRETKEY",
		timestamp, "ap-southeast-2", "s3", req,
	)
	assert.NoError(t, err)
	// Intentionally do not set X-Amz-Content-Sha256 — leaves middleware on
	// the default (read-body) branch.

	rr := httptest.NewRecorder()
	server.GetHandler().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusRequestEntityTooLarge, rr.Code)
	assert.Contains(t, rr.Body.String(), "EntityTooLarge")
}

// TestGetSigningKey verifies the key derivation process
func TestGetSigningKey(t *testing.T) {
	// Test values from AWS documentation
	secret := "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
	date := "20150830"
	region := "us-east-1"
	service := "iam"

	// Expected signing key (hex-encoded)
	// This value is from the AWS Signature V4 documentation example
	expectedKey := "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9"

	// Generate the signing key
	signingKey := auth.GetSigningKey(secret, date, region, service)
	actualKey := hex.EncodeToString(signingKey)

	// Verify the key matches the expected value
	assert.Equal(t, expectedKey, actualKey)
}

// TestHmacSHA256 tests the HMAC-SHA256 function
func TestHmacSHA256(t *testing.T) {
	key := []byte("key")
	data := "The quick brown fox jumps over the lazy dog"

	// Expected HMAC-SHA256 (hex-encoded)
	expectedHmac := "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"

	// Calculate HMAC-SHA256
	hmac := auth.HmacSHA256(key, data)
	actualHmac := hex.EncodeToString(hmac)

	// Verify the HMAC matches the expected value
	assert.Equal(t, expectedHmac, actualHmac)
}

// TestHashSHA256 tests the SHA256 hash function
func TestHashSHA256(t *testing.T) {
	data := "The quick brown fox jumps over the lazy dog"

	// Expected SHA256 (hex-encoded)
	expectedHash := "d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592"

	// Calculate SHA256
	actualHash := auth.HashSHA256(data)

	// Verify the hash matches the expected value
	assert.Equal(t, expectedHash, actualHash)
}
