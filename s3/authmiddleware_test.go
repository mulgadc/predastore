package s3

import (
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/mulgadc/predastore/auth"
	"github.com/stretchr/testify/assert"
)

const host = "example.com"

func TestSigV4AuthMiddleware(t *testing.T) {
	// Create a test S3 config with known auth credentials
	s3Config := &Config{
		Region: "ap-southeast-2",
		Buckets: []S3_Buckets{
			{
				Name:     "test-bucket01",
				Region:   "ap-southeast-2",
				Type:     "fs",
				Pathname: "tests/data/test-bucket01",
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
			expectStatus:   http.StatusOK,
			expectResponse: "", // Response is XML ListBucketResult, just check status
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create an HTTP/2 server for testing
			server := NewHTTP2Server(s3Config)

			// Create request
			req := httptest.NewRequest(tt.method, tt.path, nil)
			if tt.setupHeaders != nil {
				tt.setupHeaders(req)
			}

			// Perform request
			rr := httptest.NewRecorder()
			server.GetHandler().ServeHTTP(rr, req)

			// Check status code
			assert.Equal(t, tt.expectStatus, rr.Code)
		})
	}
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
