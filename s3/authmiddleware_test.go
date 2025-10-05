package s3

import (
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
)

const host = "example.com"

func TestSigV4AuthMiddleware(t *testing.T) {
	// Create a test S3 config with known auth credentials
	s3Config := &Config{
		Region: "ap-southeast-2",
		Auth: []AuthEntry{
			{
				AccessKeyID:     "TESTACCESSKEY",
				SecretAccessKey: "TESTSECRETKEY",
				Policy: []PolicyRule{
					{
						Bucket:  "testbucket",
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
			path:   "/testbucket",
			setupHeaders: func(req *http.Request) {
				// No auth header
			},
			expectStatus:   fiber.StatusForbidden,
			expectResponse: "Missing Authorization header",
			query:          url.Values{}, // add S3 query param
		},

		{
			name:   "Valid Signature",
			method: "GET",
			path:   "/testbucket",
			setupHeaders: func(req *http.Request) {
				// Use our utility function to generate a valid authorization header
				timestamp := time.Now().UTC().Format("20060102T150405Z")

				// Generate valid auth header
				err := GenerateAuthHeaderReq(
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
			expectStatus:   fiber.StatusOK,
			expectResponse: "Request authenticated",
		},

		{
			name:   "Valid Signature Wrong Region",
			method: "GET",
			path:   "/testbucket",
			setupHeaders: func(req *http.Request) {
				// Use our utility function to generate a valid authorization header
				timestamp := time.Now().UTC().Format("20060102T150405Z")

				// Generate valid auth header
				err := GenerateAuthHeaderReq(
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
			expectStatus:   fiber.StatusForbidden,
			expectResponse: "Request authenticated",
		},

		{
			name:   "Invalid Access Key",
			method: "GET",
			path:   "/testbucket",
			setupHeaders: func(req *http.Request) {
				timestamp := time.Now().UTC().Format("20060102T150405Z")

				// Generate auth header with invalid access key
				err := GenerateAuthHeaderReq(
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
			expectStatus:   fiber.StatusForbidden,
			expectResponse: "Invalid access key",
		},
		{
			name:   "Invalid Signature",
			method: "GET",
			path:   "/testbucket",
			setupHeaders: func(req *http.Request) {
				timestamp := time.Now().UTC().Format("20060102T150405Z")

				// Generate valid header first
				err := GenerateAuthHeaderReq(
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
			expectStatus:   fiber.StatusForbidden,
			expectResponse: "Invalid signature",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a new Fiber app
			app := fiber.New(fiber.Config{

				// Set the body limit for S3 specs to 5GiB
				BodyLimit: 5 * 1024 * 1024 * 1024,

				// Override default error handler
				ErrorHandler: func(ctx *fiber.Ctx, err error) error {
					return s3Config.ErrorHandler(ctx, err)
				}})

			// Add middleware and test endpoint
			app.Use(s3Config.SigV4AuthMiddleware)
			app.All("*", func(c *fiber.Ctx) error {

				return c.SendString("Request authenticated")
			})

			// Create request
			req := httptest.NewRequest(tt.method, tt.path, nil)
			if tt.setupHeaders != nil {
				tt.setupHeaders(req)
			}

			// Perform request
			resp, err := app.Test(req)
			assert.NoError(t, err)

			// Check status code
			assert.Equal(t, tt.expectStatus, resp.StatusCode)

			// If we're expecting a specific response, check it
			/*
				if tt.expectResponse != "" {
					body := make([]byte, 1024)
					n, err := resp.Body.Read(body)
					assert.NoError(t, err, "Error reading response body")
					assert.Contains(t, string(body[:n]), tt.expectResponse)
				}
			*/

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
	signingKey := getSigningKey(secret, date, region, service)
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
	hmac := hmacSHA256(key, data)
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
	actualHash := hashSHA256(data)

	// Verify the hash matches the expected value
	assert.Equal(t, expectedHash, actualHash)
}
