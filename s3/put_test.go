package s3

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPutObject(t *testing.T) {
	s3 := New()
	err := s3.ReadConfig("../tests/config/server.toml")
	assert.NoError(t, err, "Should read config without error")

	// Setup Fiber app using SetupRoutes
	app := s3.SetupRoutes()

	// Create test content to upload
	testContent := []byte("This is a test file created during unit testing")

	// Make a PUT request
	req := httptest.NewRequest("PUT", "/testbucket/test_upload.txt", bytes.NewReader(testContent))

	// Add authentication headers using the credentials from server.toml
	if len(s3.Auth) > 0 {
		// Use the first auth entry from the config
		authEntry := s3.Auth[0]

		// Use our utility function to generate a valid authorization header
		timestamp := time.Now().UTC().Format("20060102T150405Z")

		err := GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, s3.Region, "s3", req)
		assert.NoError(t, err, "Error generating auth header")
	}

	resp, err := app.Test(req)

	assert.NoError(t, err, "Request should not error")
	assert.Equal(t, 200, resp.StatusCode, "Status code should be 200")

	// Verify the file was created with the correct content
	uploadedPath := filepath.Join(s3.Buckets[0].Pathname, "test_upload.txt")
	defer os.Remove(uploadedPath) // Clean up after test

	// Check if file exists
	_, err = os.Stat(uploadedPath)
	assert.NoError(t, err, "File should exist after upload")

	// Check file content
	content, err := os.ReadFile(uploadedPath)
	assert.NoError(t, err, "Should be able to read uploaded file")
	assert.Equal(t, testContent, content, "Uploaded content should match")
}

func TestPutObjectBinary(t *testing.T) {
	s3 := New()
	err := s3.ReadConfig("../tests/config/server.toml")
	assert.NoError(t, err, "Should read config without error")

	// Setup Fiber app using SetupRoutes
	app := s3.SetupRoutes()

	// Create binary test content (10 KB of random data)
	testContent := make([]byte, 10240)
	for i := range testContent {
		testContent[i] = byte(i % 256)
	}

	// Make a PUT request
	req := httptest.NewRequest("PUT", "/testbucket/test_binary_upload.dat", bytes.NewReader(testContent))

	// Add authentication headers using the credentials from server.toml
	if len(s3.Auth) > 0 {
		// Use the first auth entry from the config
		authEntry := s3.Auth[0]

		// Use our utility function to generate a valid authorization header
		timestamp := time.Now().UTC().Format("20060102T150405Z")

		err := GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, s3.Region, "s3", req)
		assert.NoError(t, err, "Error generating auth header")
	}

	resp, err := app.Test(req)

	assert.NoError(t, err, "Request should not error")
	assert.Equal(t, 200, resp.StatusCode, "Status code should be 200")

	// Verify the file was created with the correct content
	uploadedPath := filepath.Join(s3.Buckets[0].Pathname, "test_binary_upload.dat")
	defer os.Remove(uploadedPath) // Clean up after test

	// Check if file exists
	_, err = os.Stat(uploadedPath)
	assert.NoError(t, err, "File should exist after upload")

	// Check file content
	content, err := os.ReadFile(uploadedPath)
	assert.NoError(t, err, "Should be able to read uploaded file")
	assert.Equal(t, testContent, content, "Uploaded binary content should match")
}

func TestPutObjectInvalidBucket(t *testing.T) {
	s3 := New()
	err := s3.ReadConfig("../tests/config/server.toml")
	assert.NoError(t, err, "Should read config without error")

	// Setup Fiber app using SetupRoutes
	app := s3.SetupRoutes()

	// Create test content to upload
	testContent := []byte("This is a test file for an invalid bucket")

	// Make a PUT request to a non-existent bucket
	req := httptest.NewRequest("PUT", "/nonexistent/test_upload.txt", bytes.NewReader(testContent))

	// Add authentication headers using the credentials from server.toml
	if len(s3.Auth) > 0 {
		// Use the first auth entry from the config
		authEntry := s3.Auth[0]

		// Use our utility function to generate a valid authorization header
		timestamp := time.Now().UTC().Format("20060102T150405Z")

		err := GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, s3.Region, "s3", req)
		assert.NoError(t, err, "Error generating auth header")
	}

	resp, err := app.Test(req)

	assert.NoError(t, err, "Request should complete")
	assert.Equal(t, 404, resp.StatusCode, "Status code should be 404")

	// Read response body
	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err, "Reading body should not error")
	var s3error S3Error

	err = xml.Unmarshal(body, &s3error)

	assert.NoError(t, err, "XML parsing failed")

	assert.Equal(t, s3error.Code, "NoSuchBucket", "Error message should indicate invalid bucket")
	assert.Equal(t, s3error.Message, "The specified bucket does not exist")

}
