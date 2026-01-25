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

	"github.com/mulgadc/predastore/auth"
	"github.com/stretchr/testify/assert"
)

func TestPutObject(t *testing.T) {
	config := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := config.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	server := NewHTTP2Server(config)

	// Create test content to upload
	testContent := []byte("This is a test file created during unit testing")

	// Make a PUT request
	req := httptest.NewRequest("PUT", "/test-bucket01/test_upload.txt", bytes.NewReader(testContent))

	// Add authentication headers using the credentials from server.toml
	if len(config.Auth) > 0 {
		authEntry := config.Auth[0]
		timestamp := time.Now().UTC().Format("20060102T150405Z")

		err := auth.GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, config.Region, "s3", req)
		assert.NoError(t, err, "Error generating auth header")
	}

	rr := httptest.NewRecorder()
	server.GetHandler().ServeHTTP(rr, req)

	assert.Equal(t, 200, rr.Code, "Status code should be 200")

	// Verify the file was created with the correct content
	uploadedPath := filepath.Join(config.Buckets[0].Pathname, "test_upload.txt")
	defer os.Remove(uploadedPath) // Clean up after test

	// Check if file exists
	_, err = os.Stat(uploadedPath)
	assert.NoError(t, err, "File should exist after upload")

	// Check file content
	content, err := os.ReadFile(uploadedPath)
	assert.NoError(t, err, "Should be able to read uploaded file")
	assert.Equal(t, testContent, content, "Uploaded content should match")
}

// Test put object to a private bucket, with no auth

func TestPutObjectPublicBucketNoAuth(t *testing.T) {
	config := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := config.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	server := NewHTTP2Server(config)

	// Create test content to upload
	testContent := []byte("This is a test file created during unit testing")

	// Make a PUT request
	req := httptest.NewRequest("PUT", "/test-bucket01/test_upload.txt", bytes.NewReader(testContent))

	rr := httptest.NewRecorder()
	server.GetHandler().ServeHTTP(rr, req)

	assert.Equal(t, 403, rr.Code, "Status code should be 403")
}

func TestPutObjectPrivateBucketNoAuth(t *testing.T) {
	config := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := config.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	server := NewHTTP2Server(config)

	// Create test content to upload
	testContent := []byte("This is a test file created during unit testing")

	// Make a PUT request
	req := httptest.NewRequest("PUT", "/private/test_upload.txt", bytes.NewReader(testContent))

	rr := httptest.NewRecorder()
	server.GetHandler().ServeHTTP(rr, req)

	assert.Equal(t, 403, rr.Code, "Status code should be 403")
}

func TestPutObjectBinary(t *testing.T) {
	config := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := config.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	server := NewHTTP2Server(config)

	// Create binary test content (10 KB of random data)
	testContent := make([]byte, 10240)
	for i := range testContent {
		testContent[i] = byte(i % 256)
	}

	// Make a PUT request
	req := httptest.NewRequest("PUT", "/test-bucket01/test_binary_upload.dat", bytes.NewReader(testContent))

	// Add authentication headers using the credentials from server.toml
	if len(config.Auth) > 0 {
		authEntry := config.Auth[0]
		timestamp := time.Now().UTC().Format("20060102T150405Z")

		err := auth.GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, config.Region, "s3", req)
		assert.NoError(t, err, "Error generating auth header")
	}

	rr := httptest.NewRecorder()
	server.GetHandler().ServeHTTP(rr, req)

	assert.Equal(t, 200, rr.Code, "Status code should be 200")

	// Verify the file was created with the correct content
	uploadedPath := filepath.Join(config.Buckets[0].Pathname, "test_binary_upload.dat")
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
	config := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := config.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	server := NewHTTP2Server(config)

	// Create test content to upload
	testContent := []byte("This is a test file for an invalid bucket")

	// Make a PUT request to a non-existent bucket
	req := httptest.NewRequest("PUT", "/nonexistent/test_upload.txt", bytes.NewReader(testContent))

	// Add authentication headers using the credentials from server.toml
	if len(config.Auth) > 0 {
		authEntry := config.Auth[0]
		timestamp := time.Now().UTC().Format("20060102T150405Z")

		err := auth.GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, config.Region, "s3", req)
		assert.NoError(t, err, "Error generating auth header")
	}

	rr := httptest.NewRecorder()
	server.GetHandler().ServeHTTP(rr, req)

	assert.Equal(t, 404, rr.Code, "Status code should be 404")

	// Read response body
	body, err := io.ReadAll(rr.Body)
	assert.NoError(t, err, "Reading body should not error")
	var s3error S3Error

	err = xml.Unmarshal(body, &s3error)

	assert.NoError(t, err, "XML parsing failed")

	assert.Equal(t, s3error.Code, "NoSuchBucket", "Error message should indicate invalid bucket")
	assert.Equal(t, s3error.Message, "The specified bucket does not exist")
}

// TODO: Add multipart upload tests
