package s3

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mulgadc/predastore/auth"
	"github.com/stretchr/testify/assert"
)

func setupTestServer(t *testing.T) *HTTP2Server {
	config := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := config.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	return NewHTTP2Server(config)
}

func TestGetObjectHead(t *testing.T) {
	server := setupTestServer(t)

	// Make a HEAD request
	req := httptest.NewRequest("HEAD", "/test-bucket01/test.txt", nil)
	rr := httptest.NewRecorder()
	server.GetHandler().ServeHTTP(rr, req)

	assert.Equal(t, 200, rr.Code, "Status code should be 200")

	// Check response headers
	assert.NotEmpty(t, rr.Header().Get("Content-Type"), "Content-Type should be set")
	assert.NotEmpty(t, rr.Header().Get("Content-Length"), "Content-Length should be set")
	assert.NotEmpty(t, rr.Header().Get("Last-Modified"), "Last-Modified should be set")
	assert.NotEmpty(t, rr.Header().Get("ETag"), "ETag should be set")

	// The response body for HEAD should be empty
	body := rr.Body.Bytes()
	assert.Empty(t, body, "HEAD response body should be empty")
}

func TestGetObjectNoBucketPermissions(t *testing.T) {
	server := setupTestServer(t)

	// Make a GET request
	req := httptest.NewRequest("GET", "/private/note.txt", nil)

	// Use our utility function to generate a valid authorization header
	timestamp := time.Now().UTC().Format("20060102T150405Z")

	err := auth.GenerateAuthHeaderReq("BADACCESSKEY", "BADSECRETKEY", timestamp, "us-east-1", "s3", req)
	assert.NoError(t, err, "Error generating auth header")

	rr := httptest.NewRecorder()
	server.GetHandler().ServeHTTP(rr, req)

	assert.Equal(t, 403, rr.Code, "Status code should be 403")

	// The response body should include the error message
	body := rr.Body.Bytes()

	var s3error S3Error
	err = xml.Unmarshal(body, &s3error)
	assert.NoError(t, err, "XML parsing failed")

	assert.Equal(t, "AccessDenied", s3error.Code, "Error message should indicate access denied")
}

func TestGetObjectBucketPermissions(t *testing.T) {
	server := setupTestServer(t)
	config := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := config.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	// Make a GET request
	req := httptest.NewRequest("GET", "/private/note.txt", nil)

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

	// Check the content matches the file
	body := rr.Body.Bytes()
	assert.Equal(t, "Hello World!\n", string(body), "File content should match")
}

// Test Get Object with auth header to public bucket
func TestGetObjectPublicBucketAuthHeader(t *testing.T) {
	server := setupTestServer(t)
	config := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := config.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	// Make a GET request for the text file
	req := httptest.NewRequest("GET", "/test-bucket01/test.txt", nil)

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

	// Check the content matches the file
	body := rr.Body.Bytes()

	// Compare with actual file content
	expected, err := os.ReadFile(filepath.Join("tests", "data", "test-bucket01", "test.txt"))
	assert.NoError(t, err, "Reading test file should not error")
	assert.Equal(t, expected, body, "File content should match")
}

func TestGetObject(t *testing.T) {
	server := setupTestServer(t)

	// Make a GET request for the text file
	req := httptest.NewRequest("GET", "/test-bucket01/test.txt", nil)
	rr := httptest.NewRecorder()
	server.GetHandler().ServeHTTP(rr, req)

	assert.Equal(t, 200, rr.Code, "Status code should be 200")

	// Check the content matches the file
	body := rr.Body.Bytes()

	// Compare with actual file content
	expected, err := os.ReadFile(filepath.Join("tests", "data", "test-bucket01", "test.txt"))
	assert.NoError(t, err, "Reading test file should not error")
	assert.Equal(t, expected, body, "File content should match")
}

func TestGetObjectWithRange(t *testing.T) {
	server := setupTestServer(t)

	// Make a GET request with a Range header
	req := httptest.NewRequest("GET", "/test-bucket01/test.txt", nil)
	req.Header.Set("Range", "bytes=0-9") // Get the first 10 bytes
	rr := httptest.NewRecorder()
	server.GetHandler().ServeHTTP(rr, req)

	assert.Equal(t, 206, rr.Code, "Status code should be 206 for partial content")

	// Check the content matches the expected range
	body := rr.Body.Bytes()

	// Compare with actual file content range
	expectedFile, err := os.ReadFile(filepath.Join("tests", "data", "test-bucket01", "test.txt"))
	assert.NoError(t, err, "Reading test file should not error")

	expected := expectedFile[:10]
	assert.Equal(t, expected, body, "Partial content should match requested range")

	// The response should have appropriate headers
	assert.NotEmpty(t, rr.Header().Get("Content-Range"), "Content-Range header should be set")
}

func TestGetObjectNonExistent(t *testing.T) {
	server := setupTestServer(t)

	// Make a GET request for a non-existent file
	req := httptest.NewRequest("GET", "/test-bucket01/nonexistent.txt", nil)
	rr := httptest.NewRecorder()
	server.GetHandler().ServeHTTP(rr, req)

	assert.Equal(t, 404, rr.Code, "Status code should be 404 for non-existent file")
}

func TestGetInvalidBucket(t *testing.T) {
	config := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := config.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	server := NewHTTP2Server(config)

	// Make a request to a non-existent bucket
	req := httptest.NewRequest("GET", "/invalidbucket/file.txt", nil)

	// Add authentication headers - needed to get NoSuchBucket instead of AccessDenied
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

	assert.Equal(t, "NoSuchBucket", s3error.Code, "Error message should indicate invalid bucket")
	assert.Equal(t, "The specified bucket does not exist", s3error.Message)
}

func TestGetInvalidObjectKey(t *testing.T) {
	server := setupTestServer(t)

	// Make a request to list objects in the test bucket with an invalid key
	req := httptest.NewRequest("GET", fmt.Sprintf("/test-bucket01/%s", string([]byte{0x80, 0x80})), nil)
	rr := httptest.NewRecorder()
	server.GetHandler().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code, "Status code should be 400 for invalid key")

	// Read response body
	body, err := io.ReadAll(rr.Body)
	assert.NoError(t, err, "Reading body should not error")
	var s3error S3Error

	err = xml.Unmarshal(body, &s3error)

	assert.NoError(t, err, "XML parsing failed")

	assert.Equal(t, "InvalidKey", s3error.Code, "Error code should indicate invalid key")
}
