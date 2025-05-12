package s3

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetObjectHead(t *testing.T) {
	s3 := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := s3.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	// Setup Fiber app using SetupRoutes
	app := s3.SetupRoutes()

	// Make a HEAD request
	req := httptest.NewRequest("HEAD", "/testbucket/test.txt", nil)
	resp, err := app.Test(req)

	assert.NoError(t, err, "Request should not error")
	assert.Equal(t, 200, resp.StatusCode, "Status code should be 200")

	// Check response headers
	assert.NotEmpty(t, resp.Header.Get("Content-Type"), "Content-Type should be set")
	assert.NotEmpty(t, resp.Header.Get("Content-Length"), "Content-Length should be set")
	assert.NotEmpty(t, resp.Header.Get("Last-Modified"), "Last-Modified should be set")
	assert.NotEmpty(t, resp.Header.Get("ETag"), "ETag should be set")

	// The response body for HEAD should be empty
	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err, "Reading body should not error")
	assert.Empty(t, body, "HEAD response body should be empty")
}

func TestGetObjectNoBucketPermissions(t *testing.T) {
	s3 := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := s3.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	// Setup Fiber app using SetupRoutes
	app := s3.SetupRoutes()

	// Make a HEAD request
	req := httptest.NewRequest("GET", "/private/note.txt", nil)

	// Use our utility function to generate a valid authorization header
	timestamp := time.Now().UTC().Format("20060102T150405Z")

	err = GenerateAuthHeaderReq("BADACCESSKEY", "BADSECRETKEY", timestamp, s3.Region, "s3", req)
	assert.NoError(t, err, "Error generating auth header")

	resp, err := app.Test(req)

	assert.NoError(t, err, "Request should not error")
	assert.Equal(t, 403, resp.StatusCode, "Status code should be 403")
	// The response body should include the error message
	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err, "Reading body should not error")

	var s3error S3Error

	err = xml.Unmarshal(body, &s3error)

	assert.NoError(t, err, "XML parsing failed")

	assert.Equal(t, s3error.Code, "AccessDenied", "Error message should indicate access denied")

}

func TestGetObjectBucketPermissions(t *testing.T) {
	s3 := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := s3.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	// Setup Fiber app using SetupRoutes
	app := s3.SetupRoutes()

	// Make a HEAD request
	req := httptest.NewRequest("GET", "/private/note.txt", nil)

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

	// Check the content matches the file
	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err, "Reading body should not error")

	assert.Equal(t, "Hello World!\n", string(body), "File content should match")

}

// Test Get Object with auth header to public bucket
func TestGetObjectPublicBucketAuthHeader(t *testing.T) {
	s3 := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := s3.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	// Setup Fiber app using SetupRoutes
	app := s3.SetupRoutes()

	// Make a GET request for the text file
	req := httptest.NewRequest("GET", "/testbucket/test.txt", nil)

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

	// Check the content matches the file
	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err, "Reading body should not error")

	// Compare with actual file content
	expected, err := os.ReadFile(filepath.Join("tests", "data", "testbucket", "test.txt"))
	assert.NoError(t, err, "Reading test file should not error")
	assert.Equal(t, expected, body, "File content should match")
}

func TestGetObject(t *testing.T) {
	s3 := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := s3.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	// Setup Fiber app using SetupRoutes
	app := s3.SetupRoutes()

	// Make a GET request for the text file
	req := httptest.NewRequest("GET", "/testbucket/test.txt", nil)
	resp, err := app.Test(req)

	assert.NoError(t, err, "Request should not error")
	assert.Equal(t, 200, resp.StatusCode, "Status code should be 200")

	// Check the content matches the file
	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err, "Reading body should not error")

	// Compare with actual file content
	expected, err := os.ReadFile(filepath.Join("tests", "data", "testbucket", "test.txt"))
	assert.NoError(t, err, "Reading test file should not error")
	assert.Equal(t, expected, body, "File content should match")
}

func TestGetObjectWithRange(t *testing.T) {
	s3 := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := s3.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	// Setup Fiber app using SetupRoutes
	app := s3.SetupRoutes()

	// Make a GET request with a Range header
	req := httptest.NewRequest("GET", "/testbucket/test.txt", nil)
	req.Header.Set("Range", "bytes=0-9") // Get the first 10 bytes
	resp, err := app.Test(req)

	assert.NoError(t, err, "Request should not error")
	assert.Equal(t, 200, resp.StatusCode, "Status code should be 200")

	// Check the content matches the expected range
	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err, "Reading body should not error")

	// Compare with actual file content range
	expectedFile, err := os.ReadFile(filepath.Join("tests", "data", "testbucket", "test.txt"))
	assert.NoError(t, err, "Reading test file should not error")

	expected := expectedFile[:10]
	assert.Equal(t, expected, body, "Partial content should match requested range")

	// The response should have appropriate headers
	assert.Equal(t, "bytes", resp.Header.Get("Accept-Ranges"), "Accept-Ranges header should be set")
	assert.NotEmpty(t, resp.Header.Get("Content-Range"), "Content-Range header should be set")
}

func TestGetObjectNonExistent(t *testing.T) {
	s3 := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := s3.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	// Setup Fiber app using SetupRoutes
	app := s3.SetupRoutes()

	// Make a GET request for a non-existent file
	req := httptest.NewRequest("GET", "/testbucket/nonexistent.txt", nil)
	resp, err := app.Test(req)

	assert.NoError(t, err, "Request should complete without error")
	assert.Equal(t, 404, resp.StatusCode, "Status code should be 404 for non-existent file")
}

func TestGetInvalidBucket(t *testing.T) {
	s3 := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := s3.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	// Setup Fiber app using SetupRoutes
	app := s3.SetupRoutes()

	// Make a request to list objects in the test bucket
	req := httptest.NewRequest("GET", "/invalidbucket/file.txt", nil)
	resp, err := app.Test(req)

	assert.NoError(t, err, "Request should not error")
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

func TestGetInvalidObjectKey(t *testing.T) {
	s3 := New(&Config{ConfigPath: filepath.Join("tests", "config", "server.toml")})
	err := s3.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	// Setup Fiber app using SetupRoutes
	app := s3.SetupRoutes()

	// Make a request to list objects in the test bucket with an invalid key
	req := httptest.NewRequest("GET", fmt.Sprintf("/testbucket/%s", string([]byte{0x80, 0x80})), nil)
	resp, err := app.Test(req)

	assert.NoError(t, err, "Request should not error")
	assert.Equal(t, 500, resp.StatusCode, "Status code should be 500")

	// Read response body
	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err, "Reading body should not error")
	var s3error S3Error

	err = xml.Unmarshal(body, &s3error)

	assert.NoError(t, err, "XML parsing failed")

	assert.Equal(t, s3error.Message, "InvalidKey", "Error message should indicate invalid key")

}
