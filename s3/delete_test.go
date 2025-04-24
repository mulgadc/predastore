package s3

import (
	"bytes"
	"encoding/xml"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDeleteObject(t *testing.T) {
	s3 := New()
	err := s3.ReadConfig(filepath.Join("tests", "config", "server.toml"), "")
	assert.NoError(t, err, "Should read config without error")

	// Setup Fiber app using SetupRoutes
	app := s3.SetupRoutes()

	// Create test content to upload
	testContent := []byte("This is a test file created during unit testing")

	// Make a PUT request
	req := httptest.NewRequest("PUT", "/local/test_delete.txt", bytes.NewReader(testContent))

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

	// Send a delete request
	req = httptest.NewRequest("DELETE", "/local/test_delete.txt", nil)

	// Add authentication headers using the credentials from server.toml
	if len(s3.Auth) > 0 {
		// Use the first auth entry from the config
		authEntry := s3.Auth[0]

		// Use our utility function to generate a valid authorization header
		timestamp := time.Now().UTC().Format("20060102T150405Z")

		err := GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, s3.Region, "s3", req)
		assert.NoError(t, err, "Error generating auth header")
	}

	resp, err = app.Test(req)

	assert.NoError(t, err, "Request should not error")
	assert.Equal(t, 204, resp.StatusCode, "Status code should be 204")
}

func TestDeleteObjectRemovePath(t *testing.T) {
	s3 := New()
	err := s3.ReadConfig(filepath.Join("tests", "config", "server.toml"), "")
	assert.NoError(t, err, "Should read config without error")

	// Setup Fiber app using SetupRoutes
	app := s3.SetupRoutes()

	// Create test content to upload
	testContent := []byte("This is a test file created during unit testing")

	// Make a PUT request
	req := httptest.NewRequest("PUT", "/local/folder1/folder2/data/test_delete.txt", bytes.NewReader(testContent))

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

	// Next, create another object in the first directory
	// Make a PUT request
	req = httptest.NewRequest("PUT", "/local/folder1/sample.txt", bytes.NewReader(testContent))

	// Add authentication headers using the credentials from server.toml
	if len(s3.Auth) > 0 {
		// Use the first auth entry from the config
		authEntry := s3.Auth[0]

		// Use our utility function to generate a valid authorization header
		timestamp := time.Now().UTC().Format("20060102T150405Z")

		err := GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, s3.Region, "s3", req)
		assert.NoError(t, err, "Error generating auth header")
	}

	resp, err = app.Test(req)

	assert.NoError(t, err, "Request should not error")
	assert.Equal(t, 200, resp.StatusCode, "Status code should be 200")

	// Send a delete request for the first object
	req = httptest.NewRequest("DELETE", "/local/folder1/folder2/data/test_delete.txt", nil)

	resp, err = app.Test(req)

	assert.NoError(t, err, "Request should error no authentication")
	assert.Equal(t, 403, resp.StatusCode, "Status code should be 403")

	// Add authentication headers using the credentials
	if len(s3.Auth) > 0 {
		// Use the first auth entry from the config
		authEntry := s3.Auth[0]

		// Use our utility function to generate a valid authorization header
		timestamp := time.Now().UTC().Format("20060102T150405Z")

		err := GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, s3.Region, "s3", req)
		assert.NoError(t, err, "Error generating auth header")
	}

	resp, err = app.Test(req)

	assert.NoError(t, err, "Request should error no authentication")
	assert.Equal(t, 204, resp.StatusCode, "Status code should be 204")

	// Next, confirm the folder hierarchy is removed
	req = httptest.NewRequest("GET", "/local/folder1/folder2/data/", nil)

	if len(s3.Auth) > 0 {
		// Use the first auth entry from the config
		authEntry := s3.Auth[0]

		// Use our utility function to generate a valid authorization header
		timestamp := time.Now().UTC().Format("20060102T150405Z")

		err := GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, s3.Region, "s3", req)
		assert.NoError(t, err, "Error generating auth header")
	}

	resp, err = app.Test(req)

	assert.NoError(t, err, "Request should not error")

	assert.Equal(t, 404, resp.StatusCode, "Status code should be 404")

	// Next, confirm the /local/folder1/sample.txt object is still there
	req = httptest.NewRequest("GET", "/local?list-type=2&prefix=folder1%2F&delimiter=%2F&encoding-type=url", nil)

	if len(s3.Auth) > 0 {
		// Use the first auth entry from the config
		authEntry := s3.Auth[0]

		// Use our utility function to generate a valid authorization header
		timestamp := time.Now().UTC().Format("20060102T150405Z")

		err := GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, s3.Region, "s3", req)
		assert.NoError(t, err, "Error generating auth header")
	}

	resp, err = app.Test(req)

	assert.NoError(t, err, "Request should not error")
	assert.Equal(t, 200, resp.StatusCode, "Status code should be 200")

	// Parse the XML response
	var result ListObjectsV2
	err = xml.NewDecoder(resp.Body).Decode(&result)
	assert.NoError(t, err, "XML parsing should not error")

	// Verify response
	assert.Equal(t, "local", result.Name, "Bucket name should match")
	assert.NotNil(t, result.Contents, "Contents should not be nil")

	assert.Equal(t, 1, len(*result.Contents), "Contents should have 1 item")
	assert.Equal(t, "sample.txt", (*result.Contents)[0].Key, "Key should match")

}
