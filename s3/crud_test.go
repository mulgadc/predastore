package s3

import (
	"bytes"
	"encoding/xml"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCRUDWithFilesystem tests basic CRUD operations with filesystem backend
func TestCRUDWithFilesystem(t *testing.T) {
	RunWithBackends(t, FilesystemOnly(), func(t *testing.T, tb *TestBackend) {
		testCRUD(t, tb)
	})
}

// TestCRUDWithDistributed tests basic CRUD operations with distributed backend
// Note: This test requires QUIC servers to be running for full functionality
func TestCRUDWithDistributed(t *testing.T) {
	t.Skip("Distributed backend requires QUIC servers - skipping until infrastructure is ready")
	RunWithBackends(t, DistributedOnly(), func(t *testing.T, tb *TestBackend) {
		testCRUD(t, tb)
	})
}

// testCRUD performs the actual CRUD test against the provided backend
func testCRUD(t *testing.T, tb *TestBackend) {
	t.Helper()

	// Get the bucket name for this backend type
	bucketName := getBucketForBackend(tb.Type)

	// Test PUT
	testContent := []byte("Test content for CRUD operations")
	objectKey := "crud-test-" + time.Now().Format("20060102150405") + ".txt"

	req := httptest.NewRequest("PUT", "/"+bucketName+"/"+objectKey, bytes.NewReader(testContent))

	// Add authentication
	if len(tb.Config.Auth) > 0 {
		authEntry := tb.Config.Auth[0]
		timestamp := time.Now().UTC().Format("20060102T150405Z")
		err := GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, tb.Config.Region, "s3", req)
		require.NoError(t, err, "Error generating auth header")
	}

	resp, err := tb.App.Test(req)
	require.NoError(t, err, "PUT request should not error")
	assert.Equal(t, 200, resp.StatusCode, "PUT should return 200")

	// Test GET
	req = httptest.NewRequest("GET", "/"+bucketName+"/"+objectKey, nil)

	if len(tb.Config.Auth) > 0 {
		authEntry := tb.Config.Auth[0]
		timestamp := time.Now().UTC().Format("20060102T150405Z")
		err := GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, tb.Config.Region, "s3", req)
		require.NoError(t, err, "Error generating auth header")
	}

	resp, err = tb.App.Test(req)
	require.NoError(t, err, "GET request should not error")
	assert.Equal(t, 200, resp.StatusCode, "GET should return 200")

	// Verify content
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(resp.Body)
	require.NoError(t, err, "Reading response body should not error")
	assert.Equal(t, testContent, buf.Bytes(), "Content should match")

	// Test HEAD
	req = httptest.NewRequest("HEAD", "/"+bucketName+"/"+objectKey, nil)

	if len(tb.Config.Auth) > 0 {
		authEntry := tb.Config.Auth[0]
		timestamp := time.Now().UTC().Format("20060102T150405Z")
		err := GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, tb.Config.Region, "s3", req)
		require.NoError(t, err, "Error generating auth header")
	}

	resp, err = tb.App.Test(req)
	require.NoError(t, err, "HEAD request should not error")
	assert.Equal(t, 200, resp.StatusCode, "HEAD should return 200")
	assert.NotEmpty(t, resp.Header.Get("Content-Length"), "Content-Length should be set")

	// Test DELETE
	req = httptest.NewRequest("DELETE", "/"+bucketName+"/"+objectKey, nil)

	if len(tb.Config.Auth) > 0 {
		authEntry := tb.Config.Auth[0]
		timestamp := time.Now().UTC().Format("20060102T150405Z")
		err := GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, tb.Config.Region, "s3", req)
		require.NoError(t, err, "Error generating auth header")
	}

	resp, err = tb.App.Test(req)
	require.NoError(t, err, "DELETE request should not error")
	assert.Equal(t, 204, resp.StatusCode, "DELETE should return 204")

	// Verify object is gone
	req = httptest.NewRequest("GET", "/"+bucketName+"/"+objectKey, nil)

	if len(tb.Config.Auth) > 0 {
		authEntry := tb.Config.Auth[0]
		timestamp := time.Now().UTC().Format("20060102T150405Z")
		err := GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, tb.Config.Region, "s3", req)
		require.NoError(t, err, "Error generating auth header")
	}

	resp, err = tb.App.Test(req)
	require.NoError(t, err, "GET after DELETE should not error")
	assert.Equal(t, 404, resp.StatusCode, "GET after DELETE should return 404")
}

// TestListBucketsWithBackend tests ListBuckets with the specified backend
func TestListBucketsWithBackend(t *testing.T) {
	RunWithBackends(t, FilesystemOnly(), func(t *testing.T, tb *TestBackend) {
		req := httptest.NewRequest("GET", "/", nil)

		if len(tb.Config.Auth) > 0 {
			authEntry := tb.Config.Auth[0]
			timestamp := time.Now().UTC().Format("20060102T150405Z")
			err := GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, tb.Config.Region, "s3", req)
			require.NoError(t, err, "Error generating auth header")
		}

		resp, err := tb.App.Test(req)
		require.NoError(t, err, "ListBuckets request should not error")
		assert.Equal(t, 200, resp.StatusCode, "ListBuckets should return 200")

		var result ListBuckets
		err = xml.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err, "XML parsing should not error")
		assert.NotEmpty(t, result.Buckets, "Should have buckets")
	})
}

// TestListObjectsWithBackend tests ListObjects with the specified backend
func TestListObjectsWithBackend(t *testing.T) {
	RunWithBackends(t, FilesystemOnly(), func(t *testing.T, tb *TestBackend) {
		bucketName := getBucketForBackend(tb.Type)

		req := httptest.NewRequest("GET", "/"+bucketName, nil)

		if len(tb.Config.Auth) > 0 {
			authEntry := tb.Config.Auth[0]
			timestamp := time.Now().UTC().Format("20060102T150405Z")
			err := GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, tb.Config.Region, "s3", req)
			require.NoError(t, err, "Error generating auth header")
		}

		resp, err := tb.App.Test(req)
		require.NoError(t, err, "ListObjects request should not error")
		assert.Equal(t, 200, resp.StatusCode, "ListObjects should return 200")

		var result ListObjectsV2
		err = xml.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err, "XML parsing should not error")
		assert.Equal(t, bucketName, result.Name, "Bucket name should match")
	})
}

// getBucketForBackend returns the appropriate test bucket for the backend type
func getBucketForBackend(backendType BackendType) string {
	switch backendType {
	case BackendFilesystem:
		return "local"
	case BackendDistributed:
		return "datastore"
	default:
		return "local"
	}
}

// TestPutOverwriteWithFilesystem tests that PUT overwrites existing objects correctly
func TestPutOverwriteWithFilesystem(t *testing.T) {
	RunWithBackends(t, FilesystemOnly(), func(t *testing.T, tb *TestBackend) {
		testPutOverwrite(t, tb)
	})
}

// TestPutOverwriteWithDistributed tests that PUT overwrites existing objects correctly
// Note: This test requires QUIC servers to be running for full functionality
func TestPutOverwriteWithDistributed(t *testing.T) {
	t.Skip("Distributed backend requires QUIC servers - skipping until infrastructure is ready")
	RunWithBackends(t, DistributedOnly(), func(t *testing.T, tb *TestBackend) {
		testPutOverwrite(t, tb)
	})
}

// testPutOverwrite verifies that uploading a file, modifying bytes, and re-uploading
// correctly overwrites the original content
func testPutOverwrite(t *testing.T, tb *TestBackend) {
	t.Helper()

	bucketName := getBucketForBackend(tb.Type)
	objectKey := "overwrite-test-" + time.Now().Format("20060102150405") + ".txt"

	// Create initial content
	initialContent := []byte("This is the initial content for overwrite testing. AAAAAAAAAA")

	// PUT initial content
	req := httptest.NewRequest("PUT", "/"+bucketName+"/"+objectKey, bytes.NewReader(initialContent))
	if len(tb.Config.Auth) > 0 {
		authEntry := tb.Config.Auth[0]
		timestamp := time.Now().UTC().Format("20060102T150405Z")
		err := GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, tb.Config.Region, "s3", req)
		require.NoError(t, err, "Error generating auth header")
	}

	resp, err := tb.App.Test(req)
	require.NoError(t, err, "Initial PUT request should not error")
	require.Equal(t, 200, resp.StatusCode, "Initial PUT should return 200")

	// GET and verify initial content
	req = httptest.NewRequest("GET", "/"+bucketName+"/"+objectKey, nil)
	if len(tb.Config.Auth) > 0 {
		authEntry := tb.Config.Auth[0]
		timestamp := time.Now().UTC().Format("20060102T150405Z")
		err := GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, tb.Config.Region, "s3", req)
		require.NoError(t, err, "Error generating auth header")
	}

	resp, err = tb.App.Test(req)
	require.NoError(t, err, "GET request should not error")
	require.Equal(t, 200, resp.StatusCode, "GET should return 200")

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(resp.Body)
	require.NoError(t, err, "Reading response body should not error")
	require.Equal(t, 0, bytes.Compare(initialContent, buf.Bytes()), "Initial content should match exactly")

	// Create modified content (change some bytes in the middle and end)
	modifiedContent := make([]byte, len(initialContent))
	copy(modifiedContent, initialContent)
	// Modify bytes at various positions
	modifiedContent[10] = 'X'
	modifiedContent[20] = 'Y'
	modifiedContent[30] = 'Z'
	// Change the trailing A's to B's
	for i := len(modifiedContent) - 10; i < len(modifiedContent); i++ {
		modifiedContent[i] = 'B'
	}

	// Verify modified content is different from initial
	require.NotEqual(t, 0, bytes.Compare(initialContent, modifiedContent), "Modified content should differ from initial")

	// PUT modified content (overwrite)
	req = httptest.NewRequest("PUT", "/"+bucketName+"/"+objectKey, bytes.NewReader(modifiedContent))
	if len(tb.Config.Auth) > 0 {
		authEntry := tb.Config.Auth[0]
		timestamp := time.Now().UTC().Format("20060102T150405Z")
		err := GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, tb.Config.Region, "s3", req)
		require.NoError(t, err, "Error generating auth header")
	}

	resp, err = tb.App.Test(req)
	require.NoError(t, err, "Overwrite PUT request should not error")
	require.Equal(t, 200, resp.StatusCode, "Overwrite PUT should return 200")

	// GET and verify modified content
	req = httptest.NewRequest("GET", "/"+bucketName+"/"+objectKey, nil)
	if len(tb.Config.Auth) > 0 {
		authEntry := tb.Config.Auth[0]
		timestamp := time.Now().UTC().Format("20060102T150405Z")
		err := GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, tb.Config.Region, "s3", req)
		require.NoError(t, err, "Error generating auth header")
	}

	resp, err = tb.App.Test(req)
	require.NoError(t, err, "GET after overwrite should not error")
	require.Equal(t, 200, resp.StatusCode, "GET after overwrite should return 200")

	buf = new(bytes.Buffer)
	_, err = buf.ReadFrom(resp.Body)
	require.NoError(t, err, "Reading response body should not error")

	// Verify the content is the modified version, not the initial
	require.Equal(t, 0, bytes.Compare(modifiedContent, buf.Bytes()),
		"Content after overwrite should match modified content exactly")
	require.NotEqual(t, 0, bytes.Compare(initialContent, buf.Bytes()),
		"Content after overwrite should NOT match initial content")

	// Cleanup: delete the test object
	req = httptest.NewRequest("DELETE", "/"+bucketName+"/"+objectKey, nil)
	if len(tb.Config.Auth) > 0 {
		authEntry := tb.Config.Auth[0]
		timestamp := time.Now().UTC().Format("20060102T150405Z")
		err := GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, tb.Config.Region, "s3", req)
		require.NoError(t, err, "Error generating auth header")
	}

	resp, err = tb.App.Test(req)
	require.NoError(t, err, "DELETE request should not error")
	require.Equal(t, 204, resp.StatusCode, "DELETE should return 204")
}
