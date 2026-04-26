package s3

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/mulgadc/predastore/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetObjectNoBucketPermissions(t *testing.T) {
	config := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := config.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	server := NewHTTP2ServerWithBackend(config, nil, NewConfigProvider(config.Auth))

	req := httptest.NewRequest(http.MethodGet, "/private/note.txt", nil)
	timestamp := time.Now().UTC().Format("20060102T150405Z")
	err = auth.GenerateAuthHeaderReq("BADACCESSKEY", "BADSECRETKEY", timestamp, "us-east-1", "s3", req)
	assert.NoError(t, err, "Error generating auth header")

	rr := httptest.NewRecorder()
	server.GetHandler().ServeHTTP(rr, req)

	assert.Equal(t, 403, rr.Code, "Status code should be 403")

	var s3error S3Error
	err = xml.Unmarshal(rr.Body.Bytes(), &s3error)
	assert.NoError(t, err, "XML parsing failed")
	assert.Equal(t, "InvalidAccessKeyId", s3error.Code, "Error message should indicate invalid access key")
}

func TestGetObjectWithRange(t *testing.T) {
	tb := setupDistributedBackend(t)
	defer tb.Cleanup()

	bucket := "datastore"
	key := "range-test.txt"
	content := []byte("Hello, this is test content for range requests!")

	// Upload test data
	putReq := httptest.NewRequest(http.MethodPut, "/"+bucket+"/"+key, bytes.NewReader(content))
	if len(tb.Config.Auth) > 0 {
		authEntry := tb.Config.Auth[0]
		timestamp := time.Now().UTC().Format("20060102T150405Z")
		err := auth.GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, tb.Config.Region, "s3", putReq)
		require.NoError(t, err)
	}
	rr := httptest.NewRecorder()
	tb.Handler.ServeHTTP(rr, putReq)
	require.Equal(t, 200, rr.Code, "PUT should return 200")

	// GET with Range header
	getReq := httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
	getReq.Header.Set("Range", "bytes=0-9")
	if len(tb.Config.Auth) > 0 {
		authEntry := tb.Config.Auth[0]
		timestamp := time.Now().UTC().Format("20060102T150405Z")
		err := auth.GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, tb.Config.Region, "s3", getReq)
		require.NoError(t, err)
	}
	rr = httptest.NewRecorder()
	tb.Handler.ServeHTTP(rr, getReq)

	assert.Equal(t, 206, rr.Code, "Status code should be 206 for partial content")
	assert.Equal(t, content[:10], rr.Body.Bytes(), "Partial content should match requested range")
	assert.NotEmpty(t, rr.Header().Get("Content-Range"), "Content-Range header should be set")
}

func TestGetObjectNonExistent(t *testing.T) {
	tb := setupDistributedBackend(t)
	defer tb.Cleanup()

	req := httptest.NewRequest(http.MethodGet, "/datastore/nonexistent.txt", nil)
	if len(tb.Config.Auth) > 0 {
		authEntry := tb.Config.Auth[0]
		timestamp := time.Now().UTC().Format("20060102T150405Z")
		err := auth.GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, tb.Config.Region, "s3", req)
		require.NoError(t, err)
	}

	rr := httptest.NewRecorder()
	tb.Handler.ServeHTTP(rr, req)

	assert.Equal(t, 404, rr.Code, "Status code should be 404 for non-existent file")
}

func TestGetInvalidBucket(t *testing.T) {
	tb := setupDistributedBackend(t)
	defer tb.Cleanup()

	req := httptest.NewRequest(http.MethodGet, "/invalidbucket/file.txt", nil)
	if len(tb.Config.Auth) > 0 {
		authEntry := tb.Config.Auth[0]
		timestamp := time.Now().UTC().Format("20060102T150405Z")
		err := auth.GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, tb.Config.Region, "s3", req)
		require.NoError(t, err)
	}

	rr := httptest.NewRecorder()
	tb.Handler.ServeHTTP(rr, req)

	assert.Equal(t, 404, rr.Code, "Status code should be 404")

	var s3error S3Error
	err := xml.Unmarshal(rr.Body.Bytes(), &s3error)
	assert.NoError(t, err, "XML parsing failed")
	assert.Equal(t, "NoSuchBucket", s3error.Code, "Error message should indicate invalid bucket")
}

func TestGetInvalidObjectKey(t *testing.T) {
	tb := setupDistributedBackend(t)
	defer tb.Cleanup()

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/datastore/%s", string([]byte{0x80, 0x80})), nil)
	if len(tb.Config.Auth) > 0 {
		authEntry := tb.Config.Auth[0]
		timestamp := time.Now().UTC().Format("20060102T150405Z")
		err := auth.GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, tb.Config.Region, "s3", req)
		require.NoError(t, err)
	}

	rr := httptest.NewRecorder()
	tb.Handler.ServeHTTP(rr, req)

	// Invalid key should return an error (400 or 404 depending on backend validation)
	assert.True(t, rr.Code == http.StatusBadRequest || rr.Code == http.StatusNotFound,
		"Invalid key should return 400 or 404, got %d", rr.Code)
}
