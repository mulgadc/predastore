package s3

import (
	"bytes"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mulgadc/predastore/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPutObjectPublicBucketNoAuth(t *testing.T) {
	config := newAuthTestConfig()

	server := NewHTTP2ServerWithBackend(config, nil, NewConfigProvider(config.Auth))

	testContent := []byte("This is a test file created during unit testing")
	req := httptest.NewRequest(http.MethodPut, "/test-bucket01/test_upload.txt", bytes.NewReader(testContent))

	rr := httptest.NewRecorder()
	server.GetHandler().ServeHTTP(rr, req)

	assert.Equal(t, 403, rr.Code, "Status code should be 403")
}

func TestPutObjectPrivateBucketNoAuth(t *testing.T) {
	config := newAuthTestConfig()

	server := NewHTTP2ServerWithBackend(config, nil, NewConfigProvider(config.Auth))

	testContent := []byte("This is a test file created during unit testing")
	req := httptest.NewRequest(http.MethodPut, "/private/test_upload.txt", bytes.NewReader(testContent))

	rr := httptest.NewRecorder()
	server.GetHandler().ServeHTTP(rr, req)

	assert.Equal(t, 403, rr.Code, "Status code should be 403")
}

func TestPutObjectInvalidBucket(t *testing.T) {
	tb := setupDistributedBackend(t)
	defer tb.Cleanup()

	testContent := []byte("This is a test file for an invalid bucket")
	req := httptest.NewRequest(http.MethodPut, "/nonexistent/test_upload.txt", bytes.NewReader(testContent))

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
	err := xml.NewDecoder(rr.Body).Decode(&s3error)
	assert.NoError(t, err, "XML parsing failed")
	assert.Equal(t, "NoSuchBucket", s3error.Code, "Error message should indicate invalid bucket")
}
