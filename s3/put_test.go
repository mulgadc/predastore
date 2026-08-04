package s3

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
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
