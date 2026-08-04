package s3

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestListBucketsNoAuth(t *testing.T) {
	config := newAuthTestConfig()

	server := NewHTTP2ServerWithBackend(config, nil, NewConfigProvider(config.Auth))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	server.GetHandler().ServeHTTP(rr, req)

	assert.Equal(t, 403, rr.Code, "Status code should be 403")
}

func TestListObjectsV2HandlerPrivateBucketNoAuth(t *testing.T) {
	config := newAuthTestConfig()

	server := NewHTTP2ServerWithBackend(config, nil, NewConfigProvider(config.Auth))

	req := httptest.NewRequest(http.MethodGet, "/private", nil)
	rr := httptest.NewRecorder()
	server.GetHandler().ServeHTTP(rr, req)

	assert.Equal(t, 403, rr.Code, "Status code should be 403")

	var result S3Error
	err := xml.NewDecoder(rr.Body).Decode(&result)
	assert.NoError(t, err, "XML parsing should not error")
	assert.Equal(t, "AccessDenied", result.Code, "Error message should indicate access denied")
}

func TestListObjectsV2HandlerPrivateBucketBadAuth(t *testing.T) {
	config := newAuthTestConfig()

	server := NewHTTP2ServerWithBackend(config, nil, NewConfigProvider(config.Auth))

	req := httptest.NewRequest(http.MethodGet, "/private", nil)
	signTestReq(t, req, nil, "BADACCESSKEY", "BADSECRETKEY", config.Region, "s3")

	rr := httptest.NewRecorder()
	server.GetHandler().ServeHTTP(rr, req)

	assert.Equal(t, 403, rr.Code, "Status code should be 403")

	var result S3Error
	err := xml.NewDecoder(rr.Body).Decode(&result)
	assert.NoError(t, err, "XML parsing should not error")
	assert.Equal(t, "InvalidAccessKeyId", result.Code, "Error message should indicate invalid access key")
}
