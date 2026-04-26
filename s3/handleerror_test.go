package s3

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/mulgadc/predastore/backend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupHandleErrorServer(t *testing.T) *HTTP2Server {
	t.Helper()
	config := &Config{Region: "us-east-1"}
	return NewHTTP2ServerWithBackend(config, nil, NewConfigProvider(nil))
}

func TestHandleError_BackendS3Error(t *testing.T) {
	server := setupHandleErrorServer(t)
	req := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	rr := httptest.NewRecorder()

	s3Err := backend.NewS3Error(backend.ErrNoSuchBucket, "Bucket not found", 404)
	server.handleError(rr, req, s3Err)

	assert.Equal(t, http.StatusNotFound, rr.Code)
	assert.Contains(t, rr.Header().Get("Content-Type"), "application/xml")

	var s3error S3Error
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &s3error))
	assert.Equal(t, "NoSuchBucket", s3error.Code)
}

func TestHandleError_NoSuchBucketString(t *testing.T) {
	server := setupHandleErrorServer(t)
	req := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	rr := httptest.NewRecorder()

	server.handleError(rr, req, errors.New("NoSuchBucket: bucket does not exist"))

	assert.Equal(t, http.StatusNotFound, rr.Code)
	var s3error S3Error
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &s3error))
	assert.Equal(t, "NoSuchBucket", s3error.Code)
}

func TestHandleError_AccessDenied(t *testing.T) {
	server := setupHandleErrorServer(t)
	req := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	rr := httptest.NewRecorder()

	server.handleError(rr, req, errors.New("AccessDenied: not allowed"))

	assert.Equal(t, http.StatusForbidden, rr.Code)
	var s3error S3Error
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &s3error))
	assert.Equal(t, "AccessDenied", s3error.Code)
}

func TestHandleError_NoSuchKey(t *testing.T) {
	server := setupHandleErrorServer(t)
	req := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	rr := httptest.NewRecorder()

	server.handleError(rr, req, errors.New("NoSuchKey: object not found"))

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestHandleError_OsNotExist(t *testing.T) {
	server := setupHandleErrorServer(t)
	req := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	rr := httptest.NewRecorder()

	server.handleError(rr, req, os.ErrNotExist)

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestHandleError_BucketNotFoundString(t *testing.T) {
	server := setupHandleErrorServer(t)
	req := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	rr := httptest.NewRecorder()

	server.handleError(rr, req, errors.New("Bucket not found"))

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestHandleError_GenericError(t *testing.T) {
	server := setupHandleErrorServer(t)
	req := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	rr := httptest.NewRecorder()

	server.handleError(rr, req, errors.New("something unexpected"))

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
	var s3error S3Error
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &s3error))
	assert.Equal(t, "InternalError", s3error.Code)
}

func TestHTTP2Server_Shutdown_NilServer(t *testing.T) {
	server := setupHandleErrorServer(t)
	// server.server is nil (no ListenAndServe called)
	err := server.Shutdown(context.Background())
	assert.NoError(t, err)
}

func TestHTTP2Server_GetRouter(t *testing.T) {
	server := setupHandleErrorServer(t)
	assert.NotNil(t, server.GetRouter())
}

func TestHTTP2Server_GetHandler(t *testing.T) {
	server := setupHandleErrorServer(t)
	assert.NotNil(t, server.GetHandler())
}
