package handlers

import (
	"encoding/xml"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleError_BackendS3Error(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	rr := httptest.NewRecorder()

	s3Err := model.NewS3Error(model.ErrNoSuchBucket, "Bucket not found", 404)
	HandleError(rr, req, s3Err)

	assert.Equal(t, http.StatusNotFound, rr.Code)
	assert.Contains(t, rr.Header().Get("Content-Type"), "application/xml")

	var s3error S3Error
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &s3error))
	assert.Equal(t, "NoSuchBucket", s3error.Code)
}

// The 507 must reach the HTTP client verbatim, like any other *model.S3Error.
func TestHandleError_InsufficientStorage(t *testing.T) {
	req := httptest.NewRequest(http.MethodPut, "/bucket/key", nil)
	rr := httptest.NewRecorder()

	HandleError(rr, req, model.ErrInsufficientStorageError)

	assert.Equal(t, http.StatusInsufficientStorage, rr.Code)

	var s3error S3Error
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &s3error))
	assert.Equal(t, "InsufficientStorage", s3error.Code)
}

func TestHandleError_NoSuchBucketString(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	rr := httptest.NewRecorder()

	HandleError(rr, req, errors.New("NoSuchBucket: bucket does not exist"))

	assert.Equal(t, http.StatusNotFound, rr.Code)
	var s3error S3Error
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &s3error))
	assert.Equal(t, "NoSuchBucket", s3error.Code)
}

func TestHandleError_AccessDenied(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	rr := httptest.NewRecorder()

	HandleError(rr, req, errors.New("AccessDenied: not allowed"))

	assert.Equal(t, http.StatusForbidden, rr.Code)
	var s3error S3Error
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &s3error))
	assert.Equal(t, "AccessDenied", s3error.Code)
}

func TestHandleError_NoSuchKey(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	rr := httptest.NewRecorder()

	HandleError(rr, req, errors.New("NoSuchKey: object not found"))

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestHandleError_OsNotExist(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	rr := httptest.NewRecorder()

	HandleError(rr, req, os.ErrNotExist)

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestHandleError_BucketNotFoundString(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	rr := httptest.NewRecorder()

	HandleError(rr, req, errors.New("Bucket not found"))

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestHandleError_GenericError(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	rr := httptest.NewRecorder()

	HandleError(rr, req, errors.New("something unexpected"))

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
	var s3error S3Error
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &s3error))
	assert.Equal(t, "InternalError", s3error.Code)
}
