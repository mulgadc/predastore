package handlers

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These three surfaces answered 200-and-discard, 500 and BucketAlreadyOwnedByYou
// where S3 answers a refusal. Each is exercised below the object still exists so
// the wrong answer is not merely masked by a missing resource.

func TestPutObjectAclIsNotImplemented(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodPut, "/"+testBucket+"/k?acl", nil)
	req = req.WithContext(WithObject(req.Context(), model.Object{
		Bucket: model.Bucket{Name: testBucket}, Key: "k",
	}))
	w := httptest.NewRecorder()

	PutObject(nil, nil, nil, testCache(), Config{}).ServeHTTP(w, req)

	require.Equal(t, http.StatusNotImplemented, w.Code, "body: %s", w.Body.String())

	var s3err S3Error
	require.NoError(t, xml.NewDecoder(w.Body).Decode(&s3err))
	assert.Equal(t, "NotImplemented", s3err.Code)
}

func TestGetObjectAclIsNotImplemented(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, "/"+testBucket+"/k?acl", nil)
	req = req.WithContext(WithObject(req.Context(), model.Object{
		Bucket: model.Bucket{Name: testBucket}, Key: "k",
	}))
	w := httptest.NewRecorder()

	GetObject(nil, nil, nil, testCache(), Config{}).ServeHTTP(w, req)

	require.Equal(t, http.StatusNotImplemented, w.Code, "body: %s", w.Body.String())

	var s3err S3Error
	require.NoError(t, xml.NewDecoder(w.Body).Decode(&s3err))
	assert.Equal(t, "NotImplemented", s3err.Code)
}

func TestPutBucketVersioningIsNotImplemented(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodPut, "/"+testBucket+"?versioning", nil)
	req = req.WithContext(WithBucket(req.Context(), model.Bucket{Name: testBucket}))
	w := httptest.NewRecorder()

	CreateBucket(nil, testCache(), Config{}).ServeHTTP(w, req)

	require.Equal(t, http.StatusNotImplemented, w.Code, "body: %s", w.Body.String())

	var s3err S3Error
	require.NoError(t, xml.NewDecoder(w.Body).Decode(&s3err))
	assert.Equal(t, "NotImplemented", s3err.Code)
}
