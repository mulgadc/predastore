package s3

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateBucket_Handler(t *testing.T) {
	tb := setupDistributedBackend(t)
	defer tb.Cleanup()

	req := httptest.NewRequest(http.MethodPut, "/new-test-bucket", nil)
	if len(tb.Config.Auth) > 0 {
		authEntry := tb.Config.Auth[0]
		signTestReq(t, req, nil, authEntry.AccessKeyID, authEntry.SecretAccessKey, tb.Config.Region, "s3")
	}
	rr := httptest.NewRecorder()
	tb.Handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestHeadBucket_NotFound(t *testing.T) {
	tb := setupDistributedBackend(t)
	defer tb.Cleanup()

	req := httptest.NewRequest(http.MethodHead, "/nonexistent-bucket-xyz", nil)
	if len(tb.Config.Auth) > 0 {
		authEntry := tb.Config.Auth[0]
		signTestReq(t, req, nil, authEntry.AccessKeyID, authEntry.SecretAccessKey, tb.Config.Region, "s3")
	}
	rr := httptest.NewRecorder()
	tb.Handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestDeleteBucket_Handler(t *testing.T) {
	tb := setupDistributedBackend(t)
	defer tb.Cleanup()

	// Create a bucket first, then delete it
	createReq := httptest.NewRequest(http.MethodPut, "/delete-test-bucket", nil)
	if len(tb.Config.Auth) > 0 {
		authEntry := tb.Config.Auth[0]
		signTestReq(t, createReq, nil, authEntry.AccessKeyID, authEntry.SecretAccessKey, tb.Config.Region, "s3")
	}
	rr := httptest.NewRecorder()
	tb.Handler.ServeHTTP(rr, createReq)
	require.Equal(t, http.StatusOK, rr.Code)

	req := httptest.NewRequest(http.MethodDelete, "/delete-test-bucket", nil)
	if len(tb.Config.Auth) > 0 {
		authEntry := tb.Config.Auth[0]
		signTestReq(t, req, nil, authEntry.AccessKeyID, authEntry.SecretAccessKey, tb.Config.Region, "s3")
	}
	rr = httptest.NewRecorder()
	tb.Handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusNoContent, rr.Code)
}

func TestCreateBucket_WithLocationConstraint(t *testing.T) {
	tb := setupDistributedBackend(t)
	defer tb.Cleanup()

	body := `<CreateBucketConfiguration><LocationConstraint>eu-west-1</LocationConstraint></CreateBucketConfiguration>`
	req := httptest.NewRequest(http.MethodPut, "/new-bucket-region", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/xml")
	if len(tb.Config.Auth) > 0 {
		authEntry := tb.Config.Auth[0]
		signTestReq(t, req, []byte(body), authEntry.AccessKeyID, authEntry.SecretAccessKey, tb.Config.Region, "s3")
	}
	rr := httptest.NewRecorder()
	tb.Handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
}
