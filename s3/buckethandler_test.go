package s3

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupBucketHandlerServer(t *testing.T) *TestBackend {
	t.Helper()
	s3 := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := s3.ReadConfig()
	require.NoError(t, err)

	be := s3.createFilesystemBackend()
	server := NewHTTP2ServerWithBackend(s3, be, NewConfigProvider(s3.Auth))
	return &TestBackend{
		Config:  s3,
		Server:  server,
		Handler: server.GetHandler(),
		Backend: be,
		Cleanup: func() { be.Close() },
	}
}

func TestCreateBucket_Handler(t *testing.T) {
	tb := setupBucketHandlerServer(t)
	defer tb.Cleanup()

	// Filesystem backend doesn't support creating buckets, should return error
	req := httptest.NewRequest(http.MethodPut, "/new-test-bucket", nil)
	rr := httptest.NewRecorder()
	tb.Handler.ServeHTTP(rr, req)

	// Should get an error (AccessDenied for filesystem backend)
	assert.NotEqual(t, http.StatusOK, rr.Code)
}

func TestHeadBucket_Handler(t *testing.T) {
	tb := setupBucketHandlerServer(t)
	defer tb.Cleanup()

	// Head an existing bucket
	req := httptest.NewRequest(http.MethodHead, "/test-bucket01", nil)
	rr := httptest.NewRecorder()
	tb.Handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.NotEmpty(t, rr.Header().Get("x-amz-bucket-region"))
}

func TestHeadBucket_NotFound(t *testing.T) {
	tb := setupBucketHandlerServer(t)
	defer tb.Cleanup()

	req := httptest.NewRequest(http.MethodHead, "/nonexistent-bucket-xyz", nil)
	rr := httptest.NewRecorder()
	tb.Handler.ServeHTTP(rr, req)

	// Filesystem backend returns 403 for unknown buckets (not configured)
	assert.True(t, rr.Code == http.StatusNotFound || rr.Code == http.StatusForbidden)
}

func TestDeleteBucket_Handler(t *testing.T) {
	tb := setupBucketHandlerServer(t)
	defer tb.Cleanup()

	// Filesystem backend doesn't support deleting buckets
	req := httptest.NewRequest(http.MethodDelete, "/test-bucket01", nil)
	rr := httptest.NewRecorder()
	tb.Handler.ServeHTTP(rr, req)

	// Should get an error (AccessDenied for filesystem backend)
	assert.NotEqual(t, http.StatusNoContent, rr.Code)
}

func TestCreateBucket_WithLocationConstraint(t *testing.T) {
	tb := setupBucketHandlerServer(t)
	defer tb.Cleanup()

	body := `<CreateBucketConfiguration><LocationConstraint>eu-west-1</LocationConstraint></CreateBucketConfiguration>`
	req := httptest.NewRequest(http.MethodPut, "/new-bucket-region", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/xml")
	rr := httptest.NewRecorder()
	tb.Handler.ServeHTTP(rr, req)

	// Filesystem backend rejects, but the XML parsing path should be exercised
	assert.NotEqual(t, http.StatusOK, rr.Code)
}
