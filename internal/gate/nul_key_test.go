package gate

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A key holding NUL would name the same placement row as a version of the key
// before the NUL, so S3's refusal of NUL is what keeps each version's bytes its
// own: version X of key a must survive a write of key "a\x00X".
// Verifies: Q-68.
func TestNULKeyCannotReplaceAnotherKeysVersion(t *testing.T) {
	blob := newMemBlob()
	cfg := copyServer(t).cfg
	cfg.Blob, cfg.RS, cfg.BlobNodeIDs = blob, RS{Data: 2, Parity: 1}, []config.NodeID{1, 2, 3}
	server := newTestGate(t, cfg)
	do := func(method, target string, body []byte, header http.Header) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, target, bytes.NewReader(body))
		req.ContentLength = int64(len(body))
		for k, v := range header {
			req.Header[k] = v
		}
		signTestReq(t, req, body, keyOwner, secret, "ap-southeast-2", "s3")
		rr := httptest.NewRecorder()
		server.ServeHTTP(rr, req)
		return rr
	}
	versioning := func(status string) {
		t.Helper()
		rr := do(http.MethodPut, "/owner-bucket?versioning", []byte(
			"<VersioningConfiguration><Status>"+status+"</Status></VersioningConfiguration>"), nil)
		require.Equal(t, http.StatusOK, rr.Code, "versioning %s: %s", status, rr.Body.String())
	}

	versioning("Enabled")
	rr := do(http.MethodPut, "/owner-bucket/a", []byte("version X"), nil)
	require.Equal(t, http.StatusOK, rr.Code, "put a: %s", rr.Body.String())
	version := rr.Header().Get("X-Amz-Version-Id")
	require.NotEmpty(t, version)
	versionETag := rr.Header().Get("ETag")
	require.Equal(t, http.StatusOK, do(http.MethodPut, "/owner-bucket/a", []byte("current"), nil).Code)

	// A suspended bucket writes the null version under the key's own hash.
	versioning("Suspended")
	mutations := blob.mutationCount()
	rr = do(http.MethodPut, "/owner-bucket/"+url.PathEscape("a\x00"+version), []byte("intruder"), nil)
	assert.Equal(t, http.StatusBadRequest, rr.Code, "put of a NUL key: %s", rr.Body.String())
	assert.Contains(t, rr.Body.String(), "<Code>BadRequest</Code>")
	assert.Equal(t, mutations, blob.mutationCount(), "blob calls made by a refused put")

	read := func(method string) *httptest.ResponseRecorder {
		t.Helper()
		return do(method, "/owner-bucket/a?versionId="+url.QueryEscape(version), nil, nil)
	}
	got := read(http.MethodGet)
	require.Equal(t, http.StatusOK, got.Code, "get version X: %s", got.Body.String())
	body, err := io.ReadAll(got.Body)
	require.NoError(t, err)
	assert.Equal(t, "version X", string(body), "GetObject of version X")
	assert.Equal(t, versionETag, read(http.MethodHead).Header().Get("ETag"), "HeadObject of version X")

	rr = do(http.MethodPut, "/owner-bucket/copy", nil, http.Header{
		"X-Amz-Copy-Source": {"owner-bucket/a?versionId=" + url.QueryEscape(version)},
	})
	require.Equal(t, http.StatusOK, rr.Code, "copy version X: %s", rr.Body.String())
	copied := do(http.MethodGet, "/owner-bucket/copy", nil, nil)
	assert.Equal(t, "version X", copied.Body.String(), "CopyObject of version X")
}
