package gate

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A key holding NUL would name the same placement row as a version of the key
// before the NUL, so S3's refusal of NUL is what keeps each version's bytes its
// own: version X of key a must survive a write of key "a\x00X".
// Verifies: Q-68.
func TestNULKeyCannotReplaceAnotherKeysVersion(t *testing.T) {
	g := newBlobGate(t)
	blob := g.blob
	do := func(method, target string, body []byte, header http.Header) *httptest.ResponseRecorder {
		t.Helper()
		return g.do(keyOwner, method, target, body, header)
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

// Every request that names a key holding NUL is refused with S3's 400 before
// anything is stored, whichever operation names it.
// Verifies: Q-68.
func TestNULKeyIsRefusedOnEveryKeyPath(t *testing.T) {
	g := newBlobGate(t)
	rr := g.do(keyOwner, http.MethodPut, "/owner-bucket/src", []byte("source"), nil)
	require.Equal(t, http.StatusOK, rr.Code, "put source: %s", rr.Body.String())
	rr = g.do(keyOwner, http.MethodPost, "/owner-bucket/assembled?uploads", nil, nil)
	require.Equal(t, http.StatusOK, rr.Code, "initiate: %s", rr.Body.String())
	var initiated handlers.InitiateMultipartUploadResult
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &initiated))

	nul := "/owner-bucket/" + url.PathEscape("a\x00b")
	upload := "uploadId=" + url.QueryEscape(initiated.UploadId)
	complete := []byte("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"x\"</ETag></Part></CompleteMultipartUpload>")
	copySource := http.Header{"X-Amz-Copy-Source": {"owner-bucket/src"}}
	cases := []struct {
		name, method, target string
		body                 []byte
		header               http.Header
	}{
		{"PutObject", http.MethodPut, nul, []byte("x"), nil},
		{"GetObject", http.MethodGet, nul, nil, nil},
		{"HeadObject", http.MethodHead, nul, nil, nil},
		{"DeleteObject", http.MethodDelete, nul, nil, nil},
		{"CreateMultipartUpload", http.MethodPost, nul + "?uploads", nil, nil},
		{"UploadPart", http.MethodPut, nul + "?partNumber=1&" + upload, []byte("x"), nil},
		{"CompleteMultipartUpload", http.MethodPost, nul + "?" + upload, complete, nil},
		{"CopyObject destination", http.MethodPut, nul, nil, copySource},
		{"UploadPartCopy destination", http.MethodPut, nul + "?partNumber=1&" + upload, nil, copySource},
		{"ListParts", http.MethodGet, nul + "?" + upload, nil, nil},
		{"AbortMultipartUpload", http.MethodDelete, nul + "?" + upload, nil, nil},
	}
	for _, c := range cases {
		rows, mutations := maps.Clone(g.meta.rows), g.blob.mutationCount()
		rr := g.do(keyOwner, c.method, c.target, c.body, c.header)
		assert.Equal(t, http.StatusBadRequest, rr.Code, "%s: %s", c.name, rr.Body.String())
		if c.method != http.MethodHead {
			assert.Contains(t, rr.Body.String(), "<Code>BadRequest</Code>", c.name)
		}
		assert.Equal(t, rows, g.meta.rows, "%s: metadata changed", c.name)
		assert.Equal(t, mutations, g.blob.mutationCount(), "%s: blob calls", c.name)
	}
}

// A copy source holding NUL is authorized before the handler sees it, so a
// caller denied the source gets 403 and a permitted caller gets S3's 400;
// neither writes the destination.
// Verifies: Q-68.
func TestNULCopySourceIsRefused(t *testing.T) {
	g := newBlobGate(t)
	rr := g.do(keyOwner, http.MethodPost, "/owner-bucket/assembled?uploads", nil, nil)
	require.Equal(t, http.StatusOK, rr.Code, "initiate: %s", rr.Body.String())
	var initiated handlers.InitiateMultipartUploadResult
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &initiated))

	destinations := map[string]string{
		"CopyObject":     "/owner-bucket/dest",
		"UploadPartCopy": "/owner-bucket/assembled?partNumber=1&uploadId=" + url.QueryEscape(initiated.UploadId),
	}
	callers := []struct {
		name, key string
		want      int
	}{
		{"caller denied the source", keySecretDenied, http.StatusForbidden},
		{"caller allowed the source", keyOwner, http.StatusBadRequest},
	}
	for name, dest := range destinations {
		for _, c := range callers {
			rows, mutations := maps.Clone(g.meta.rows), g.blob.mutationCount()
			rr := g.do(c.key, http.MethodPut, dest, nil, http.Header{"X-Amz-Copy-Source": {"owner-bucket/secret/a%00b"}})
			assert.Equal(t, c.want, rr.Code, "%s, %s: %s", name, c.name, rr.Body.String())
			if c.want == http.StatusBadRequest {
				assert.Contains(t, rr.Body.String(), "<Code>BadRequest</Code>", "%s, %s", name, c.name)
			}
			assert.Equal(t, rows, g.meta.rows, "%s, %s: metadata changed", name, c.name)
			assert.Equal(t, mutations, g.blob.mutationCount(), "%s, %s: blob calls", name, c.name)
		}
	}
}

// XML cannot carry NUL, even as a character reference, so a batch delete naming
// one is malformed and deletes nothing.
// Verifies: Q-68.
func TestNULKeyInBatchDeleteIsMalformed(t *testing.T) {
	g := newBlobGate(t)
	rr := g.do(keyOwner, http.MethodPut, "/owner-bucket/a", []byte("kept"), nil)
	require.Equal(t, http.StatusOK, rr.Code, "put: %s", rr.Body.String())

	body := []byte("<Delete><Object><Key>a</Key></Object><Object><Key>a&#0;b</Key></Object></Delete>")
	rows := maps.Clone(g.meta.rows)
	rr = g.do(keyOwner, http.MethodPost, "/owner-bucket?delete", body, http.Header{"Content-Md5": {contentMD5(body)}})
	assert.Equal(t, http.StatusBadRequest, rr.Code, "body: %s", rr.Body.String())
	assert.Contains(t, rr.Body.String(), "<Code>MalformedXML</Code>")
	assert.Equal(t, rows, g.meta.rows, "metadata changed by a refused batch")
	assert.Equal(t, http.StatusOK, g.do(keyOwner, http.MethodGet, "/owner-bucket/a", nil, nil).Code)
}

// blobGate is the signed copy-test gate with blob storage behind it.
type blobGate struct {
	t      *testing.T
	server *Server
	blob   *memBlob
	meta   *fakeMeta
}

func newBlobGate(t *testing.T) blobGate {
	t.Helper()
	blob := newMemBlob()
	cfg := copyServer(t).cfg
	cfg.Blob, cfg.RS, cfg.BlobNodeIDs = blob, RS{Data: 2, Parity: 1}, []config.NodeID{1, 2, 3}
	meta, ok := cfg.Meta.(*fakeMeta)
	require.True(t, ok)
	return blobGate{t: t, server: newTestGate(t, cfg), blob: blob, meta: meta}
}

func (g blobGate) do(accessKey, method, target string, body []byte, header http.Header) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	maps.Copy(req.Header, header)
	g.t.Helper()
	signTestReq(g.t, req, body, accessKey, secret, "ap-southeast-2", "s3")
	rr := httptest.NewRecorder()
	g.server.ServeHTTP(rr, req)
	return rr
}

func contentMD5(body []byte) string {
	sum := md5.Sum(body)
	return base64.StdEncoding.EncodeToString(sum[:])
}
