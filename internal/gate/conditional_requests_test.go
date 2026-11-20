package gate

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/mulgadc/predastore/internal/gate/handlers"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// putETag writes body to key and returns the ETag the gate answered with.
func (g keyGate) putETag(key string, body []byte) string {
	g.t.Helper()
	rr := g.do(http.MethodPut, sdkPath(key), body, nil)
	require.Equal(g.t, http.StatusOK, rr.Code, "put %q: %s", key, rr.Body.String())
	etag := rr.Header().Get("ETag")
	require.NotEmpty(g.t, etag, "put %q answered no ETag", key)
	return etag
}

func (g keyGate) requireBody(key, want string) {
	g.t.Helper()
	rr := g.do(http.MethodGet, sdkPath(key), nil, nil)
	require.Equal(g.t, http.StatusOK, rr.Code, "get %q: %s", key, rr.Body.String())
	assert.Equal(g.t, want, rr.Body.String(), "body of %q", key)
}

// Conditional writes are not supported yet, so a create-only PUT over an
// existing key must be refused outright; clients use it as a lock.
// Verifies: Q-68.
func TestPutObjectIfNoneMatchStarIsRefusedAndKeepsTheObject(t *testing.T) {
	g := newKeyGate(t)
	const key = "lock"
	g.putETag(key, []byte("first"))

	rr := g.do(http.MethodPut, sdkPath(key), []byte("second"), http.Header{"If-None-Match": {"*"}})

	assert.Equal(t, http.StatusNotImplemented, rr.Code, "body: %s", rr.Body.String())
	assert.Contains(t, rr.Body.String(), "<Code>NotImplemented</Code>")
	g.requireBody(key, "first")
}

// An If-Match PUT must be refused rather than overwrite the current object.
// Verifies: Q-68.
func TestPutObjectIfMatchIsRefusedAndKeepsTheObject(t *testing.T) {
	g := newKeyGate(t)
	const key = "checkpoint"
	stale := g.putETag(key, []byte("v1"))
	current := g.putETag(key, []byte("v2"))
	require.NotEqual(t, stale, current, "content-derived ETags must differ")

	rr := g.do(http.MethodPut, sdkPath(key), []byte("v3"), http.Header{"If-Match": {stale}})

	assert.Equal(t, http.StatusNotImplemented, rr.Code, "body: %s", rr.Body.String())
	assert.Contains(t, rr.Body.String(), "<Code>NotImplemented</Code>")
	g.requireBody(key, "v2")
}

// Conditional deletes are not supported yet, so one must be refused outright
// rather than delete an object its precondition should have protected.
// Verifies: Q-68.
func TestDeleteObjectIfMatchIsRefusedAndKeepsTheObject(t *testing.T) {
	g := newKeyGate(t)
	const key = "doomed"
	stale := g.putETag(key, []byte("v1"))
	g.putETag(key, []byte("v2"))

	rr := g.do(http.MethodDelete, sdkPath(key), nil, http.Header{"If-Match": {stale}})

	assert.Equal(t, http.StatusNotImplemented, rr.Code, "body: %s", rr.Body.String())
	assert.Contains(t, rr.Body.String(), "<Code>NotImplemented</Code>")
	g.requireBody(key, "v2")
}

func requireNotImplemented(t *testing.T, rr *httptest.ResponseRecorder) {
	t.Helper()
	require.Equal(t, http.StatusNotImplemented, rr.Code, "body: %s", rr.Body.String())
	require.Contains(t, rr.Body.String(), "<Code>NotImplemented</Code>")
}

// A refused PUT must be refused before its body is stored, so a new key stays
// absent and no shard is left behind.
// Verifies: Q-68.
func TestConditionalPutIsRefusedBeforeAnyShardIsWritten(t *testing.T) {
	for _, header := range []http.Header{{"If-None-Match": {"*"}}, {"If-Match": {`"abc"`}}} {
		g := newKeyGate(t)
		before := g.blob.shardCount()

		rr := g.do(http.MethodPut, sdkPath("fresh"), []byte("body"), header)

		requireNotImplemented(t, rr)
		assert.Equal(t, before, g.blob.shardCount(), "shards written by a refused PUT with %v", header)
		assert.Equal(t, http.StatusNotFound, g.do(http.MethodHead, sdkPath("fresh"), nil, nil).Code)
	}
}

// A refused completion must leave the upload and its parts in place, so the
// client can still complete it without the precondition.
// Verifies: Q-68.
func TestConditionalCompleteMultipartUploadIsRefusedAndKeepsTheUpload(t *testing.T) {
	g := newKeyGate(t)
	const key = "assembled"
	rr := g.do(http.MethodPost, sdkPath(key)+"?uploads", nil, nil)
	require.Equal(t, http.StatusOK, rr.Code, "initiate: %s", rr.Body.String())
	var initiated handlers.InitiateMultipartUploadResult
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &initiated))
	uploadID := url.QueryEscape(initiated.UploadId)

	rr = g.do(http.MethodPut, sdkPath(key)+"?partNumber=1&uploadId="+uploadID, []byte("one part"), nil)
	require.Equal(t, http.StatusOK, rr.Code, "upload part: %s", rr.Body.String())
	complete := []byte(fmt.Sprintf(
		"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>",
		rr.Header().Get("ETag")))

	rr = g.do(http.MethodPost, sdkPath(key)+"?uploadId="+uploadID, complete, http.Header{"If-None-Match": {"*"}})
	requireNotImplemented(t, rr)
	assert.Equal(t, http.StatusNotFound, g.do(http.MethodHead, sdkPath(key), nil, nil).Code)

	rr = g.do(http.MethodGet, sdkPath(key)+"?uploadId="+uploadID, nil, nil)
	require.Equal(t, http.StatusOK, rr.Code, "list parts after refusal: %s", rr.Body.String())
	assert.Contains(t, rr.Body.String(), "<PartNumber>1</PartNumber>")

	rr = g.do(http.MethodPost, sdkPath(key)+"?uploadId="+uploadID, complete, nil)
	require.Equal(t, http.StatusOK, rr.Code, "complete without precondition: %s", rr.Body.String())
	g.requireBody(key, "one part")
}

// A precondition on a copy's destination must refuse the copy, not overwrite
// the destination.
// Verifies: Q-68.
func TestConditionalCopyObjectDestinationIsRefusedAndKeepsTheObject(t *testing.T) {
	g := newKeyGate(t)
	g.putETag("source", []byte("from source"))
	dest := g.putETag("dest", []byte("original"))
	before := g.blob.shardCount()

	rr := g.do(http.MethodPut, sdkPath("dest"), nil, http.Header{
		"X-Amz-Copy-Source": {strings.TrimPrefix(sdkPath("source"), "/")},
		"If-Match":          {dest},
	})

	requireNotImplemented(t, rr)
	assert.Equal(t, before, g.blob.shardCount(), "shards written by a refused copy")
	g.requireBody("dest", "original")
}

// One per-object ETag refuses the whole batch, so no key in it is deleted.
// Verifies: Q-68.
func TestDeleteObjectsWithAnETagIsRefusedAndDeletesNothing(t *testing.T) {
	g := newKeyGate(t)
	g.putETag("kept-a", []byte("a"))
	etag := g.putETag("kept-b", []byte("b"))
	del := []byte("<Delete><Object><Key>kept-a</Key></Object>" +
		"<Object><Key>kept-b</Key><ETag>" + etag + "</ETag></Object></Delete>")

	rr := g.do(http.MethodPost, "/local?delete=", del, nil)

	requireNotImplemented(t, rr)
	g.requireBody("kept-a", "a")
	g.requireBody("kept-b", "b")
}
