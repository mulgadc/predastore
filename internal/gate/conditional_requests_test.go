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

func (g keyGate) enableVersioning() {
	g.t.Helper()
	rr := g.do(http.MethodPut, "/local?versioning", []byte(
		"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"), nil)
	require.Equal(g.t, http.StatusOK, rr.Code, "enable versioning: %s", rr.Body.String())
}

// objectState is what a refused request must leave unchanged: the current
// body, the listing and every version with which one is current.
type objectState struct {
	body     string
	listed   []string
	versions []string
}

func (g keyGate) state(key string) objectState {
	g.t.Helper()
	rr := g.do(http.MethodGet, sdkPath(key), nil, nil)
	require.Equal(g.t, http.StatusOK, rr.Code, "get %q: %s", key, rr.Body.String())
	s := objectState{body: rr.Body.String(), listed: g.listedKeys(url.Values{})}

	rr = g.do(http.MethodGet, "/local?versions=", nil, nil)
	require.Equal(g.t, http.StatusOK, rr.Code, "list versions: %s", rr.Body.String())
	var versions handlers.ListVersionsResult
	require.NoError(g.t, xml.Unmarshal(rr.Body.Bytes(), &versions))
	for _, v := range versions.Versions {
		s.versions = append(s.versions, fmt.Sprintf("%s@%s latest=%t", v.Key, v.VersionId, v.IsLatest))
	}
	for _, m := range versions.DeleteMarkers {
		s.versions = append(s.versions, fmt.Sprintf("%s@%s marker latest=%t", m.Key, m.VersionId, m.IsLatest))
	}
	return s
}

func requireNotImplemented(t *testing.T, rr *httptest.ResponseRecorder) {
	t.Helper()
	require.Equal(t, http.StatusNotImplemented, rr.Code, "body: %s", rr.Body.String())
	require.Contains(t, rr.Body.String(), "<Code>NotImplemented</Code>")
}

// conditionalHeaders are the preconditions every write path must refuse,
// including empty values, which a client still sent.
func conditionalHeaders(etag string) map[string]http.Header {
	return map[string]http.Header{
		"If-Match":            {"If-Match": {etag}},
		"If-None-Match *":     {"If-None-Match": {"*"}},
		"If-None-Match etag":  {"If-None-Match": {etag}},
		"empty If-Match":      {"If-Match": {""}},
		"empty If-None-Match": {"If-None-Match": {""}},
	}
}

// refusedPutHeaders are the PutObject preconditions S3 does not evaluate: an
// If-None-Match other than "*", and either header sent empty.
func refusedPutHeaders(etag string) map[string]http.Header {
	return map[string]http.Header{
		"If-None-Match etag":  {"If-None-Match": {etag}},
		"empty If-Match":      {"If-Match": {""}},
		"empty If-None-Match": {"If-None-Match": {""}},
	}
}

// A PUT carrying a precondition S3 does not evaluate must be refused before it
// stores anything over an existing versioned key.
// Verifies: Q-68.
func TestUnsupportedConditionalPutObjectIsRefusedAndKeepsTheObject(t *testing.T) {
	for name, header := range refusedPutHeaders(`"abc"`) {
		t.Run(name, func(t *testing.T) {
			g := newKeyGate(t)
			g.enableVersioning()
			g.putETag("lock", []byte("v1"))
			g.putETag("lock", []byte("v2"))
			before, mutations := g.state("lock"), g.blob.mutationCount()

			requireNotImplemented(t, g.do(http.MethodPut, sdkPath("lock"), []byte("v3"), header))

			assert.Equal(t, mutations, g.blob.mutationCount(), "blob calls made by a refused PUT")
			assert.Equal(t, before, g.state("lock"))
		})
	}
}

// A refused PUT to a new key must leave it absent and touch no shard.
// Verifies: Q-68.
func TestUnsupportedConditionalPutIsRefusedBeforeAnyShardIsWritten(t *testing.T) {
	for name, header := range refusedPutHeaders(`"abc"`) {
		t.Run(name, func(t *testing.T) {
			g := newKeyGate(t)

			requireNotImplemented(t, g.do(http.MethodPut, sdkPath("fresh"), []byte("body"), header))

			assert.Zero(t, g.blob.mutationCount(), "blob calls made by a refused PUT")
			assert.Equal(t, http.StatusNotFound, g.do(http.MethodHead, sdkPath("fresh"), nil, nil).Code)
		})
	}
}

// If-None-Match: * over an absent key creates it.
// Verifies: Q-68.
func TestPutIfNoneMatchCreatesAnAbsentKey(t *testing.T) {
	g := newKeyGate(t)

	rr := g.do(http.MethodPut, sdkPath("lock"), []byte("first"), http.Header{"If-None-Match": {"*"}})

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	assert.NotEmpty(t, rr.Header().Get("ETag"))
	g.requireBody("lock", "first")
}

// If-None-Match: * over an existing key answers 412 and leaves the original
// body, its listing and its versions as they were.
// Verifies: Q-68.
func TestPutIfNoneMatchOverAnExistingKeyFails(t *testing.T) {
	g := newKeyGate(t)
	g.putETag("lock", []byte("original"))
	before := g.state("lock")

	rr := g.do(http.MethodPut, sdkPath("lock"), []byte("second"), http.Header{"If-None-Match": {"*"}})

	requirePreconditionFailed(t, rr)
	assert.Equal(t, before, g.state("lock"))
	g.requireBody("lock", "original")
}

// If-Match with an ETag the object no longer has answers 412 and keeps the
// current body.
// Verifies: Q-68.
func TestPutIfMatchWithAStaleETagFails(t *testing.T) {
	g := newKeyGate(t)
	stale := g.putETag("lock", []byte("v1"))
	g.putETag("lock", []byte("v2"))
	before := g.state("lock")

	rr := g.do(http.MethodPut, sdkPath("lock"), []byte("v3"), http.Header{"If-Match": {stale}})

	requirePreconditionFailed(t, rr)
	assert.Equal(t, before, g.state("lock"))
	g.requireBody("lock", "v2")
}

// A refused completion must leave the destination and the upload in place, so
// the client can still complete it without the precondition.
// Verifies: Q-68.
func TestConditionalCompleteMultipartUploadIsRefusedAndKeepsTheUpload(t *testing.T) {
	for name, header := range conditionalHeaders(`"abc"`) {
		t.Run(name, func(t *testing.T) {
			g := newKeyGate(t)
			g.enableVersioning()
			const key = "assembled"
			g.putETag(key, []byte("original"))

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
			before, mutations := g.state(key), g.blob.mutationCount()

			requireNotImplemented(t, g.do(http.MethodPost, sdkPath(key)+"?uploadId="+uploadID, complete, header))

			assert.Equal(t, mutations, g.blob.mutationCount(), "blob calls made by a refused completion")
			assert.Equal(t, before, g.state(key))
			rr = g.do(http.MethodGet, sdkPath(key)+"?uploadId="+uploadID, nil, nil)
			require.Equal(t, http.StatusOK, rr.Code, "list parts after refusal: %s", rr.Body.String())
			assert.Contains(t, rr.Body.String(), "<PartNumber>1</PartNumber>")

			rr = g.do(http.MethodPost, sdkPath(key)+"?uploadId="+uploadID, complete, nil)
			require.Equal(t, http.StatusOK, rr.Code, "complete without precondition: %s", rr.Body.String())
			g.requireBody(key, "one part")
		})
	}
}

// A precondition on a copy's destination must refuse the copy, not overwrite
// the destination.
// Verifies: Q-68.
func TestConditionalCopyObjectDestinationIsRefusedAndKeepsTheObject(t *testing.T) {
	for name, header := range conditionalHeaders(`"abc"`) {
		t.Run(name, func(t *testing.T) {
			g := newKeyGate(t)
			g.enableVersioning()
			g.putETag("source", []byte("from source"))
			g.putETag("dest", []byte("original"))
			before, mutations := g.state("dest"), g.blob.mutationCount()

			header = header.Clone()
			header.Set("X-Amz-Copy-Source", strings.TrimPrefix(sdkPath("source"), "/"))
			requireNotImplemented(t, g.do(http.MethodPut, sdkPath("dest"), nil, header))

			assert.Equal(t, mutations, g.blob.mutationCount(), "blob calls made by a refused copy")
			assert.Equal(t, before, g.state("dest"))
		})
	}
}

// Conditional deletes are not supported yet, so one must be refused rather
// than delete or mark an object its precondition should have protected.
// Verifies: Q-68.
func TestConditionalDeleteObjectIsRefusedAndKeepsTheObject(t *testing.T) {
	for name, header := range map[string]http.Header{
		"If-Match":       {"If-Match": {`"abc"`}},
		"empty If-Match": {"If-Match": {""}},
	} {
		t.Run(name, func(t *testing.T) {
			g := newKeyGate(t)
			g.enableVersioning()
			g.putETag("doomed", []byte("v1"))
			g.putETag("doomed", []byte("v2"))
			before, mutations := g.state("doomed"), g.blob.mutationCount()

			requireNotImplemented(t, g.do(http.MethodDelete, sdkPath("doomed"), nil, header))

			assert.Equal(t, mutations, g.blob.mutationCount(), "blob calls made by a refused delete")
			assert.Equal(t, before, g.state("doomed"))
		})
	}
}

// An expected-owner mismatch is still answered as 403 when the delete also
// carries a precondition, and the object stays.
// Verifies: Q-68.
func TestConditionalDeleteObjectWithAWrongOwnerIsDenied(t *testing.T) {
	g := newKeyGate(t)
	g.putETag("owned", []byte("kept"))

	rr := g.do(http.MethodDelete, sdkPath("owned"), nil, http.Header{
		"If-Match":                    {`"abc"`},
		"X-Amz-Expected-Bucket-Owner": {"111122223333"},
	})

	assert.Equal(t, http.StatusForbidden, rr.Code, "body: %s", rr.Body.String())
	assert.Contains(t, rr.Body.String(), "<Code>AccessDenied</Code>")
	g.requireBody("owned", "kept")
}

// One per-object ETag, even an empty one, refuses the whole batch, so no key
// in it is deleted.
// Verifies: Q-68.
func TestDeleteObjectsWithAnETagIsRefusedAndDeletesNothing(t *testing.T) {
	for name, etag := range map[string]string{"ETag": `"abc"`, "empty ETag": ""} {
		t.Run(name, func(t *testing.T) {
			g := newKeyGate(t)
			g.enableVersioning()
			g.putETag("kept-a", []byte("a"))
			g.putETag("kept-b", []byte("b"))
			before, mutations := g.state("kept-a"), g.blob.mutationCount()
			del := []byte("<Delete><Object><Key>kept-a</Key></Object>" +
				"<Object><Key>kept-b</Key><ETag>" + etag + "</ETag></Object></Delete>")

			requireNotImplemented(t, g.do(http.MethodPost, "/local?delete=", del, nil))

			assert.Equal(t, mutations, g.blob.mutationCount(), "blob calls made by a refused batch")
			assert.Equal(t, before, g.state("kept-a"))
			g.requireBody("kept-b", "b")
		})
	}
}
