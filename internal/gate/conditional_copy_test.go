package gate

import (
	"bytes"
	"encoding/xml"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/handlers"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// copyCase is one set of copy-source conditions and whether the copy runs.
type copyCase struct {
	name   string
	header http.Header
	copies bool
}

// copySourceCases are the x-amz-copy-source-if-* conditions, alone and in the
// two pairs S3 documents, against a source with this etag and modification.
func copySourceCases(f readFixture) []copyCase {
	const (
		ifMatch           = "X-Amz-Copy-Source-If-Match"
		ifNoneMatch       = "X-Amz-Copy-Source-If-None-Match"
		ifModifiedSince   = "X-Amz-Copy-Source-If-Modified-Since"
		ifUnmodifiedSince = "X-Amz-Copy-Source-If-Unmodified-Since"
	)
	return []copyCase{
		{"If-Match current", http.Header{ifMatch: {f.etag}}, true},
		{"If-Match star", http.Header{ifMatch: {"*"}}, true},
		{"If-Match other", http.Header{ifMatch: {`"other"`}}, false},
		{"If-None-Match current", http.Header{ifNoneMatch: {f.etag}}, false},
		{"If-None-Match other", http.Header{ifNoneMatch: {`"other"`}}, true},
		{"If-Modified-Since at modification", http.Header{ifModifiedSince: {f.date(0)}}, false},
		{"If-Modified-Since before", http.Header{ifModifiedSince: {f.date(-time.Hour)}}, true},
		{"If-Unmodified-Since at modification", http.Header{ifUnmodifiedSince: {f.date(0)}}, true},
		{"If-Unmodified-Since before", http.Header{ifUnmodifiedSince: {f.date(-time.Hour)}}, false},
		{"If-Match true overrides If-Unmodified-Since false",
			http.Header{ifMatch: {f.etag}, ifUnmodifiedSince: {f.date(-time.Hour)}}, true},
		{"If-None-Match false with If-Modified-Since true",
			http.Header{ifNoneMatch: {f.etag}, ifModifiedSince: {f.date(-time.Hour)}}, false},
	}
}

func requirePreconditionFailed(t *testing.T, rr *httptest.ResponseRecorder) {
	t.Helper()
	require.Equal(t, http.StatusPreconditionFailed, rr.Code, "body: %s", rr.Body.String())
	require.Contains(t, rr.Body.String(), "<Code>PreconditionFailed</Code>")
}

// A copy runs only when its source conditions hold; a failing one answers 412,
// never 304, and leaves the destination as it was without any blob write.
// Verifies: Q-68.
func TestConditionalCopyObjectEvaluatesTheSource(t *testing.T) {
	for i, name := range copyCaseNames() {
		t.Run(name, func(t *testing.T) {
			f := newReadFixture(t)
			c := copySourceCases(f)[i]
			f.g.putETag("dest", []byte("original"))
			before, mutations := f.g.state("dest"), f.g.blob.mutationCount()

			header := c.header.Clone()
			header.Set("X-Amz-Copy-Source", strings.TrimPrefix(sdkPath("obj"), "/"))
			rr := f.g.do(http.MethodPut, sdkPath("dest"), nil, header)

			if c.copies {
				require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
				f.g.requireBody("dest", "0123456789")
				return
			}
			requirePreconditionFailed(t, rr)
			assert.Equal(t, mutations, f.g.blob.mutationCount(), "blob calls made by a refused copy")
			assert.Equal(t, before, f.g.state("dest"))
		})
	}
}

// copyCaseNames lists the cases once so each subtest can build its headers
// from the fixture it copies from.
func copyCaseNames() []string {
	var names []string
	for _, c := range copySourceCases(readFixture{modified: time.Unix(0, 0)}) {
		names = append(names, c.name)
	}
	return names
}

// partUpload is a multipart upload on key "assembled".
type partUpload struct {
	g      keyGate
	target string
}

func newPartUpload(t *testing.T, g keyGate) partUpload {
	t.Helper()
	rr := g.do(http.MethodPost, sdkPath("assembled")+"?uploads", nil, nil)
	require.Equal(t, http.StatusOK, rr.Code, "initiate: %s", rr.Body.String())
	var initiated handlers.InitiateMultipartUploadResult
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &initiated))
	return partUpload{g: g, target: sdkPath("assembled") + "?uploadId=" + url.QueryEscape(initiated.UploadId)}
}

func (u partUpload) copyPart(header http.Header) *httptest.ResponseRecorder {
	header = header.Clone()
	if header.Get("X-Amz-Copy-Source") == "" {
		header.Set("X-Amz-Copy-Source", strings.TrimPrefix(sdkPath("obj"), "/"))
	}
	return u.g.do(http.MethodPut, u.target+"&partNumber=1", nil, header)
}

// parts answers the upload's parts by number with their ETags.
func (u partUpload) parts() map[int]string {
	u.g.t.Helper()
	rr := u.g.do(http.MethodGet, u.target, nil, nil)
	require.Equal(u.g.t, http.StatusOK, rr.Code, "list parts: %s", rr.Body.String())
	var listed handlers.ListPartsResult
	require.NoError(u.g.t, xml.Unmarshal(rr.Body.Bytes(), &listed))
	got := map[int]string{}
	for _, p := range listed.Parts {
		got[p.PartNumber] = p.ETag
	}
	return got
}

// complete assembles part 1 under etag and answers the assembled body.
func (u partUpload) complete(etag string) string {
	u.g.t.Helper()
	body := "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>" + etag +
		"</ETag></Part></CompleteMultipartUpload>"
	rr := u.g.do(http.MethodPost, u.target, []byte(body), nil)
	require.Equal(u.g.t, http.StatusOK, rr.Code, "complete: %s", rr.Body.String())
	rr = u.g.do(http.MethodGet, sdkPath("assembled"), nil, nil)
	require.Equal(u.g.t, http.StatusOK, rr.Code, "get assembled: %s", rr.Body.String())
	return rr.Body.String()
}

// A part copy runs only when its source conditions hold, and a failing one
// leaves the part already uploaded under that number as it was.
// Verifies: Q-68.
func TestConditionalUploadPartCopyEvaluatesTheSource(t *testing.T) {
	for i, name := range copyCaseNames() {
		t.Run(name, func(t *testing.T) {
			f := newReadFixture(t)
			c := copySourceCases(f)[i]
			u := newPartUpload(t, f.g)
			rr := f.g.do(http.MethodPut, u.target+"&partNumber=1", []byte("seeded"), nil)
			require.Equal(t, http.StatusOK, rr.Code, "upload part: %s", rr.Body.String())
			seeded := rr.Header().Get("ETag")
			mutations := f.g.blob.mutationCount()

			rr = u.copyPart(c.header)

			if c.copies {
				require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
				assert.Equal(t, "0123456789", u.complete(u.parts()[1]))
				return
			}
			requirePreconditionFailed(t, rr)
			assert.Equal(t, mutations, f.g.blob.mutationCount(), "blob calls made by a refused part copy")
			assert.Equal(t, map[int]string{1: seeded}, u.parts())
			assert.Equal(t, "seeded", u.complete(seeded))
		})
	}
}

// An unqualified part-copy source is the current version, versionId=null is
// the retained null version, and a current delete marker hides the key.
// Verifies: Q-68.
func TestConditionalUploadPartCopyUsesTheCurrentVersion(t *testing.T) {
	f := newReadFixture(t)
	f.g.enableVersioning()
	current := f.g.putETag("obj", []byte("newer"))

	u := newPartUpload(t, f.g)
	requirePreconditionFailed(t, u.copyPart(http.Header{"X-Amz-Copy-Source-If-Match": {f.etag}}))
	assert.Empty(t, u.parts())

	rr := u.copyPart(http.Header{"X-Amz-Copy-Source-If-Match": {current}})
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	assert.Equal(t, "newer", u.complete(u.parts()[1]))

	null := strings.TrimPrefix(sdkPath("obj"), "/") + "?versionId=null"
	u = newPartUpload(t, f.g)
	requirePreconditionFailed(t, u.copyPart(http.Header{"X-Amz-Copy-Source": {null}, "X-Amz-Copy-Source-If-Match": {current}}))
	rr = u.copyPart(http.Header{"X-Amz-Copy-Source": {null}, "X-Amz-Copy-Source-If-Match": {f.etag}})
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	assert.Equal(t, "0123456789", u.complete(u.parts()[1]))

	rr = f.g.do(http.MethodDelete, sdkPath("obj"), nil, nil)
	require.Equal(t, http.StatusNoContent, rr.Code, "delete: %s", rr.Body.String())
	u = newPartUpload(t, f.g)
	rr = u.copyPart(http.Header{"X-Amz-Copy-Source-If-Match": {"*"}})
	assert.Equal(t, http.StatusNotFound, rr.Code, "body: %s", rr.Body.String())
	assert.Equal(t, "true", rr.Header().Get("X-Amz-Delete-Marker"))
	assert.Empty(t, u.parts())
}

// A copy whose source a delete marker hides answers 404 and says so, as S3
// does, and writes nothing.
// Verifies: Q-68.
func TestConditionalCopyObjectOfAHiddenSource(t *testing.T) {
	f := newReadFixture(t)
	f.g.enableVersioning()
	rr := f.g.do(http.MethodDelete, sdkPath("obj"), nil, nil)
	require.Equal(t, http.StatusNoContent, rr.Code, "delete: %s", rr.Body.String())
	mutations := f.g.blob.mutationCount()

	header := http.Header{"X-Amz-Copy-Source": {strings.TrimPrefix(sdkPath("obj"), "/")}, "X-Amz-Copy-Source-If-Match": {"*"}}
	rr = f.g.do(http.MethodPut, sdkPath("dest"), nil, header)
	assert.Equal(t, http.StatusNotFound, rr.Code, "body: %s", rr.Body.String())
	assert.Contains(t, rr.Body.String(), "<Code>NoSuchKey</Code>")
	assert.Equal(t, "true", rr.Header().Get("X-Amz-Delete-Marker"))
	assert.Equal(t, mutations, f.g.blob.mutationCount(), "blob calls made by a refused copy")
}

// putVersion writes body to obj on a versioned bucket and answers its version ID
// and ETag.
func (f readFixture) putVersion(t *testing.T, body string) (versionID, etag string) {
	t.Helper()
	rr := f.g.do(http.MethodPut, sdkPath("obj"), []byte(body), nil)
	require.Equal(t, http.StatusOK, rr.Code, "put: %s", rr.Body.String())
	return rr.Header().Get("X-Amz-Version-Id"), rr.Header().Get("ETag")
}

// A part copy of a named version copies that version, evaluates its conditions
// against it and reports it, as S3 does. A well-formed ID with no version behind
// it answers 404 NoSuchVersion, a malformed one 400, and neither stores a part.
// Verifies: Q-68.
func TestConditionalUploadPartCopyOfANamedVersion(t *testing.T) {
	f := newReadFixture(t)
	f.g.enableVersioning()
	older, olderETag := f.putVersion(t, "older")
	f.putVersion(t, "newer")
	named := func(id string) string { return strings.TrimPrefix(sdkPath("obj"), "/") + "?versionId=" + id }

	u := newPartUpload(t, f.g)
	requirePreconditionFailed(t, u.copyPart(http.Header{"X-Amz-Copy-Source": {named(older)}, "X-Amz-Copy-Source-If-Match": {f.etag}}))
	assert.Empty(t, u.parts())
	rr := u.copyPart(http.Header{"X-Amz-Copy-Source": {named(older)}, "X-Amz-Copy-Source-If-Match": {olderETag}})
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	assert.Equal(t, older, rr.Header().Get("X-Amz-Copy-Source-Version-Id"))
	assert.Equal(t, "older", u.complete(u.parts()[1]))

	rr = f.g.do(http.MethodDelete, sdkPath("obj")+"?versionId="+older, nil, nil)
	require.Equal(t, http.StatusNoContent, rr.Code, "delete version: %s", rr.Body.String())
	rr = f.g.do(http.MethodDelete, sdkPath("obj"), nil, nil)
	require.Equal(t, http.StatusNoContent, rr.Code, "delete: %s", rr.Body.String())
	marker := rr.Header().Get("X-Amz-Version-Id")
	require.NotEmpty(t, marker)

	// S3 answers a named delete marker 400 InvalidRequest; a marker here has no
	// record its ID can reach, so it answers as a version that is gone.
	for name, want := range map[string]struct {
		id     string
		status int
		code   string
	}{
		"deleted version": {older, http.StatusNotFound, "NoSuchVersion"},
		"delete marker":   {marker, http.StatusNotFound, "NoSuchVersion"},
		"malformed":       {"zzz", http.StatusBadRequest, "InvalidArgument"},
	} {
		u := newPartUpload(t, f.g)
		mutations := f.g.blob.mutationCount()
		rr := u.copyPart(http.Header{"X-Amz-Copy-Source": {named(want.id)}})
		assert.Equal(t, want.status, rr.Code, "%s: %s", name, rr.Body.String())
		assert.Contains(t, rr.Body.String(), "<Code>"+want.code+"</Code>", name)
		assert.Equal(t, mutations, f.g.blob.mutationCount(), "%s: blob calls made by a refused part copy", name)
		assert.Empty(t, u.parts(), name)
	}
}

// A part copy of a named version is authorized as s3:GetObjectVersion on the
// source through the whole signed gate: a caller who may write the upload and
// read the current source, but not its history, is refused and stores nothing.
// Verifies: Q-68.
func TestNamedPartCopyNeedsGetObjectVersion(t *testing.T) {
	blob := newMemBlob()
	cfg := copyServer(t).cfg
	cfg.Blob, cfg.RS, cfg.BlobNodeIDs = blob, RS{Data: 2, Parity: 1}, []config.NodeID{1, 2, 3}
	server := newTestGate(t, cfg)
	do := func(accessKey, method, target string, body []byte, header http.Header) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, target, bytes.NewReader(body))
		req.ContentLength = int64(len(body))
		maps.Copy(req.Header, header)
		signTestReq(t, req, body, accessKey, secret, "ap-southeast-2", "s3")
		rr := httptest.NewRecorder()
		server.ServeHTTP(rr, req)
		return rr
	}

	rr := do(keyOwner, http.MethodPut, "/owner-bucket?versioning", []byte(
		"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"), nil)
	require.Equal(t, http.StatusOK, rr.Code, "enable versioning: %s", rr.Body.String())
	rr = do(keyOwner, http.MethodPut, "/owner-bucket/src", []byte("older"), nil)
	require.Equal(t, http.StatusOK, rr.Code, "put: %s", rr.Body.String())
	older := rr.Header().Get("X-Amz-Version-Id")
	require.Equal(t, http.StatusOK, do(keyOwner, http.MethodPut, "/owner-bucket/src", []byte("newer"), nil).Code)
	rr = do(keyOwner, http.MethodPost, "/owner-bucket/assembled?uploads", nil, nil)
	require.Equal(t, http.StatusOK, rr.Code, "initiate: %s", rr.Body.String())
	var initiated handlers.InitiateMultipartUploadResult
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &initiated))
	upload := "/owner-bucket/assembled?uploadId=" + url.QueryEscape(initiated.UploadId)
	mutations := blob.mutationCount()

	rr = do(keyNoVersions, http.MethodPut, upload+"&partNumber=1", nil,
		http.Header{"X-Amz-Copy-Source": {"owner-bucket/src?versionId=" + older}})
	assert.Equal(t, http.StatusForbidden, rr.Code, "body: %s", rr.Body.String())
	assert.Contains(t, rr.Body.String(), "<Code>AccessDenied</Code>")
	assert.Equal(t, mutations, blob.mutationCount(), "blob calls made by a refused part copy")

	parts := do(keyOwner, http.MethodGet, upload, nil, nil)
	require.Equal(t, http.StatusOK, parts.Code, "list parts: %s", parts.Body.String())
	assert.NotContains(t, parts.Body.String(), "<PartNumber>")

	rr = do(keyNoVersions, http.MethodPut, upload+"&partNumber=1", nil,
		http.Header{"X-Amz-Copy-Source": {"owner-bucket/src"}})
	assert.Equal(t, http.StatusOK, rr.Code, "the current version needs s3:GetObject only: %s", rr.Body.String())
}
