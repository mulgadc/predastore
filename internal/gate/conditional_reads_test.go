package gate

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const httpDate = http.TimeFormat

// readFixture is one object and the validators the gate served for it.
type readFixture struct {
	g        keyGate
	etag     string
	modified time.Time
}

func newReadFixture(t *testing.T) readFixture {
	t.Helper()
	g := newKeyGate(t)
	etag := g.putETag("obj", []byte("0123456789"))
	rr := g.do(http.MethodHead, sdkPath("obj"), nil, nil)
	require.Equal(t, http.StatusOK, rr.Code)
	modified, err := http.ParseTime(rr.Header().Get("Last-Modified"))
	require.NoError(t, err)
	return readFixture{g: g, etag: etag, modified: modified}
}

func (f readFixture) date(offset time.Duration) string {
	return f.modified.Add(offset).UTC().Format(httpDate)
}

// Each read condition, alone and in the two pairs S3 documents, answers as S3
// does on both GET and HEAD.
// Verifies: Q-68.
func TestConditionalReadsAnswerAsS3Does(t *testing.T) {
	f := newReadFixture(t)
	cases := []struct {
		name   string
		header http.Header
		want   int
	}{
		{"If-Match current", http.Header{"If-Match": {f.etag}}, http.StatusOK},
		{"If-Match unquoted", http.Header{"If-Match": {f.etag[1 : len(f.etag)-1]}}, http.StatusOK},
		{"If-Match list", http.Header{"If-Match": {`"other", ` + f.etag}}, http.StatusOK},
		{"If-Match star", http.Header{"If-Match": {"*"}}, http.StatusOK},
		{"If-Match other", http.Header{"If-Match": {`"other"`}}, http.StatusPreconditionFailed},
		{"If-None-Match current", http.Header{"If-None-Match": {f.etag}}, http.StatusNotModified},
		{"If-None-Match star", http.Header{"If-None-Match": {"*"}}, http.StatusNotModified},
		{"If-None-Match other", http.Header{"If-None-Match": {`"other"`}}, http.StatusOK},
		{"If-Modified-Since at modification", http.Header{"If-Modified-Since": {f.date(0)}}, http.StatusNotModified},
		{"If-Modified-Since before", http.Header{"If-Modified-Since": {f.date(-time.Hour)}}, http.StatusOK},
		{"If-Unmodified-Since at modification", http.Header{"If-Unmodified-Since": {f.date(0)}}, http.StatusOK},
		{"If-Unmodified-Since before", http.Header{"If-Unmodified-Since": {f.date(-time.Hour)}}, http.StatusPreconditionFailed},
		{"If-Modified-Since unparseable", http.Header{"If-Modified-Since": {"yesterday"}}, http.StatusOK},
		{"If-Unmodified-Since unparseable", http.Header{"If-Unmodified-Since": {"yesterday"}}, http.StatusOK},
		{"If-Match true overrides If-Unmodified-Since false",
			http.Header{"If-Match": {f.etag}, "If-Unmodified-Since": {f.date(-time.Hour)}}, http.StatusOK},
		{"If-None-Match false with If-Modified-Since true",
			http.Header{"If-None-Match": {f.etag}, "If-Modified-Since": {f.date(-time.Hour)}}, http.StatusNotModified},
		{"If-None-Match true overrides If-Modified-Since false",
			http.Header{"If-None-Match": {`"other"`}, "If-Modified-Since": {f.date(0)}}, http.StatusOK},
	}
	for _, c := range cases {
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			t.Run(method+" "+c.name, func(t *testing.T) {
				rr := f.g.do(method, sdkPath("obj"), nil, c.header)
				require.Equal(t, c.want, rr.Code, "body: %s", rr.Body.String())
				switch {
				case c.want == http.StatusOK && method == http.MethodGet:
					assert.Equal(t, "0123456789", rr.Body.String())
				case c.want == http.StatusNotModified:
					assert.Empty(t, rr.Body.String(), "a 304 carries no body")
					assert.Equal(t, f.etag, rr.Header().Get("ETag"))
					assert.Equal(t, f.date(0), rr.Header().Get("Last-Modified"))
				case c.want == http.StatusPreconditionFailed && method == http.MethodGet:
					assert.Contains(t, rr.Body.String(), "<Code>PreconditionFailed</Code>")
				case method == http.MethodHead:
					assert.Empty(t, rr.Body.String(), "a HEAD answer carries no body")
				}
			})
		}
	}
}

// A ranged read whose condition fails answers 412 and serves no range.
// Verifies: Q-68.
func TestConditionalRangedReadFailsBeforeTheRange(t *testing.T) {
	f := newReadFixture(t)

	rr := f.g.do(http.MethodGet, sdkPath("obj"), nil, http.Header{
		"If-Match": {`"other"`},
		"Range":    {"bytes=2-5"},
	})

	require.Equal(t, http.StatusPreconditionFailed, rr.Code, "body: %s", rr.Body.String())
	assert.Empty(t, rr.Header().Get("Content-Range"))
	assert.Contains(t, rr.Body.String(), "<Code>PreconditionFailed</Code>")
}

// A versionId read evaluates its conditions against that version, not the
// current one.
// Verifies: Q-68.
func TestConditionalVersionReadEvaluatesThatVersion(t *testing.T) {
	g := newKeyGate(t)
	g.enableVersioning()
	rr := g.do(http.MethodPut, sdkPath("obj"), []byte("v1"), nil)
	require.Equal(t, http.StatusOK, rr.Code)
	oldETag, oldVersion := rr.Header().Get("ETag"), rr.Header().Get("X-Amz-Version-Id")
	require.NotEmpty(t, oldVersion)
	g.putETag("obj", []byte("v2"))

	rr = g.do(http.MethodGet, sdkPath("obj")+"?versionId="+oldVersion, nil, http.Header{"If-Match": {oldETag}})
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	assert.Equal(t, "v1", rr.Body.String())

	rr = g.do(http.MethodGet, sdkPath("obj"), nil, http.Header{"If-Match": {oldETag}})
	assert.Equal(t, http.StatusPreconditionFailed, rr.Code, "the current version no longer matches: %s", rr.Body.String())
}
