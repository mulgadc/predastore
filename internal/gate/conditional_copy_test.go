package gate

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

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
	for _, c := range copySourceCases(newReadFixture(t)) {
		t.Run(c.name, func(t *testing.T) {
			f := newReadFixture(t)
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

// A part copy runs only when its source conditions hold, and a failing one
// stores no part.
// Verifies: Q-68.
func TestConditionalUploadPartCopyEvaluatesTheSource(t *testing.T) {
	for _, c := range copySourceCases(newReadFixture(t)) {
		t.Run(c.name, func(t *testing.T) {
			f := newReadFixture(t)
			rr := f.g.do(http.MethodPost, sdkPath("assembled")+"?uploads", nil, nil)
			require.Equal(t, http.StatusOK, rr.Code, "initiate: %s", rr.Body.String())
			var initiated handlers.InitiateMultipartUploadResult
			require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &initiated))
			upload := sdkPath("assembled") + "?uploadId=" + url.QueryEscape(initiated.UploadId)
			mutations := f.g.blob.mutationCount()

			header := c.header.Clone()
			header.Set("X-Amz-Copy-Source", strings.TrimPrefix(sdkPath("obj"), "/"))
			rr = f.g.do(http.MethodPut, upload+"&partNumber=1", nil, header)

			parts := f.g.do(http.MethodGet, upload, nil, nil)
			require.Equal(t, http.StatusOK, parts.Code, "list parts: %s", parts.Body.String())
			if c.copies {
				require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
				assert.Contains(t, parts.Body.String(), "<PartNumber>1</PartNumber>")
				return
			}
			requirePreconditionFailed(t, rr)
			assert.Equal(t, mutations, f.g.blob.mutationCount(), "blob calls made by a refused part copy")
			assert.NotContains(t, parts.Body.String(), "<PartNumber>")
		})
	}
}
