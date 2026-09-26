package handlers

import (
	"encoding/xml"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mulgadc/bluebottle/pkg/sigv4"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errBody fails every read, standing in for the SigV4-verified body that reports a rewritten
// payload to whoever reads it.
type errBody struct{ err error }

func (b errBody) Read([]byte) (int, error) { return 0, b.err }
func (b errBody) Close() error             { return nil }

// TestCreateBucketRejectsUnreadableConfiguration covers the read that carries the payload
// check. Discarding its error created the bucket under a location constraint that failed its
// signed digest, or under none at all when the body never arrived.
func TestCreateBucketRejectsUnreadableConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		code string
	}{
		{name: "payload digest mismatch", err: sigv4.ErrContentSHA256Mismatch, code: "XAmzContentSHA256Mismatch"},
		{name: "read failure", err: errors.New("connection reset by peer"), code: "MalformedXML"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPut, "/"+testBucket, errBody{err: tc.err})
			req.ContentLength = 128
			req = req.WithContext(WithBucket(req.Context(), model.Bucket{Name: testBucket}))
			w := httptest.NewRecorder()

			// The nil MetaClient is the assertion: reaching the store at all means the
			// handler carried on past a body it could not read.
			CreateBucket(nil, testCache(), Config{Region: "ap-southeast-2"}).ServeHTTP(w, req)

			require.Equal(t, http.StatusBadRequest, w.Code, "body: %s", w.Body.String())

			var s3err S3Error
			require.NoError(t, xml.NewDecoder(w.Body).Decode(&s3err))
			assert.Equal(t, tc.code, s3err.Code)
		})
	}
}

// createBucketWithBody drives CreateBucket the way the router leaves a request,
// with a configuration document the handler has to read.
func createBucketWithBody(mc MetaClient, cache *BucketCache, body string) *httptest.ResponseRecorder {
	req := subResourceRequest(http.MethodPut, "/"+testBucket, body)
	req.ContentLength = int64(len(body))
	w := httptest.NewRecorder()
	CreateBucket(mc, cache, Config{Region: "ap-southeast-2"}).ServeHTTP(w, req)
	return w
}

// emptyCache is a cluster the bucket under test does not exist in yet, which is
// what CreateBucket needs: testCache seeds it and the create answers
// BucketAlreadyOwnedByYou.
func emptyCache() *BucketCache { return NewBucketCache(nil) }

// The AWS provider sends a bucket's tags inside CreateBucket and only falls
// back to PutBucketTagging when the create refuses them, so accepting the body
// and dropping the tags loses them silently — the apply reports success and the
// bucket comes back untagged.
func TestCreateBucketAppliesTagsFromTheConfiguration(t *testing.T) {
	t.Parallel()

	mc := newFakeMeta()
	cache := emptyCache()

	w := createBucketWithBody(mc, cache, `<CreateBucketConfiguration>`+
		`<LocationConstraint>ap-southeast-2</LocationConstraint>`+
		`<Tags><Tag><Key>Name</Key><Value>uploads</Value></Tag>`+
		`<Tag><Key>Example</Key><Value>s3-webapp</Value></Tag></Tags>`+
		`</CreateBucketConfiguration>`)
	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())

	w = httptest.NewRecorder()
	GetBucketTagging(mc, cache).ServeHTTP(w, subResourceRequest(http.MethodGet, "/"+testBucket+"?tagging", ""))
	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())

	var got Tagging
	require.NoError(t, xml.NewDecoder(w.Body).Decode(&got))
	assert.Equal(t, []Tag{{Key: "Example", Value: "s3-webapp"}, {Key: "Name", Value: "uploads"}}, got.TagSet)
}

// The Location header names the bucket on the endpoint that served the create.
// It used to be built as an s3.<region>.amazonaws.com URL, which is a host this
// deployment does not serve and a client that follows it leaves the cluster.
func TestCreateBucketReportsALocationOnThisEndpoint(t *testing.T) {
	t.Parallel()

	w := createBucketWithBody(newFakeMeta(), emptyCache(),
		`<CreateBucketConfiguration><LocationConstraint>ap-southeast-2</LocationConstraint></CreateBucketConfiguration>`)
	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
	assert.Equal(t, "/"+testBucket, w.Header().Get("Location"))
}

// A configuration carrying no tags leaves the bucket untagged rather than
// carrying an empty tag set, which is a different answer to GetBucketTagging.
func TestCreateBucketWithoutTagsLeavesTheBucketUntagged(t *testing.T) {
	t.Parallel()

	mc := newFakeMeta()
	cache := emptyCache()

	w := createBucketWithBody(mc, cache,
		`<CreateBucketConfiguration><LocationConstraint>ap-southeast-2</LocationConstraint></CreateBucketConfiguration>`)
	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())

	w = httptest.NewRecorder()
	GetBucketTagging(mc, cache).ServeHTTP(w, subResourceRequest(http.MethodGet, "/"+testBucket+"?tagging", ""))
	require.Equal(t, http.StatusNotFound, w.Code)
	assert.Equal(t, "NoSuchTagSet", decodeS3Error(t, w).Code)
}

// The tag set is validated before the bucket is stored: a create that reports
// success having dropped an invalid tag leaves the caller believing it applied.
func TestCreateBucketRefusesAnInvalidTagAndCreatesNothing(t *testing.T) {
	t.Parallel()

	mc := newFakeMeta()
	cache := emptyCache()

	w := createBucketWithBody(mc, cache, `<CreateBucketConfiguration>`+
		`<Tags><Tag><Key>aws:managed</Key><Value>x</Value></Tag></Tags>`+
		`</CreateBucketConfiguration>`)
	require.Equal(t, http.StatusBadRequest, w.Code, "body: %s", w.Body.String())
	assert.Equal(t, "InvalidTag", decodeS3Error(t, w).Code)

	w = httptest.NewRecorder()
	GetBucketTagging(mc, cache).ServeHTTP(w, subResourceRequest(http.MethodGet, "/"+testBucket+"?tagging", ""))
	assert.Equal(t, http.StatusNotFound, w.Code)
}
