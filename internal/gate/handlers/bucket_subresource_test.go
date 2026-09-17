package handlers

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// subResourceRequest builds a request the way the router leaves one for a
// bucket-scoped handler: the resource already resolved onto the context.
func subResourceRequest(method, target string, body string) *http.Request {
	var req *http.Request
	if body == "" {
		req = httptest.NewRequest(method, target, nil)
	} else {
		req = httptest.NewRequest(method, target, strings.NewReader(body))
	}
	return req.WithContext(WithBucket(req.Context(), model.Bucket{Name: testBucket}))
}

func decodeS3Error(t *testing.T, w *httptest.ResponseRecorder) S3Error {
	t.Helper()
	var s3err S3Error
	require.NoError(t, xml.NewDecoder(w.Body).Decode(&s3err))
	return s3err
}

// TestBucketTaggingRoundTrip is the gap's whole point: the AWS provider issues
// PutBucketTagging on every bucket it creates when default_tags is set, so a
// write that does not read back leaves Terraform unable to converge.
func TestBucketTaggingRoundTrip(t *testing.T) {
	t.Parallel()

	mc := newFakeMeta()
	cache := testCache()

	w := httptest.NewRecorder()
	PutBucketTagging(mc, cache).ServeHTTP(w, subResourceRequest(http.MethodPut, "/"+testBucket+"?tagging",
		`<Tagging><TagSet><Tag><Key>env</Key><Value>dev</Value></Tag><Tag><Key>owner</Key><Value>platform</Value></Tag></TagSet></Tagging>`))
	require.Equal(t, http.StatusNoContent, w.Code, "body: %s", w.Body.String())

	w = httptest.NewRecorder()
	GetBucketTagging(mc, cache).ServeHTTP(w, subResourceRequest(http.MethodGet, "/"+testBucket+"?tagging", ""))
	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())

	var got Tagging
	require.NoError(t, xml.NewDecoder(w.Body).Decode(&got))
	assert.Equal(t, []Tag{{Key: "env", Value: "dev"}, {Key: "owner", Value: "platform"}}, got.TagSet)
}

// TestPutBucketTaggingReplacesTheWholeSet pins S3's semantics rather than a
// merge: a tag dropped from the document has to disappear from the bucket.
func TestPutBucketTaggingReplacesTheWholeSet(t *testing.T) {
	t.Parallel()

	mc := newFakeMeta()
	cache := testCache()

	w := httptest.NewRecorder()
	PutBucketTagging(mc, cache).ServeHTTP(w, subResourceRequest(http.MethodPut, "/"+testBucket+"?tagging",
		`<Tagging><TagSet><Tag><Key>a</Key><Value>1</Value></Tag><Tag><Key>b</Key><Value>2</Value></Tag></TagSet></Tagging>`))
	require.Equal(t, http.StatusNoContent, w.Code)

	w = httptest.NewRecorder()
	PutBucketTagging(mc, cache).ServeHTTP(w, subResourceRequest(http.MethodPut, "/"+testBucket+"?tagging",
		`<Tagging><TagSet><Tag><Key>b</Key><Value>two</Value></Tag></TagSet></Tagging>`))
	require.Equal(t, http.StatusNoContent, w.Code)

	w = httptest.NewRecorder()
	GetBucketTagging(mc, cache).ServeHTTP(w, subResourceRequest(http.MethodGet, "/"+testBucket+"?tagging", ""))
	require.Equal(t, http.StatusOK, w.Code)

	var got Tagging
	require.NoError(t, xml.NewDecoder(w.Body).Decode(&got))
	assert.Equal(t, []Tag{{Key: "b", Value: "two"}}, got.TagSet)
}

// TestGetBucketTaggingOnUntaggedBucket keeps "none configured" distinguishable
// from "none readable": an empty TagSet would tell a caller the read worked.
func TestGetBucketTaggingOnUntaggedBucket(t *testing.T) {
	t.Parallel()

	w := httptest.NewRecorder()
	GetBucketTagging(newFakeMeta(), testCache()).ServeHTTP(w,
		subResourceRequest(http.MethodGet, "/"+testBucket+"?tagging", ""))

	require.Equal(t, http.StatusNotFound, w.Code, "body: %s", w.Body.String())
	assert.Equal(t, "NoSuchTagSet", decodeS3Error(t, w).Code)
}

func TestDeleteBucketTagging(t *testing.T) {
	t.Parallel()

	mc := newFakeMeta()
	cache := testCache()

	w := httptest.NewRecorder()
	PutBucketTagging(mc, cache).ServeHTTP(w, subResourceRequest(http.MethodPut, "/"+testBucket+"?tagging",
		`<Tagging><TagSet><Tag><Key>a</Key><Value>1</Value></Tag></TagSet></Tagging>`))
	require.Equal(t, http.StatusNoContent, w.Code)

	w = httptest.NewRecorder()
	DeleteBucketTagging(mc, cache).ServeHTTP(w, subResourceRequest(http.MethodDelete, "/"+testBucket+"?tagging", ""))
	require.Equal(t, http.StatusNoContent, w.Code, "body: %s", w.Body.String())

	w = httptest.NewRecorder()
	GetBucketTagging(mc, cache).ServeHTTP(w, subResourceRequest(http.MethodGet, "/"+testBucket+"?tagging", ""))
	require.Equal(t, http.StatusNotFound, w.Code)

	// Idempotent, as on S3: the second delete is not an error.
	w = httptest.NewRecorder()
	DeleteBucketTagging(mc, cache).ServeHTTP(w, subResourceRequest(http.MethodDelete, "/"+testBucket+"?tagging", ""))
	assert.Equal(t, http.StatusNoContent, w.Code)
}

func TestPutBucketTaggingRejectsInvalidTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		code string
	}{
		{name: "not well-formed", body: `<Tagging><TagSet>`, code: "MalformedXML"},
		{name: "empty key", body: `<Tagging><TagSet><Tag><Key></Key><Value>v</Value></Tag></TagSet></Tagging>`, code: "InvalidTag"},
		{name: "reserved prefix", body: `<Tagging><TagSet><Tag><Key>aws:name</Key><Value>v</Value></Tag></TagSet></Tagging>`, code: "InvalidTag"},
		{name: "key too long", body: `<Tagging><TagSet><Tag><Key>` + strings.Repeat("k", 129) + `</Key><Value>v</Value></Tag></TagSet></Tagging>`, code: "InvalidTag"},
		{name: "value too long", body: `<Tagging><TagSet><Tag><Key>k</Key><Value>` + strings.Repeat("v", 257) + `</Value></Tag></TagSet></Tagging>`, code: "InvalidTag"},
		{name: "duplicate key", body: `<Tagging><TagSet><Tag><Key>k</Key><Value>1</Value></Tag><Tag><Key>k</Key><Value>2</Value></Tag></TagSet></Tagging>`, code: "InvalidTag"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mc := newFakeMeta()
			w := httptest.NewRecorder()
			PutBucketTagging(mc, testCache()).ServeHTTP(w,
				subResourceRequest(http.MethodPut, "/"+testBucket+"?tagging", tc.body))

			require.Equal(t, http.StatusBadRequest, w.Code, "body: %s", w.Body.String())
			assert.Equal(t, tc.code, decodeS3Error(t, w).Code)
			assert.Empty(t, mc.rows, "a rejected tag set must not be stored")
		})
	}
}

// TestGetBucketLocationReturnsTheBucketRegion covers the legacy region-resolution
// path: a null constraint means us-east-1, so an SDK taking this route signs for
// the wrong region.
func TestGetBucketLocationReturnsTheBucketRegion(t *testing.T) {
	t.Parallel()

	w := httptest.NewRecorder()
	GetBucketLocation(newFakeMeta(), testCache()).ServeHTTP(w,
		subResourceRequest(http.MethodGet, "/"+testBucket+"?location", ""))

	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())

	var got LocationConstraint
	require.NoError(t, xml.NewDecoder(w.Body).Decode(&got))
	assert.Equal(t, "ap-southeast-2", got.Value)
}

// TestGetBucketLocationIsEmptyForUsEast1 is the one case where the null reading
// is correct, and the only reason an empty answer is ever right.
func TestGetBucketLocationIsEmptyForUsEast1(t *testing.T) {
	t.Parallel()

	cache := NewBucketCache([]BucketConfig{{Name: testBucket, Region: "us-east-1"}})
	w := httptest.NewRecorder()
	GetBucketLocation(newFakeMeta(), cache).ServeHTTP(w,
		subResourceRequest(http.MethodGet, "/"+testBucket+"?location", ""))

	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())

	var got LocationConstraint
	require.NoError(t, xml.NewDecoder(w.Body).Decode(&got))
	assert.Empty(t, got.Value)
}

// refusedBucketWrites is written out rather than read from the handler's own
// list, so dropping an entry from that list fails here instead of quietly
// removing its case and leaving the sub-resource answering as CreateBucket.
var refusedBucketWrites = []string{
	"policy", "acl", "versioning", "encryption", "lifecycle", "publicAccessBlock",
	"ownershipControls", "cors", "object-lock", "notification", "logging",
	"replication", "website", "accelerate", "requestPayment", "analytics",
	"intelligent-tiering", "inventory", "metrics",
}

func TestUnsupportedBucketWritesAreRefusedByName(t *testing.T) {
	t.Parallel()

	for _, param := range refusedBucketWrites {
		t.Run(param, func(t *testing.T) {
			t.Parallel()

			w := httptest.NewRecorder()
			// The nil MetaClient is the assertion: reaching the store means the
			// handler carried on into the create path.
			CreateBucket(nil, testCache(), Config{Region: "ap-southeast-2"}).ServeHTTP(w,
				subResourceRequest(http.MethodPut, "/"+testBucket+"?"+param, ""))

			require.Equal(t, http.StatusNotImplemented, w.Code, "body: %s", w.Body.String())
			assert.Equal(t, "NotImplemented", decodeS3Error(t, w).Code)
		})
	}
}

// TestEveryRefusedWriteIsAccountedFor catches the other direction: a
// sub-resource added to the handler's list without a decision recorded here.
func TestEveryRefusedWriteIsAccountedFor(t *testing.T) {
	t.Parallel()

	declared := make([]string, 0, len(unsupportedBucketWrites))
	for _, sub := range unsupportedBucketWrites {
		declared = append(declared, sub.param)
	}
	assert.ElementsMatch(t, refusedBucketWrites, declared)
}

// TestCreateBucketStillServesAPlainPut guards the refusal list against matching
// too widely: the SDKs append x-id to ordinary requests.
func TestCreateBucketStillServesAPlainPut(t *testing.T) {
	t.Parallel()

	mc := newFakeMeta()
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/fresh?x-id=CreateBucket", nil)
	req = req.WithContext(WithBucket(req.Context(), model.Bucket{Name: "fresh"}))

	CreateBucket(mc, testCache(), Config{Region: "ap-southeast-2"}).ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
}
