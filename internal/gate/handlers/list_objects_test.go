package handlers

import (
	"context"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"testing"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const listTestBucket = "b"

// listFixture is a bucket whose contents are listing rows in global state. The
// rows carry no object hash, so sizes read back as zero; paging does not depend
// on them.
type listFixture struct {
	mc      *fakeMeta
	handler http.Handler
}

func newListFixture(t *testing.T, keys ...string) listFixture {
	t.Helper()
	mc := newFakeMeta()
	for _, key := range keys {
		require.NoError(t, metaPut(context.Background(), mc, model.TableObjects,
			objectARN(listTestBucket, key), []byte("row")))
	}
	cache := NewBucketCache([]BucketConfig{{Name: listTestBucket, Region: "ap-southeast-2"}})
	return listFixture{mc: mc, handler: ListObjects(mc, cache)}
}

// list issues one ListObjectsV2 request and decodes the answer.
func (f listFixture) list(t *testing.T, query url.Values) ListObjectsV2 {
	t.Helper()
	rr := f.do(t, query)
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var result ListObjectsV2
	require.NoError(t, xml.NewDecoder(rr.Body).Decode(&result))
	return result
}

func (f listFixture) do(t *testing.T, query url.Values) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/"+listTestBucket+"?"+query.Encode(), nil)
	req = req.WithContext(WithBucket(req.Context(), model.Bucket{Name: listTestBucket}))
	rr := httptest.NewRecorder()
	f.handler.ServeHTTP(rr, req)
	return rr
}

// walk pages the bucket to exhaustion the way an SDK paginator does, returning
// every entry in the order the server produced it.
func (f listFixture) walk(t *testing.T, query url.Values, pageSize int) []string {
	t.Helper()
	var got []string
	page := url.Values{}
	maps.Copy(page, query)
	page.Set("max-keys", strconv.Itoa(pageSize))

	for i := 0; ; i++ {
		require.Less(t, i, 100, "paginator did not terminate")
		result := f.list(t, page)

		require.LessOrEqual(t, result.KeyCount, pageSize)

		// The document carries objects and common prefixes as two lists, so
		// their interleaving is not on the wire and a client reassembles it.
		// Every entry on a page still sorts before every entry on the next.
		var batch []string
		for _, c := range contentsOf(result) {
			batch = append(batch, c.Key)
		}
		for _, p := range prefixesOf(result) {
			batch = append(batch, p.Prefix)
		}
		sort.Strings(batch)
		got = append(got, batch...)

		if !result.IsTruncated {
			require.Empty(t, result.NextContinuationToken,
				"a complete listing must not offer a token to follow")
			return got
		}
		require.NotEmpty(t, result.NextContinuationToken,
			"a truncated listing the client cannot resume is a silent short read")
		page.Set("continuation-token", result.NextContinuationToken)
	}
}

// An empty slice encodes to no elements at all, so the decoder leaves the
// pointer nil rather than pointing at an empty slice.
func contentsOf(r ListObjectsV2) []ListObjectsV2_Contents {
	if r.Contents == nil {
		return nil
	}
	return *r.Contents
}

func prefixesOf(r ListObjectsV2) []ListObjectsV2_Dir {
	if r.CommonPrefixes == nil {
		return nil
	}
	return *r.CommonPrefixes
}

func listKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%03d", i)
	}
	return keys
}

// The defect this work exists for: a client paging a bucket larger than one
// page either saw the whole bucket at once or stopped after the first page.
func TestListObjectsPagesEveryKeyExactlyOnce(t *testing.T) {
	t.Parallel()

	want := listKeys(25)
	f := newListFixture(t, want...)

	assert.Equal(t, want, f.walk(t, url.Values{}, 4))
}

func TestListObjectsHonoursMaxKeys(t *testing.T) {
	t.Parallel()

	f := newListFixture(t, listKeys(10)...)
	result := f.list(t, url.Values{"max-keys": {"3"}})

	assert.True(t, result.IsTruncated)
	assert.Equal(t, 3, result.KeyCount)
	assert.Equal(t, 3, result.MaxKeys)
	assert.Len(t, contentsOf(result), 3)
}

// MaxKeys used to report 1000 whatever the client asked for, which contradicts
// the body it sits beside.
func TestListObjectsMaxKeysClampsAndRejects(t *testing.T) {
	t.Parallel()

	f := newListFixture(t, listKeys(3)...)

	t.Run("above the ceiling clamps", func(t *testing.T) {
		t.Parallel()
		result := f.list(t, url.Values{"max-keys": {"5000"}})
		assert.Equal(t, defaultMaxKeys, result.MaxKeys)
		assert.False(t, result.IsTruncated)
	})

	t.Run("absent is the default", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, defaultMaxKeys, f.list(t, url.Values{}).MaxKeys)
	})

	// Not truncated: a truncation flag with no token to follow either stalls
	// the client or loops it on the same empty page.
	t.Run("zero is an empty page", func(t *testing.T) {
		t.Parallel()
		result := f.list(t, url.Values{"max-keys": {"0"}})
		assert.Equal(t, 0, result.KeyCount)
		assert.False(t, result.IsTruncated)
		assert.Empty(t, result.NextContinuationToken)
	})

	for _, bad := range []string{"-1", "not-a-number", "1.5"} {
		t.Run("rejects "+bad, func(t *testing.T) {
			t.Parallel()
			rr := f.do(t, url.Values{"max-keys": {bad}})
			assert.Equal(t, http.StatusBadRequest, rr.Code)

			var s3err S3Error
			require.NoError(t, xml.NewDecoder(rr.Body).Decode(&s3err))
			assert.Equal(t, string(model.ErrInvalidArgument), s3err.Code)
		})
	}
}

func TestListObjectsStartAfter(t *testing.T) {
	t.Parallel()

	f := newListFixture(t, "a", "b", "c", "d")
	result := f.list(t, url.Values{"start-after": {"b"}})

	assert.Equal(t, 2, result.KeyCount)
	assert.Equal(t, "c", (contentsOf(result))[0].Key)
	assert.Equal(t, "b", result.StartAfter, "S3 echoes the parameter back")
}

func TestListObjectsStartAfterComposesWithPrefix(t *testing.T) {
	t.Parallel()

	f := newListFixture(t, "logs/1", "logs/2", "logs/3", "other/1")
	result := f.list(t, url.Values{"prefix": {"logs/"}, "start-after": {"logs/1"}})

	require.Len(t, contentsOf(result), 2)
	assert.Equal(t, "logs/2", (contentsOf(result))[0].Key)
	assert.Equal(t, "logs/3", (contentsOf(result))[1].Key)
}

// A continuation token is opaque, so a client sending one back must not have to
// know it is a key. It also wins over start-after, which is what S3 does.
func TestListObjectsContinuationTokenSupersedesStartAfter(t *testing.T) {
	t.Parallel()

	f := newListFixture(t, "a", "b", "c", "d")
	token := base64.StdEncoding.EncodeToString([]byte("c"))

	result := f.list(t, url.Values{
		"continuation-token": {token},
		"start-after":        {"a"},
	})

	require.Len(t, contentsOf(result), 1)
	assert.Equal(t, "d", (contentsOf(result))[0].Key)
	assert.Equal(t, token, result.ContinuationToken)
}

func TestListObjectsRejectsUndecodableToken(t *testing.T) {
	t.Parallel()

	f := newListFixture(t, "a")
	rr := f.do(t, url.Values{"continuation-token": {"not!base64"}})

	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

// A delimiter collapses many keys into one row, so a page boundary can land on
// a common prefix. Resuming from one has to skip everything underneath it
// rather than re-listing the keys it stands for.
func TestListObjectsPagesCommonPrefixes(t *testing.T) {
	t.Parallel()

	f := newListFixture(t,
		"a/1", "a/2", "a/3",
		"b/1", "b/2",
		"c/1",
		"top",
	)

	got := f.walk(t, url.Values{"delimiter": {"/"}}, 2)
	assert.Equal(t, []string{"a/", "b/", "c/", "top"}, got)
}

// Common prefixes count against max-keys the same as keys do. Counting only
// objects lets a listing of nothing but directories never terminate.
func TestListObjectsCountsCommonPrefixesTowardsTheLimit(t *testing.T) {
	t.Parallel()

	f := newListFixture(t, "a/1", "b/1", "c/1")
	result := f.list(t, url.Values{"delimiter": {"/"}, "max-keys": {"2"}})

	assert.Equal(t, 2, result.KeyCount)
	assert.Len(t, prefixesOf(result), 2)
	assert.Empty(t, contentsOf(result))
	assert.True(t, result.IsTruncated)
}

func TestListObjectsEchoesDelimiter(t *testing.T) {
	t.Parallel()

	f := newListFixture(t, "a/1")
	assert.Equal(t, "/", f.list(t, url.Values{"delimiter": {"/"}}).Delimiter)
}

// Every page size must produce the same listing, or the cursor is losing or
// repeating entries at some boundary a single page size would not reveal.
func TestListObjectsPagingIsIndependentOfPageSize(t *testing.T) {
	t.Parallel()

	keys := listKeys(17)
	f := newListFixture(t, keys...)

	for _, size := range []int{1, 2, 3, 5, 16, 17, 18, 100} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, keys, f.walk(t, url.Values{}, size))
		})
	}
}
