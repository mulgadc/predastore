package handlers

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const deleteTestBucket = "b"

// deleteFixture is a bucket holding real objects — written through the write
// path and recorded under the keys production records — behind the batch
// delete handler.
type deleteFixture struct {
	write   writeFixture
	handler http.Handler
}

func newDeleteFixture(t *testing.T, keys ...string) deleteFixture {
	t.Helper()
	f := newWriteFixture(4, 2)
	cache := NewBucketCache([]BucketConfig{{Name: deleteTestBucket, Region: "ap-southeast-2"}})

	fixture := deleteFixture{write: f, handler: DeleteObjects(f.mc, cache)}
	for _, key := range keys {
		fixture.seed(t, key, []byte("body of "+key))
	}
	return fixture
}

// seed stores one object the way PutObject does: shards written and committed,
// the placement record under the object hash, and the listing row under the ARN.
func (f deleteFixture) seed(t *testing.T, key string, body []byte) {
	t.Helper()
	ctx := context.Background()
	objectHash := model.ObjectHash(deleteTestBucket, key)

	place, _, err := f.write.write(ctx, objectHash, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err)

	record, err := EncodePlacement(place)
	require.NoError(t, err)
	require.NoError(t, metaPut(ctx, f.write.mc, model.TableObjects, string(objectHash[:]), record))
	require.NoError(t, metaPut(ctx, f.write.mc, model.TableObjects,
		objectARN(deleteTestBucket, key), objectHash[:]))
}

// exists reports whether the object's placement record is still in state, which
// is what a later GET resolves through.
func (f deleteFixture) exists(t *testing.T, key string) bool {
	t.Helper()
	objectHash := model.ObjectHash(deleteTestBucket, key)
	_, err := metaGet(context.Background(), f.write.mc, model.TableObjects, string(objectHash[:]))
	return err == nil
}

// deleteKeys issues one batch delete and decodes the answer.
func (f deleteFixture) deleteKeys(t *testing.T, quiet bool, keys ...string) (*httptest.ResponseRecorder, DeleteResult) {
	t.Helper()
	rr := f.post(t, deleteBody(quiet, keys...))

	var result DeleteResult
	if rr.Code == http.StatusOK {
		require.NoError(t, xml.NewDecoder(bytes.NewReader(rr.Body.Bytes())).Decode(&result),
			"body: %s", rr.Body.String())
	}
	return rr, result
}

func (f deleteFixture) post(t *testing.T, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/"+deleteTestBucket+"?delete=", strings.NewReader(body))
	req = req.WithContext(WithBucket(req.Context(), model.Bucket{Name: deleteTestBucket}))
	rr := httptest.NewRecorder()
	f.handler.ServeHTTP(rr, req)
	return rr
}

func deleteBody(quiet bool, keys ...string) string {
	var b strings.Builder
	b.WriteString("<Delete>")
	if quiet {
		b.WriteString("<Quiet>true</Quiet>")
	}
	for _, key := range keys {
		fmt.Fprintf(&b, "<Object><Key>%s</Key></Object>", key)
	}
	b.WriteString("</Delete>")
	return b.String()
}

func deletedKeys(result DeleteResult) []string {
	keys := make([]string, 0, len(result.Deleted))
	for _, d := range result.Deleted {
		keys = append(keys, d.Key)
	}
	return keys
}

func TestDeleteObjectsRemovesEveryKey(t *testing.T) {
	f := newDeleteFixture(t, "a.txt", "b.txt", "sub/c.txt")

	rr, result := f.deleteKeys(t, false, "a.txt", "b.txt", "sub/c.txt")

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Empty(t, result.Errors)
	// The answer follows the order the client asked in.
	assert.Equal(t, []string{"a.txt", "b.txt", "sub/c.txt"}, deletedKeys(result))

	for _, key := range []string{"a.txt", "b.txt", "sub/c.txt"} {
		assert.False(t, f.exists(t, key), "%s survived the batch delete", key)
	}
}

// A key that is not there is deleted, not an error: a client emptying a bucket
// races its own listing, and this is the difference from the single-object
// route, which answers 404.
func TestDeleteObjectsReportsMissingKeyAsDeleted(t *testing.T) {
	f := newDeleteFixture(t, "present.txt")

	rr, result := f.deleteKeys(t, false, "present.txt", "never-existed.txt")

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Empty(t, result.Errors)
	assert.Equal(t, []string{"present.txt", "never-existed.txt"}, deletedKeys(result))
}

// One key that cannot be deleted must not take the rest of the batch with it.
func TestDeleteObjectsReportsPerKeyFailure(t *testing.T) {
	f := newDeleteFixture(t, "good.txt")

	// A record the placement decoder rejects is the failure a caller can do
	// nothing about, and the one that must not fail its neighbours.
	corrupt := model.ObjectHash(deleteTestBucket, "corrupt.txt")
	require.NoError(t, metaPut(context.Background(), f.write.mc, model.TableObjects,
		string(corrupt[:]), []byte("not a placement record")))

	rr, result := f.deleteKeys(t, false, "good.txt", "corrupt.txt")

	assert.Equal(t, http.StatusOK, rr.Code, "a failed key must not fail the request")
	assert.Equal(t, []string{"good.txt"}, deletedKeys(result))
	require.Len(t, result.Errors, 1)
	assert.Equal(t, "corrupt.txt", result.Errors[0].Key)
	assert.Equal(t, string(model.ErrInternalError), result.Errors[0].Code)
	assert.False(t, f.exists(t, "good.txt"))
}

func TestDeleteObjectsQuietReturnsErrorsOnly(t *testing.T) {
	f := newDeleteFixture(t, "good.txt")

	corrupt := model.ObjectHash(deleteTestBucket, "corrupt.txt")
	require.NoError(t, metaPut(context.Background(), f.write.mc, model.TableObjects,
		string(corrupt[:]), []byte("not a placement record")))

	rr, result := f.deleteKeys(t, true, "good.txt", "corrupt.txt")

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Empty(t, result.Deleted, "quiet suppresses the deleted keys")
	require.Len(t, result.Errors, 1)
	assert.Equal(t, "corrupt.txt", result.Errors[0].Key)
	assert.False(t, f.exists(t, "good.txt"), "quiet still deletes")
}

func TestDeleteObjectsRejectsBatchesOutsideBounds(t *testing.T) {
	f := newDeleteFixture(t)

	tests := map[string]string{
		"no objects": deleteBody(false),
		"over 1000":  deleteBody(false, manyKeys(maxDeleteObjects+1)...),
		"not XML":    "{}",
		"wrong root": "<Nonsense><Object><Key>a</Key></Object></Nonsense>",
	}

	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			rr := f.post(t, body)
			assert.Equal(t, http.StatusBadRequest, rr.Code)
			assert.Contains(t, rr.Body.String(), string(model.ErrMalformedXML))
		})
	}
}

// A full batch is the size this operation exists for, so it is exercised rather
// than assumed to behave like a small one.
func TestDeleteObjectsAcceptsAFullBatch(t *testing.T) {
	f := newDeleteFixture(t)
	keys := manyKeys(maxDeleteObjects)

	rr, result := f.deleteKeys(t, false, keys...)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Empty(t, result.Errors)
	assert.Len(t, result.Deleted, maxDeleteObjects)
	assert.Equal(t, keys, deletedKeys(result))
}

func manyKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%04d", i)
	}
	return keys
}
