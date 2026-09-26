package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// deleteWithLaggingMeta runs one DELETE through the handler over a meta store
// that answers a read from a replica which has not applied the delete yet.
func deleteWithLaggingMeta(f handlerFixture, bucket, key, versionID string) *httptest.ResponseRecorder {
	target := "/" + bucket + "/" + key
	if versionID != "" {
		target += "?versionId=" + versionID
	}
	req := httptest.NewRequest(http.MethodDelete, target, nil).WithContext(objectCtx(bucket, key))
	rr := httptest.NewRecorder()
	DeleteObject(newLaggingMeta(f.mc), f.bc, f.cache, f.cfg).ServeHTTP(rr, req)
	return rr
}

// Terraform's force_destroy empties a bucket by deleting each key at the
// version ListObjectVersions reported, which on an unversioned bucket is null.
// The listing row has to go with it.
//
// It did not: the re-read that decides whether to promote a surviving version
// was answered by a replica that had not applied the delete, so the null
// version looked alive and the listing row was written back. The key was then
// listed with nothing behind it, unreadable and undeletable, and its bucket
// could never be emptied.
func TestDeletingTheNullVersionRemovesTheListingRowDespiteALaggingReplica(t *testing.T) {
	f := newHandlerFixture()
	createBucket(t, f, "b")
	require.Equal(t, http.StatusOK, f.put("b", "k.txt", []byte("hello")).Code)

	rr := deleteWithLaggingMeta(f, "b", "k.txt", nullVersionID)
	require.Equal(t, http.StatusNoContent, rr.Code, rr.Body.String())

	_, err := metaGet(context.Background(), f.mc, model.TableObjects, objectARN("b", "k.txt"))
	assert.Error(t, err, "the listing row must be gone, so the bucket can be emptied")

	if remaining := f.list(t, "b").Contents; remaining != nil {
		assert.Empty(t, *remaining, "ListObjectsV2 must not report a key with nothing behind it")
	}
	assert.Empty(t, f.listVersions(t, "b").Versions, "ListObjectVersions must agree with ListObjectsV2")
}

// The same delete on a versioned bucket must still promote the version that
// survives it, rather than dropping the key because the read looked stale.
func TestDeletingOneVersionPromotesTheNextDespiteALaggingReplica(t *testing.T) {
	f := newHandlerFixture()
	createBucket(t, f, "b")
	require.Equal(t, http.StatusOK, f.putVersioning(t, "b", VersioningEnabled).Code)

	require.Equal(t, http.StatusOK, f.put("b", "k.txt", []byte("first")).Code)
	second := f.put("b", "k.txt", []byte("second"))
	require.Equal(t, http.StatusOK, second.Code)
	newest := second.Header().Get(versionIDHeader)
	require.NotEmpty(t, newest)

	rr := deleteWithLaggingMeta(f, "b", "k.txt", newest)
	require.Equal(t, http.StatusNoContent, rr.Code, rr.Body.String())

	listed := f.list(t, "b")
	require.NotNil(t, listed.Contents)
	require.Len(t, *listed.Contents, 1, "the older version must still be current")
	assert.Equal(t, "k.txt", (*listed.Contents)[0].Key)

	body := f.get("b", "k.txt")
	require.Equal(t, http.StatusOK, body.Code, body.Body.String())
	assert.Equal(t, "first", body.Body.String())
}
