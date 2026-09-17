package handlers

import (
	"context"
	"encoding/hex"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The fixture drives the real handlers over the real shard round-trip, so a
// test here proves the bytes of an old version survive rather than proving an
// index row was written.

func (f handlerFixture) putVersioning(t *testing.T, bucket, status string) *httptest.ResponseRecorder {
	t.Helper()
	body := "<VersioningConfiguration><Status>" + status + "</Status></VersioningConfiguration>"
	req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?versioning", strings.NewReader(body)).
		WithContext(WithBucket(context.Background(), model.Bucket{Name: bucket}))
	rr := httptest.NewRecorder()
	PutBucketVersioning(f.mc, f.cache).ServeHTTP(rr, req)
	return rr
}

func (f handlerFixture) getVersioning(t *testing.T, bucket string) VersioningConfiguration {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/"+bucket+"?versioning", nil).
		WithContext(WithBucket(context.Background(), model.Bucket{Name: bucket}))
	rr := httptest.NewRecorder()
	GetBucketVersioning(f.mc, f.cache).ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	var doc VersioningConfiguration
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &doc))
	return doc
}

func (f handlerFixture) getVersion(bucket, key, versionID string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+key+"?versionId="+versionID, nil).
		WithContext(objectCtx(bucket, key))
	rr := httptest.NewRecorder()
	GetObject(f.mc, f.bc, f.ring, f.cache, f.cfg).ServeHTTP(rr, req)
	return rr
}

func (f handlerFixture) del(bucket, key, versionID string) *httptest.ResponseRecorder {
	target := "/" + bucket + "/" + key
	if versionID != "" {
		target += "?versionId=" + versionID
	}
	req := httptest.NewRequest(http.MethodDelete, target, nil).WithContext(objectCtx(bucket, key))
	rr := httptest.NewRecorder()
	DeleteObject(f.mc, f.bc, f.cache, f.cfg).ServeHTTP(rr, req)
	return rr
}

// createBucket makes a bucket the way a client does, rather than declaring it
// in the cache. A declared bucket is part of the deployment and DeleteBucket
// refuses to remove one whatever its contents, so it cannot exercise the
// emptiness checks.
func createBucket(t *testing.T, f handlerFixture, bucket string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, "/"+bucket, nil).
		WithContext(WithBucket(context.Background(), model.Bucket{Name: bucket}))
	rr := httptest.NewRecorder()
	CreateBucket(f.mc, f.cache, f.cfg).ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
}

func (f handlerFixture) deleteBucket(bucket string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodDelete, "/"+bucket, nil).
		WithContext(WithBucket(context.Background(), model.Bucket{Name: bucket}))
	rr := httptest.NewRecorder()
	DeleteBucket(f.mc, f.cache).ServeHTTP(rr, req)
	return rr
}

func (f handlerFixture) listVersions(t *testing.T, bucket string) ListVersionsResult {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/"+bucket+"?versions", nil).
		WithContext(WithBucket(context.Background(), model.Bucket{Name: bucket}))
	rr := httptest.NewRecorder()
	ListObjectVersions(f.mc, f.cache).ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	var result ListVersionsResult
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &result))
	return result
}

func TestBucketVersioningRoundTrips(t *testing.T) {
	f := newHandlerFixture("bucket")

	// A bucket that has never been versioned reports no status, which S3
	// distinguishes from Suspended.
	assert.Empty(t, f.getVersioning(t, "bucket").Status)

	require.Equal(t, http.StatusOK, f.putVersioning(t, "bucket", "Enabled").Code)
	assert.Equal(t, "Enabled", f.getVersioning(t, "bucket").Status)

	require.Equal(t, http.StatusOK, f.putVersioning(t, "bucket", "Suspended").Code)
	assert.Equal(t, "Suspended", f.getVersioning(t, "bucket").Status)
}

func TestPutBucketVersioningRejectsAnUnknownStatus(t *testing.T) {
	f := newHandlerFixture("bucket")

	body := `<VersioningConfiguration><Status>Off</Status></VersioningConfiguration>`
	req := httptest.NewRequest(http.MethodPut, "/bucket?versioning", strings.NewReader(body)).
		WithContext(WithBucket(context.Background(), model.Bucket{Name: "bucket"}))
	rr := httptest.NewRecorder()
	PutBucketVersioning(f.mc, f.cache).ServeHTTP(rr, req)

	// Answering 200 and storing nothing would report versioning as enabled on a
	// bucket that still overwrites in place.
	require.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())
	assert.Empty(t, f.getVersioning(t, "bucket").Status)
}

// TestOverwriteRetainsThePriorVersionsBytes is the gap itself: not that two
// index rows exist, but that the bytes of the version that was overwritten are
// still readable.
func TestOverwriteRetainsThePriorVersionsBytes(t *testing.T) {
	f := newHandlerFixture("bucket")
	require.Equal(t, http.StatusOK, f.putVersioning(t, "bucket", "Enabled").Code)

	first := []byte("the first version of this object")
	rr := f.put("bucket", "doc", first)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	firstID := rr.Header().Get(versionIDHeader)
	require.NotEmpty(t, firstID)

	second := []byte("the second version, which is longer than the first")
	rr = f.put("bucket", "doc", second)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	secondID := rr.Header().Get(versionIDHeader)
	require.NotEmpty(t, secondID)
	require.NotEqual(t, firstID, secondID)

	got := f.getVersion("bucket", "doc", firstID)
	require.Equal(t, http.StatusOK, got.Code, got.Body.String())
	assert.Equal(t, first, got.Body.Bytes(), "the overwritten version must still hold its own bytes")

	got = f.getVersion("bucket", "doc", secondID)
	require.Equal(t, http.StatusOK, got.Code, got.Body.String())
	assert.Equal(t, second, got.Body.Bytes())

	// An unqualified read is the newest version.
	got = f.get("bucket", "doc")
	require.Equal(t, http.StatusOK, got.Code)
	assert.Equal(t, second, got.Body.Bytes())

	versions := f.listVersions(t, "bucket")
	require.Len(t, versions.Versions, 2)
	assert.Equal(t, secondID, versions.Versions[0].VersionId)
	assert.True(t, versions.Versions[0].IsLatest)
	assert.Equal(t, firstID, versions.Versions[1].VersionId)
	assert.False(t, versions.Versions[1].IsLatest)
}

// TestAnObjectWrittenBeforeVersioningIsTheNullVersion covers the upgrade path:
// enabling versioning on a bucket that already holds objects must not hide them.
func TestAnObjectWrittenBeforeVersioningIsTheNullVersion(t *testing.T) {
	f := newHandlerFixture("bucket")

	original := []byte("written while the bucket was unversioned")
	rr := f.put("bucket", "doc", original)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Empty(t, rr.Header().Get(versionIDHeader), "an unversioned write names no version")

	require.Equal(t, http.StatusOK, f.putVersioning(t, "bucket", "Enabled").Code)

	versions := f.listVersions(t, "bucket")
	require.Len(t, versions.Versions, 1)
	assert.Equal(t, "null", versions.Versions[0].VersionId)

	got := f.getVersion("bucket", "doc", "null")
	require.Equal(t, http.StatusOK, got.Code, got.Body.String())
	assert.Equal(t, original, got.Body.Bytes())

	// Overwriting it must leave the null version readable.
	rr = f.put("bucket", "doc", []byte("written after versioning was enabled"))
	require.Equal(t, http.StatusOK, rr.Code)

	got = f.getVersion("bucket", "doc", "null")
	require.Equal(t, http.StatusOK, got.Code, got.Body.String())
	assert.Equal(t, original, got.Body.Bytes())
}

func TestDeleteOnAVersionedBucketHidesRatherThanDestroys(t *testing.T) {
	f := newHandlerFixture("bucket")
	require.Equal(t, http.StatusOK, f.putVersioning(t, "bucket", "Enabled").Code)

	body := []byte("still here after the delete")
	rr := f.put("bucket", "doc", body)
	require.Equal(t, http.StatusOK, rr.Code)
	versionID := rr.Header().Get(versionIDHeader)

	del := f.del("bucket", "doc", "")
	require.Equal(t, http.StatusNoContent, del.Code, del.Body.String())
	assert.Equal(t, "true", del.Header().Get(deleteMarkerHeader))
	markerID := del.Header().Get(versionIDHeader)
	require.NotEmpty(t, markerID)

	// Gone from the listing, and gone from an unqualified read.
	assert.Empty(t, f.list(t, "bucket").Contents)
	got := f.get("bucket", "doc")
	assert.Equal(t, http.StatusNotFound, got.Code)
	assert.Equal(t, "true", got.Header().Get(deleteMarkerHeader))

	// The bytes are untouched.
	got = f.getVersion("bucket", "doc", versionID)
	require.Equal(t, http.StatusOK, got.Code, got.Body.String())
	assert.Equal(t, body, got.Body.Bytes())

	versions := f.listVersions(t, "bucket")
	require.Len(t, versions.Versions, 1)
	require.Len(t, versions.DeleteMarkers, 1)
	assert.True(t, versions.DeleteMarkers[0].IsLatest)

	// Deleting the marker brings the object back.
	require.Equal(t, http.StatusNoContent, f.del("bucket", "doc", markerID).Code)
	got = f.get("bucket", "doc")
	require.Equal(t, http.StatusOK, got.Code, got.Body.String())
	assert.Equal(t, body, got.Body.Bytes())
}

func TestDeletingTheCurrentVersionPromotesItsPredecessor(t *testing.T) {
	f := newHandlerFixture("bucket")
	require.Equal(t, http.StatusOK, f.putVersioning(t, "bucket", "Enabled").Code)

	first := []byte("first")
	require.Equal(t, http.StatusOK, f.put("bucket", "doc", first).Code)

	rr := f.put("bucket", "doc", []byte("second"))
	require.Equal(t, http.StatusOK, rr.Code)
	secondID := rr.Header().Get(versionIDHeader)

	require.Equal(t, http.StatusNoContent, f.del("bucket", "doc", secondID).Code)

	got := f.get("bucket", "doc")
	require.Equal(t, http.StatusOK, got.Code, got.Body.String())
	assert.Equal(t, first, got.Body.Bytes(), "the predecessor must become current")

	// And it is back in the plain listing, which reads the same record.
	require.NotNil(t, f.list(t, "bucket").Contents)
	assert.Len(t, *f.list(t, "bucket").Contents, 1)
}

func TestDeletingANamedVersionDestroysOnlyThatVersion(t *testing.T) {
	f := newHandlerFixture("bucket")
	require.Equal(t, http.StatusOK, f.putVersioning(t, "bucket", "Enabled").Code)

	rr := f.put("bucket", "doc", []byte("first"))
	firstID := rr.Header().Get(versionIDHeader)
	rr = f.put("bucket", "doc", []byte("second"))
	secondID := rr.Header().Get(versionIDHeader)

	require.Equal(t, http.StatusNoContent, f.del("bucket", "doc", firstID).Code)

	assert.Equal(t, http.StatusNotFound, f.getVersion("bucket", "doc", firstID).Code)
	got := f.getVersion("bucket", "doc", secondID)
	require.Equal(t, http.StatusOK, got.Code, got.Body.String())
	assert.Equal(t, []byte("second"), got.Body.Bytes())

	versions := f.listVersions(t, "bucket")
	require.Len(t, versions.Versions, 1)
	assert.Equal(t, secondID, versions.Versions[0].VersionId)
}

// TestSuspendedOverwritesTheNullVersionAndSparesTheRest pins the one case where
// a write on a bucket with versioning history still destroys data, which is
// S3's rule and the reason Suspended is not the same as Enabled.
func TestSuspendedOverwritesTheNullVersionAndSparesTheRest(t *testing.T) {
	f := newHandlerFixture("bucket")
	require.Equal(t, http.StatusOK, f.putVersioning(t, "bucket", "Enabled").Code)

	rr := f.put("bucket", "doc", []byte("kept"))
	keptID := rr.Header().Get(versionIDHeader)

	require.Equal(t, http.StatusOK, f.putVersioning(t, "bucket", "Suspended").Code)

	rr = f.put("bucket", "doc", []byte("null one"))
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "null", rr.Header().Get(versionIDHeader))

	rr = f.put("bucket", "doc", []byte("null two"))
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "null", rr.Header().Get(versionIDHeader))

	// The second suspended write replaced the first; the enabled-era version did
	// not move.
	got := f.getVersion("bucket", "doc", "null")
	require.Equal(t, http.StatusOK, got.Code, got.Body.String())
	assert.Equal(t, []byte("null two"), got.Body.Bytes())

	got = f.getVersion("bucket", "doc", keptID)
	require.Equal(t, http.StatusOK, got.Code, got.Body.String())
	assert.Equal(t, []byte("kept"), got.Body.Bytes())

	versions := f.listVersions(t, "bucket")
	assert.Len(t, versions.Versions, 2)
}

func TestUnversionedBucketBehaviourIsUnchanged(t *testing.T) {
	f := newHandlerFixture("bucket")

	require.Equal(t, http.StatusOK, f.put("bucket", "doc", []byte("first")).Code)
	rr := f.put("bucket", "doc", []byte("second"))
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Empty(t, rr.Header().Get(versionIDHeader))

	got := f.get("bucket", "doc")
	require.Equal(t, http.StatusOK, got.Code)
	assert.Equal(t, []byte("second"), got.Body.Bytes())

	// No version index rows at all: an unversioned bucket must not start
	// accumulating state it never asked for.
	items, err := metaScan(context.Background(), f.mc, model.TableObjectVersions, versionBucketPrefix("bucket"), 0)
	require.NoError(t, err)
	assert.Empty(t, items)

	require.Equal(t, http.StatusNoContent, f.del("bucket", "doc", "").Code)
	assert.Equal(t, http.StatusNotFound, f.get("bucket", "doc").Code)
}

// TestVersionsOrderByWriteOrderNotByMillisecond pins the ordering against the
// case that breaks a timestamp: several writes to one key inside a single
// millisecond, which an SDK retry or a loop produces routinely. Every version
// then carries the same LastModified, and ordering on it leaves the newest
// version wherever the random id happens to sort -- so a read of the key
// returns an arbitrary one of them.
func TestVersionsOrderByWriteOrderNotByMillisecond(t *testing.T) {
	f := newHandlerFixture("bucket")
	require.Equal(t, http.StatusOK, f.putVersioning(t, "bucket", "Enabled").Code)

	const writes = 6
	ids := make([]string, writes)
	for i := range writes {
		rr := f.put("bucket", "doc", []byte{byte('a' + i)})
		require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
		ids[i] = rr.Header().Get(versionIDHeader)
	}

	versions := f.listVersions(t, "bucket")
	require.Len(t, versions.Versions, writes)
	for i, entry := range versions.Versions {
		assert.Equal(t, ids[writes-1-i], entry.VersionId, "version %d is out of write order", i)
	}

	got := f.get("bucket", "doc")
	require.Equal(t, http.StatusOK, got.Code, got.Body.String())
	assert.Equal(t, []byte{byte('a' + writes - 1)}, got.Body.Bytes(), "the current version must be the last written")
}

// TestVersionIndexKeysSortNewestFirstWithinAMillisecond is the same property
// pinned without depending on how fast the machine runs: the clock is frozen,
// so every epoch here shares a millisecond and only the minter's sequence
// separates them.
func TestVersionIndexKeysSortNewestFirstWithinAMillisecond(t *testing.T) {
	t.Parallel()

	minter, err := NewEpochMinter(1)
	require.NoError(t, err)
	frozen := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	minter.now = func() time.Time { return frozen }

	keys := make([]string, 4)
	for i := range keys {
		epoch, err := minter.Next()
		require.NoError(t, err)
		require.Equal(t, frozen, EpochTime(epoch), "the clock did not move, so neither may the timestamp")
		keys[i] = versionIndexKey("bucket", "doc", epoch, "id")
	}

	// A scan returns rows in key order, so the newest write must sort first.
	for i := 1; i < len(keys); i++ {
		assert.Less(t, keys[i], keys[i-1], "write %d does not sort ahead of write %d", i, i-1)
	}
}

// TestDeleteBucketRefusesABucketHiddenBehindDeleteMarkers covers the one way a
// versioned bucket can look empty while holding every byte ever written to it:
// each key's listing key is gone, so the object scan finds nothing.
func TestDeleteBucketRefusesABucketHiddenBehindDeleteMarkers(t *testing.T) {
	f := newHandlerFixture()
	createBucket(t, f, "bucket")

	require.Equal(t, http.StatusOK, f.putVersioning(t, "bucket", "Enabled").Code)
	require.Equal(t, http.StatusOK, f.put("bucket", "doc", []byte("hidden, not gone")).Code)

	del := f.del("bucket", "doc", "")
	require.Equal(t, http.StatusNoContent, del.Code)
	assert.Empty(t, f.list(t, "bucket").Contents)

	rr := f.deleteBucket("bucket")
	require.Equal(t, http.StatusConflict, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "BucketNotEmpty")

	// Destroying the last version and its marker empties it for real.
	require.Equal(t, http.StatusNoContent, f.del("bucket", "doc", del.Header().Get(versionIDHeader)).Code)
	versions := f.listVersions(t, "bucket")
	require.Len(t, versions.Versions, 1)
	require.Equal(t, http.StatusNoContent, f.del("bucket", "doc", versions.Versions[0].VersionId).Code)

	assert.Equal(t, http.StatusNoContent, f.deleteBucket("bucket").Code)
}

// TestUnversionedObjectHashIsUnchanged pins the backward-compatibility claim
// against a literal, so the hash of an already-stored object cannot drift with
// a later edit to the version scheme.
func TestUnversionedObjectHashIsUnchanged(t *testing.T) {
	t.Parallel()

	// sha256("bucket/key")
	const want = "0a9d370e050857030986e38bdd6cc1bf687fe4941aca697da0e8920d8ec408e3"
	got := model.ObjectHash("bucket", "key")
	assert.Equal(t, want, hex.EncodeToString(got[:]), "an already-stored object must stay addressable")

	// A version is a different shard set, never the same one under a new name.
	assert.NotEqual(t, got, model.VersionHash("bucket", "key", "v1"))
	assert.Equal(t, got, versionObjectHash("bucket", "key", nullVersionID))
}
