package handlers

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every state call is a request to a replica over the wire, not a local read
// (internal/meta/client.go), so the count of them on a hot path is a latency
// figure. The numbers here are the counts the same probe measures on the
// revision before versioning existed: versioning must cost an unversioned
// bucket -- which is every bucket in a deployment that never asks for it --
// nothing on the paths it does not use.
func TestUnversionedObjectPathsMakeNoExtraStateCalls(t *testing.T) {
	f := newHandlerFixture("bucket")
	// One request to settle the versioning status, which is read once per bucket
	// rather than once per request. The cold read is pinned separately below.
	require.Equal(t, http.StatusOK, f.put("bucket", "warm", []byte("body")).Code)

	f.mc.resetOps()
	require.Equal(t, http.StatusOK, f.put("bucket", "doc", []byte("body")).Code)
	assert.Equal(t, 2, f.mc.roundTrips(), "put: %v", f.mc.ops)

	f.mc.resetOps()
	require.Equal(t, http.StatusOK, f.get("bucket", "doc").Code)
	assert.Equal(t, 1, f.mc.roundTrips(), "get: %v", f.mc.ops)

	f.mc.resetOps()
	require.Equal(t, http.StatusOK, f.head("bucket", "doc").Code)
	assert.Equal(t, 1, f.mc.roundTrips(), "head: %v", f.mc.ops)

	f.mc.resetOps()
	require.Equal(t, http.StatusNoContent, f.del("bucket", "doc", "").Code)
	assert.Equal(t, 3, f.mc.roundTrips(), "delete: %v", f.mc.ops)
}

// The versioning status is read once per bucket, not once per request. Without
// that it would be read forever on every object request, because the answer for
// a bucket that was never versioned is an absent row and nothing else on the
// path reads it.
func TestTheVersioningStatusIsReadOncePerBucketNotPerRequest(t *testing.T) {
	f := newHandlerFixture("bucket")

	f.mc.resetOps()
	require.Equal(t, http.StatusOK, f.put("bucket", "doc", []byte("body")).Code)
	cold := f.mc.roundTrips()

	f.mc.resetOps()
	require.Equal(t, http.StatusOK, f.put("bucket", "doc", []byte("more")).Code)
	assert.Equal(t, cold-1, f.mc.roundTrips(), "second put: %v", f.mc.ops)
}

// A versioned bucket pays for what it asked for, and this pins how much. A read
// costs more than an unversioned one because the current version has to be
// resolved through the listing key rather than computed from the object name.
func TestVersionedObjectPathsStateCalls(t *testing.T) {
	f := newHandlerFixture("bucket")
	require.Equal(t, http.StatusOK, f.putVersioning(t, "bucket", "Enabled").Code)

	f.mc.resetOps()
	require.Equal(t, http.StatusOK, f.put("bucket", "doc", []byte("body")).Code)
	assert.Equal(t, 3, f.mc.roundTrips(), "versioned put: %v", f.mc.ops)

	f.mc.resetOps()
	require.Equal(t, http.StatusOK, f.get("bucket", "doc").Code)
	assert.Equal(t, 4, f.mc.roundTrips(), "versioned get: %v", f.mc.ops)
}

// Enabling versioning takes effect on the gate that served it immediately. A
// cached status that outlived the change would send the next write down the
// unversioned path, where it releases the generation it replaced -- so the
// version the client enabled versioning to keep would be destroyed by the very
// next write.
func TestEnablingVersioningTakesEffectOnTheNextWrite(t *testing.T) {
	f := newHandlerFixture("bucket")

	require.Equal(t, http.StatusOK, f.put("bucket", "doc", []byte("first")).Code)
	require.Equal(t, http.StatusOK, f.putVersioning(t, "bucket", "Enabled").Code)
	require.Equal(t, http.StatusOK, f.put("bucket", "doc", []byte("second")).Code)

	listed := f.listVersions(t, "bucket")
	require.Len(t, listed.Versions, 2, "the write after the enable must not replace the null version")
}
