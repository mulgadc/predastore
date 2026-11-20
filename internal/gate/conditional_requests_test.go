package gate

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// putETag writes body to key and returns the ETag the gate answered with.
func (g keyGate) putETag(key string, body []byte) string {
	g.t.Helper()
	rr := g.do(http.MethodPut, sdkPath(key), body, nil)
	require.Equal(g.t, http.StatusOK, rr.Code, "put %q: %s", key, rr.Body.String())
	etag := rr.Header().Get("ETag")
	require.NotEmpty(g.t, etag, "put %q answered no ETag", key)
	return etag
}

func (g keyGate) requireBody(key, want string) {
	g.t.Helper()
	rr := g.do(http.MethodGet, sdkPath(key), nil, nil)
	require.Equal(g.t, http.StatusOK, rr.Code, "get %q: %s", key, rr.Body.String())
	assert.Equal(g.t, want, rr.Body.String(), "body of %q", key)
}

// A create-only PUT over an existing key must be refused and leave the
// existing object readable; clients use it as a lock.
// Verifies: Q-68
func TestPutObjectIfNoneMatchStarRefusesAnExistingKey(t *testing.T) {
	g := newKeyGate(t)
	const key = "lock"
	g.putETag(key, []byte("first"))

	rr := g.do(http.MethodPut, sdkPath(key), []byte("second"), http.Header{"If-None-Match": {"*"}})

	assert.Equal(t, http.StatusPreconditionFailed, rr.Code, "body: %s", rr.Body.String())
	assert.Contains(t, rr.Body.String(), "<Code>PreconditionFailed</Code>")
	g.requireBody(key, "first")
}

// An If-Match naming a superseded ETag must not overwrite the current object.
// Verifies: Q-68
func TestPutObjectIfMatchRefusesAStaleETag(t *testing.T) {
	g := newKeyGate(t)
	const key = "checkpoint"
	stale := g.putETag(key, []byte("v1"))
	current := g.putETag(key, []byte("v2"))
	require.NotEqual(t, stale, current, "content-derived ETags must differ")

	rr := g.do(http.MethodPut, sdkPath(key), []byte("v3"), http.Header{"If-Match": {stale}})

	assert.Equal(t, http.StatusPreconditionFailed, rr.Code, "body: %s", rr.Body.String())
	assert.Contains(t, rr.Body.String(), "<Code>PreconditionFailed</Code>")
	g.requireBody(key, "v2")
}

// An If-Match delete naming a superseded ETag must leave the object in place.
// Verifies: Q-68
func TestDeleteObjectIfMatchRefusesAStaleETag(t *testing.T) {
	g := newKeyGate(t)
	const key = "doomed"
	stale := g.putETag(key, []byte("v1"))
	g.putETag(key, []byte("v2"))

	rr := g.do(http.MethodDelete, sdkPath(key), nil, http.Header{"If-Match": {stale}})

	assert.Equal(t, http.StatusPreconditionFailed, rr.Code, "body: %s", rr.Body.String())
	assert.Contains(t, rr.Body.String(), "<Code>PreconditionFailed</Code>")
	g.requireBody(key, "v2")
}
