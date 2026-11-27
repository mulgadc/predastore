package gate

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// condGate is a keyGate that keeps its meta store, so a test can stand a
// legacy record in for the one a write left.
type condGate struct {
	keyGate

	meta *fakeMeta
}

func newCondGate(t *testing.T, versioning string) condGate {
	t.Helper()
	cfg := newAuthTestConfig()
	blob := newMemBlob()
	mc := newFakeMeta(t)
	cfg.Blob, cfg.Meta = blob, mc
	cfg.RS = RS{Data: 2, Parity: 1}
	cfg.BlobNodeIDs = []config.NodeID{1, 2, 3}
	g := condGate{
		keyGate: keyGate{t: t, server: newTestGate(t, cfg), region: cfg.Region, blob: blob},
		meta:    mc,
	}
	if versioning != "" {
		g.setVersioning(versioning)
	}
	return g
}

func (g condGate) setVersioning(status string) {
	g.t.Helper()
	rr := g.do(http.MethodPut, "/local?versioning", []byte(
		"<VersioningConfiguration><Status>"+status+"</Status></VersioningConfiguration>"), nil)
	require.Equal(g.t, http.StatusOK, rr.Code, "set versioning %s: %s", status, rr.Body.String())
}

// view is everything a refused write must leave as it was: what a read of the
// key answers, the listing, and every version with which one is current.
type view struct {
	code     int
	body     string
	listed   []string
	versions []string
}

func (g condGate) view(key string) view {
	g.t.Helper()
	rr := g.do(http.MethodGet, sdkPath(key), nil, nil)
	v := view{code: rr.Code, listed: g.listedKeys(url.Values{})}
	if rr.Code == http.StatusOK {
		v.body = rr.Body.String()
	}
	rr = g.do(http.MethodGet, "/local?versions=", nil, nil)
	require.Equal(g.t, http.StatusOK, rr.Code, "list versions: %s", rr.Body.String())
	var versions handlers.ListVersionsResult
	require.NoError(g.t, xml.Unmarshal(rr.Body.Bytes(), &versions))
	for _, ver := range versions.Versions {
		v.versions = append(v.versions, fmt.Sprintf("%s@%s latest=%t", ver.Key, ver.VersionId, ver.IsLatest))
	}
	for _, m := range versions.DeleteMarkers {
		v.versions = append(v.versions, fmt.Sprintf("%s@%s marker latest=%t", m.Key, m.VersionId, m.IsLatest))
	}
	return v
}

// dropDigest rewrites the current record of an unversioned key as a record
// from before ETags were stored.
func (g condGate) dropDigest(key string) {
	g.t.Helper()
	hash := model.ObjectHash("local", key)
	stored := handlers.TableKey(model.TableObjects, string(hash[:]))
	place, err := handlers.DecodePlacement(g.meta.rows[stored])
	require.NoError(g.t, err)
	place.Digest = nil
	row, err := handlers.EncodePlacement(place)
	require.NoError(g.t, err)
	g.meta.rows[stored] = row
}

// noIntentLeft fails when a settled write leaves its intent row behind.
func (g condGate) noIntentLeft() {
	g.t.Helper()
	for k := range g.meta.rows {
		assert.NotContains(g.t, k, handlers.IntentPrefix, "an intent row outlived its write")
	}
}

var bucketStates = []string{"", handlers.VersioningEnabled, handlers.VersioningSuspended}

func stateName(s string) string {
	if s == "" {
		return "never-versioned"
	}
	return s
}

// A write whose condition holds stores the body and answers its ETag, on
// every bucket state.
// Verifies: Q-68.
func TestConditionalPutWritesWhenTheConditionHolds(t *testing.T) {
	for _, state := range bucketStates {
		t.Run(stateName(state), func(t *testing.T) {
			for name, run := range map[string]func(g condGate) *http.Header{
				"If-None-Match * absent": func(condGate) *http.Header { return &http.Header{"If-None-Match": {"*"}} },
				"If-Match current": func(g condGate) *http.Header {
					return &http.Header{"If-Match": {g.putETag("lock", []byte("old"))}}
				},
				"If-Match *": func(g condGate) *http.Header {
					g.putETag("lock", []byte("old"))
					return &http.Header{"If-Match": {"*"}}
				},
				"If-Match in a list": func(g condGate) *http.Header {
					return &http.Header{"If-Match": {`"nope", ` + g.putETag("lock", []byte("old"))}}
				},
			} {
				t.Run(name, func(t *testing.T) {
					g := newCondGate(t, state)
					header := run(g)

					rr := g.do(http.MethodPut, sdkPath("lock"), []byte("new"), *header)

					require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
					assert.Equal(t, `"22af645d1859cb5ca6da0c484f1f37ea"`, rr.Header().Get("ETag"))
					if state != "" {
						assert.NotEmpty(t, rr.Header().Get("X-Amz-Version-Id"))
					}
					g.requireBody("lock", "new")
					assert.Contains(t, g.listedKeys(url.Values{}), "lock")
					g.noIntentLeft()
				})
			}
		})
	}
}

// A write whose condition fails answers 412, or 404 for If-Match on no
// object, and leaves the object, its listing and its versions unchanged.
// Verifies: Q-68.
func TestConditionalPutLeavesTheObjectWhenTheConditionFails(t *testing.T) {
	cases := map[string]struct {
		seed   func(g condGate)
		header http.Header
		code   int
	}{
		"If-None-Match * existing": {
			seed:   func(g condGate) { g.putETag("lock", []byte("old")) },
			header: http.Header{"If-None-Match": {"*"}}, code: http.StatusPreconditionFailed,
		},
		"If-Match differs": {
			seed:   func(g condGate) { g.putETag("lock", []byte("old")) },
			header: http.Header{"If-Match": {`"0123456789abcdef0123456789abcdef"`}}, code: http.StatusPreconditionFailed,
		},
		"If-Match absent": {
			seed:   func(condGate) {},
			header: http.Header{"If-Match": {`"0123456789abcdef0123456789abcdef"`}}, code: http.StatusNotFound,
		},
		"If-Match * absent": {
			seed:   func(condGate) {},
			header: http.Header{"If-Match": {"*"}}, code: http.StatusNotFound,
		},
	}
	for _, state := range bucketStates {
		t.Run(stateName(state), func(t *testing.T) {
			for name, c := range cases {
				t.Run(name, func(t *testing.T) {
					g := newCondGate(t, state)
					c.seed(g)
					before, mutations := g.view("lock"), g.blob.mutationCount()

					rr := g.do(http.MethodPut, sdkPath("lock"), []byte("new"), c.header)

					require.Equal(t, c.code, rr.Code, "body: %s", rr.Body.String())
					if c.code == http.StatusNotFound {
						assert.Contains(t, rr.Body.String(), "<Code>NoSuchKey</Code>")
					} else {
						assert.Contains(t, rr.Body.String(), "<Code>PreconditionFailed</Code>")
					}
					assert.Equal(t, mutations, g.blob.mutationCount(), "a write refused at its early check touched a shard")
					assert.Equal(t, before, g.view("lock"))
					g.noIntentLeft()
				})
			}
		})
	}
}

// A record from before ETags were stored matches only "*": If-Match with a
// value fails, and If-None-Match: * still sees the object.
// Verifies: Q-68.
func TestConditionalPutOverALegacyRecord(t *testing.T) {
	for name, c := range map[string]struct {
		header http.Header
		code   int
	}{
		"If-Match value":  {http.Header{"If-Match": {`"22af645d1859cb5ca6da0c484f1f37ea"`}}, http.StatusPreconditionFailed},
		"If-None-Match *": {http.Header{"If-None-Match": {"*"}}, http.StatusPreconditionFailed},
		"If-Match *":      {http.Header{"If-Match": {"*"}}, http.StatusOK},
	} {
		t.Run(name, func(t *testing.T) {
			g := newCondGate(t, "")
			g.putETag("lock", []byte("new"))
			g.dropDigest("lock")
			before := g.view("lock")

			rr := g.do(http.MethodPut, sdkPath("lock"), []byte("next"), c.header)

			require.Equal(t, c.code, rr.Code, "body: %s", rr.Body.String())
			if c.code == http.StatusOK {
				g.requireBody("lock", "next")
				return
			}
			assert.Equal(t, before, g.view("lock"))
		})
	}
}

// A current delete marker is no object: If-None-Match: * creates the key
// again, and If-Match answers 404 with the marker left current.
// Verifies: Q-68.
func TestConditionalPutOverADeleteMarker(t *testing.T) {
	for _, state := range []string{handlers.VersioningEnabled, handlers.VersioningSuspended} {
		t.Run(stateName(state), func(t *testing.T) {
			seed := func(t *testing.T) condGate {
				g := newCondGate(t, state)
				g.putETag("lock", []byte("old"))
				rr := g.do(http.MethodDelete, sdkPath("lock"), nil, nil)
				require.Equal(t, http.StatusNoContent, rr.Code, "delete: %s", rr.Body.String())
				require.Equal(t, "true", rr.Header().Get("X-Amz-Delete-Marker"))
				return g
			}

			t.Run("If-None-Match *", func(t *testing.T) {
				g := seed(t)
				rr := g.do(http.MethodPut, sdkPath("lock"), []byte("new"), http.Header{"If-None-Match": {"*"}})
				require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
				g.requireBody("lock", "new")
			})
			t.Run("If-Match", func(t *testing.T) {
				g := seed(t)
				before := g.view("lock")
				rr := g.do(http.MethodPut, sdkPath("lock"), []byte("new"), http.Header{"If-Match": {"*"}})
				require.Equal(t, http.StatusNotFound, rr.Code, "body: %s", rr.Body.String())
				assert.Equal(t, before, g.view("lock"))
			})
		})
	}
}

// Suspending versioning leaves an Enabled version current. A conditional
// write evaluates it, and on success writes the null version over it.
// Verifies: Q-68.
func TestConditionalPutOnASuspendedBucketEvaluatesTheCurrentVersion(t *testing.T) {
	g := newCondGate(t, handlers.VersioningEnabled)
	g.putETag("lock", []byte("null-before"))
	current := g.putETag("lock", []byte("enabled-version"))
	g.setVersioning(handlers.VersioningSuspended)
	before := g.view("lock")

	rr := g.do(http.MethodPut, sdkPath("lock"), []byte("x"), http.Header{"If-None-Match": {"*"}})
	requirePreconditionFailed(t, rr)
	assert.Equal(t, before, g.view("lock"))

	rr = g.do(http.MethodPut, sdkPath("lock"), []byte("new"), http.Header{"If-Match": {current}})
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	assert.Equal(t, "null", rr.Header().Get("X-Amz-Version-Id"))
	g.requireBody("lock", "new")
}

// Both headers together are refused, so neither is applied alone.
// Verifies: Q-68.
func TestConditionalPutWithBothHeadersIsRefused(t *testing.T) {
	g := newCondGate(t, "")
	etag := g.putETag("lock", []byte("old"))
	before, mutations := g.view("lock"), g.blob.mutationCount()

	rr := g.do(http.MethodPut, sdkPath("lock"), []byte("new"),
		http.Header{"If-Match": {etag}, "If-None-Match": {"*"}})

	requireNotImplemented(t, rr)
	assert.Equal(t, mutations, g.blob.mutationCount())
	assert.Equal(t, before, g.view("lock"))
}

// A zero-byte object has no shards and so no intent, and its condition is
// still evaluated.
// Verifies: Q-68.
func TestConditionalPutOfAnEmptyObject(t *testing.T) {
	g := newCondGate(t, "")

	rr := g.do(http.MethodPut, sdkPath("lock"), nil, http.Header{"If-None-Match": {"*"}})
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	g.requireBody("lock", "")

	rr = g.do(http.MethodPut, sdkPath("lock"), nil, http.Header{"If-None-Match": {"*"}})
	requirePreconditionFailed(t, rr)
	g.noIntentLeft()
}
