package gate

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The key is the decoded path after the bucket, whatever escaping the client
// chose. chi routes on RawPath when the client escaped anything, which made
// the key the escaped wire form, so these pin the decoded one per route shape.
func TestResolveObjectDecodesTheKey(t *testing.T) {
	for _, tc := range []struct {
		name, target, want string
	}{
		{"escaped space and unicode", "/local/dir%20with%20space/%C3%BC-file%2Bplus.txt", "dir with space/ü-file+plus.txt"},
		{"raw plus is a plus, not a space", "/local/a+b", "a+b"},
		{"escaped plus", "/local/a%2Bb", "a+b"},
		{"escaped percent is a literal percent", "/local/100%25", "100%"},
		{"double escape decodes once", "/local/a%2520b", "a%20b"},
		{"plain key", "/local/plain.txt", "plain.txt"},
		{"escaped slash separates", "/local/a%2Fb", "a/b"},
		{"trailing slash with escaping", "/local/a%20b/", "a b"},
		{"trailing slash with double escape", "/local/a%2520b/", "a%20b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got string
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				object, _ := handlers.ObjectFrom(r.Context())
				got = object.Key
			})
			rr := httptest.NewRecorder()
			resolveRouter(next).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, tc.target, nil))

			require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
			assert.Equal(t, tc.want, got)
		})
	}
}

// An escaped dot segment is still a dot segment once decoded, so it is refused
// rather than stored as a literal key no other layer would agree on.
func TestResolveObjectRejectsEscapedDotSegments(t *testing.T) {
	for _, target := range []string{"/local/%2E%2E/x", "/local/a/%2e/b"} {
		t.Run(target, func(t *testing.T) {
			next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Fatal("a dot segment reached the handler")
			})
			rr := httptest.NewRecorder()
			resolveRouter(next).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, target, nil))

			assert.Equal(t, http.StatusBadRequest, rr.Code)
			assert.Contains(t, rr.Body.String(), "InvalidKey")
		})
	}
}

// The bucket routes on the decoded path too, with or without a trailing slash.
func TestResolveBucketOnTheDecodedPath(t *testing.T) {
	for _, target := range []string{"/local", "/local/", "/%6Cocal"} {
		t.Run(target, func(t *testing.T) {
			var got string
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				bucket, _ := handlers.BucketFrom(r.Context())
				if _, isObject := handlers.ObjectFrom(r.Context()); isObject {
					got = "resolved as an object"
					return
				}
				got = bucket.Name
			})
			rr := httptest.NewRecorder()
			resolveRouter(next).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, target, nil))

			require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
			assert.Equal(t, "local", got)
		})
	}
}
