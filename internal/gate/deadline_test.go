// A request deadline that also bounds a body caps the object at whatever
// transfers inside it, which is how the gate came to fail every read of an
// object over about a gigabyte on a healthy cluster. These hold the split:
// the fixed exchanges keep a total bound, object data is bounded by progress.

package gate

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// assertClosed fails with the given reason rather than deadlocking the run,
// which is what a bare receive on a channel that never closes would do.
func assertClosed(t *testing.T, ch <-chan struct{}, reason string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal(reason)
	}
}

// The default. Everything that is not object data keeps a total bound, so a
// handler that hangs before it reaches any body is cut off rather than holding
// the connection until the idle timeout notices.
func TestAnOrdinaryRequestIsBoundedByTheDeadline(t *testing.T) {
	stopped := make(chan struct{})
	inner := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		close(stopped)
	})

	deadlineMiddleware(20*time.Millisecond, inner).ServeHTTP(
		httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/b", nil))

	assertClosed(t, stopped, "an ordinary request must be bounded")
}

// The property that matters. After bulkBody the request must not be on a
// timer: a body that is still arriving is not a body that has failed, and a
// 16 GiB object legitimately outlives any fixed duration.
func TestBulkBodyReleasesTheDeadline(t *testing.T) {
	cancelled := false
	inner := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			cancelled = true
		case <-time.After(120 * time.Millisecond):
		}
	})

	deadlineMiddleware(20*time.Millisecond, bulkBody(inner)).ServeHTTP(
		httptest.NewRecorder(), httptest.NewRequest(http.MethodPut, "/b/k", nil))

	assert.False(t, cancelled,
		"the deadline still cancelled a bulk request, so a large body is capped by it")
}

// Releasing the deadline must not release cancellation. A client that goes
// away has to stop the work, or a disconnected upload runs to completion.
func TestBulkBodyKeepsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})

	inner := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		close(stopped)
	})

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	req := httptest.NewRequest(http.MethodPut, "/b/k", nil).WithContext(ctx)
	deadlineMiddleware(time.Hour, bulkBody(inner)).ServeHTTP(httptest.NewRecorder(), req)

	assertClosed(t, stopped, "a cancelled client did not stop a bulk handler")
}

// bulkBody is reached through the chain, not called directly, so the stopper
// has to survive whatever the middleware between them does to the context.
func TestTheStopperIsReachableThroughInterveningMiddleware(t *testing.T) {
	cancelled := false
	inner := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			cancelled = true
		case <-time.After(120 * time.Millisecond):
		}
	})

	// Stands in for the auth, span and throttle middleware that sit between
	// the deadline and the handler, each of which re-derives the context.
	intervening := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := context.WithValue(r.Context(), deadlineStopperKey{}, r.Context().Value(deadlineStopperKey{}))
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}

	deadlineMiddleware(20*time.Millisecond, intervening(bulkBody(inner))).ServeHTTP(
		httptest.NewRecorder(), httptest.NewRequest(http.MethodPut, "/b/k", nil))

	assert.False(t, cancelled, "bulkBody could not find the stopper through the chain")
}

// The server must not reimpose a whole-body bound underneath all of this.
// ReadTimeout and WriteTimeout are total by construction, so a non-zero value
// on either caps every object regardless of what the middleware does.
func TestServerSetsNoWholeBodyTimeouts(t *testing.T) {
	srv := newHTTPServer("127.0.0.1:0", http.NotFoundHandler(), nil, httpProtocols(true))

	require.Zero(t, srv.ReadTimeout, "ReadTimeout caps an upload at a fixed duration")
	require.Zero(t, srv.WriteTimeout, "WriteTimeout caps a download at a fixed duration")
	assert.NotZero(t, srv.ReadHeaderTimeout, "the header exchange is fixed-size and must stay bounded")
	assert.NotZero(t, srv.IdleTimeout, "an idle connection must still be reclaimed")
	assert.NotZero(t, srv.MaxHeaderBytes)
}
