// A request deadline that also bounds a body caps the object at whatever
// transfers inside it, which is how the gate came to fail every read of an
// object over about a gigabyte on a healthy cluster. These hold the split:
// the fixed exchanges keep a total bound, object data is bounded by progress.

package gate

import (
	"context"
	"io"
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

// The bound bulkBody's comment promises. Releasing the total deadline without
// replacing it left a client that stops mid-body holding a handler until it
// disconnected, which is a stall the blob client's guard cannot reach: that one
// aborts a stream to a blob node, not a read on the client's own connection.
func TestBulkBodyAbandonsABodyThatStopsArriving(t *testing.T) {
	readErr := make(chan error, 1)
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := io.Copy(io.Discard, r.Body)
		readErr <- err
	})

	srv := httptest.NewServer(requestDeadlineMiddleware(bulkBodyWithin(100*time.Millisecond, inner)))
	defer srv.Close()

	// Declares more than it sends and then never sends the rest, so the read
	// blocks on bytes that are still owed rather than on a closed connection.
	pr, pw := io.Pipe()
	defer func() { _ = pw.Close() }()
	go func() { _, _ = pw.Write([]byte("partial")) }()

	req, err := http.NewRequest(http.MethodPut, srv.URL+"/b/k", pr)
	require.NoError(t, err)
	req.ContentLength = 4096
	go func() {
		resp, err := srv.Client().Do(req)
		if err == nil {
			_ = resp.Body.Close()
		}
	}()

	select {
	case err := <-readErr:
		require.Error(t, err, "a body that stopped arriving must not read as a complete one")
	case <-time.After(5 * time.Second):
		t.Fatal("the gate held a stalled body past its idle bound")
	}
}

// The counter-property, and the reason the bound is on progress rather than on
// total duration: a body that keeps arriving is never cut off, however long it
// takes in total. Six sends at half the bound run to three times it.
func TestBulkBodyDoesNotBoundABodyStillArriving(t *testing.T) {
	const idle = 100 * time.Millisecond

	done := make(chan error, 1)
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := io.Copy(io.Discard, r.Body)
		done <- err
	})

	srv := httptest.NewServer(requestDeadlineMiddleware(bulkBodyWithin(idle, inner)))
	defer srv.Close()

	pr, pw := io.Pipe()
	go func() {
		for range 6 {
			time.Sleep(idle / 2)
			if _, err := pw.Write([]byte("chunk")); err != nil {
				break
			}
		}
		_ = pw.Close()
	}()

	req, err := http.NewRequest(http.MethodPut, srv.URL+"/b/k", pr)
	require.NoError(t, err)
	go func() {
		resp, err := srv.Client().Do(req)
		if err == nil {
			_ = resp.Body.Close()
		}
	}()

	select {
	case err := <-done:
		require.NoError(t, err, "a body still making progress was cut off")
	case <-time.After(5 * time.Second):
		t.Fatal("the body never finished")
	}
}
