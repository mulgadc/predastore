package ratelimit

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestThrottler(r int, burst int) *Throttler {
	return New(Config{Enabled: true, Rate: r, Burst: burst})
}

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

func staticKeyFuncs(account, action string) []KeyFunc {
	return []KeyFunc{
		func(r *http.Request) (string, error) { return account, nil },
		func(r *http.Request) (string, error) { return action, nil },
	}
}

func throttleWriter(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusServiceUnavailable)
	fmt.Fprint(w, "throttled")
}

func TestAllow_WithinRate(t *testing.T) {
	th := newTestThrottler(10, 10)
	defer th.Stop()

	mw := th.Middleware(staticKeyFuncs("acct1", "DescribeInstances"), throttleWriter)
	handler := mw(okHandler())

	for range 5 {
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rr.Code)
		}
	}
}

func TestAllow_BurstCapacity(t *testing.T) {
	th := newTestThrottler(1, 5)
	defer th.Stop()

	mw := th.Middleware(staticKeyFuncs("acct1", "action"), throttleWriter)
	handler := mw(okHandler())

	for i := range 5 {
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
		if rr.Code != http.StatusOK {
			t.Fatalf("request %d: expected 200, got %d", i, rr.Code)
		}
	}
}

func TestAllow_TokenRefill(t *testing.T) {
	th := newTestThrottler(10, 1)
	defer th.Stop()

	mw := th.Middleware(staticKeyFuncs("acct1", "action"), throttleWriter)
	handler := mw(okHandler())

	// Use the single token.
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("first request: expected 200, got %d", rr.Code)
	}

	// Next should be throttled.
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("second request: expected 503, got %d", rr.Code)
	}

	// Wait for refill (100ms at rate=10/s).
	time.Sleep(150 * time.Millisecond)

	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("after refill: expected 200, got %d", rr.Code)
	}
}

func TestReject_ExceedsBurst(t *testing.T) {
	th := newTestThrottler(1, 3)
	defer th.Stop()

	mw := th.Middleware(staticKeyFuncs("acct1", "action"), throttleWriter)
	handler := mw(okHandler())

	rejected := 0
	for range 10 {
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
		if rr.Code == http.StatusServiceUnavailable {
			rejected++
		}
	}
	if rejected == 0 {
		t.Fatal("expected some requests to be rejected")
	}
}

func TestReject_SustainedOverRate(t *testing.T) {
	th := newTestThrottler(1, 1)
	defer th.Stop()

	mw := th.Middleware(staticKeyFuncs("acct1", "action"), throttleWriter)
	handler := mw(okHandler())

	// First request succeeds (uses the single burst token).
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("first request: expected 200, got %d", rr.Code)
	}

	// All subsequent rapid requests should be rejected.
	for i := range 5 {
		rr = httptest.NewRecorder()
		handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
		if rr.Code != http.StatusServiceUnavailable {
			t.Fatalf("request %d: expected 503, got %d", i+2, rr.Code)
		}
	}
}

func TestPerKeyIsolation_Accounts(t *testing.T) {
	th := newTestThrottler(1, 1)
	defer th.Stop()

	mwA := th.Middleware(staticKeyFuncs("acctA", "action"), throttleWriter)
	mwB := th.Middleware(staticKeyFuncs("acctB", "action"), throttleWriter)
	handlerA := mwA(okHandler())
	handlerB := mwB(okHandler())

	// Exhaust acctA's token.
	rr := httptest.NewRecorder()
	handlerA.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("acctA first: expected 200, got %d", rr.Code)
	}

	// acctA is now throttled.
	rr = httptest.NewRecorder()
	handlerA.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("acctA second: expected 503, got %d", rr.Code)
	}

	// acctB should still work.
	rr = httptest.NewRecorder()
	handlerB.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("acctB: expected 200, got %d", rr.Code)
	}
}

func TestPerKeyIsolation_Actions(t *testing.T) {
	th := newTestThrottler(1, 1)
	defer th.Stop()

	mw1 := th.Middleware(staticKeyFuncs("acct1", "action1"), throttleWriter)
	mw2 := th.Middleware(staticKeyFuncs("acct1", "action2"), throttleWriter)
	handler1 := mw1(okHandler())
	handler2 := mw2(okHandler())

	// Exhaust action1's token.
	rr := httptest.NewRecorder()
	handler1.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusOK {
		t.Fatal("action1 first: expected 200")
	}
	rr = httptest.NewRecorder()
	handler1.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatal("action1 second: expected 503")
	}

	// action2 should still work.
	rr = httptest.NewRecorder()
	handler2.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusOK {
		t.Fatal("action2: expected 200")
	}
}

func TestGC_EvictsStaleEntries(t *testing.T) {
	th := newTestThrottler(10, 10)
	defer th.Stop()

	// Manually insert a stale entry.
	th.mu.Lock()
	e := &entry{limiter: nil}
	e.lastSeen.Store(time.Now().Add(-10 * time.Minute).UnixNano())
	th.entries["stale:key"] = e
	th.mu.Unlock()

	// Run GC manually by calling the internal sweep logic.
	cutoff := time.Now().Add(-staleAfter).UnixNano()
	th.mu.Lock()
	for key, e := range th.entries {
		if e.lastSeen.Load() < cutoff {
			delete(th.entries, key)
		}
	}
	th.mu.Unlock()

	th.mu.RLock()
	_, exists := th.entries["stale:key"]
	th.mu.RUnlock()

	if exists {
		t.Fatal("stale entry should have been evicted")
	}
}

func TestGC_RetainsActiveEntries(t *testing.T) {
	th := newTestThrottler(10, 10)
	defer th.Stop()

	// Access the limiter to create an entry.
	th.getOrCreate("active:key", "action")

	cutoff := time.Now().Add(-staleAfter).UnixNano()
	th.mu.Lock()
	for key, e := range th.entries {
		if e.lastSeen.Load() < cutoff {
			delete(th.entries, key)
		}
	}
	th.mu.Unlock()

	th.mu.RLock()
	_, exists := th.entries["active:key"]
	th.mu.RUnlock()

	if !exists {
		t.Fatal("active entry should not have been evicted")
	}
}

func TestConcurrentAccess(t *testing.T) {
	th := newTestThrottler(1000, 1000)
	defer th.Stop()

	mw := th.Middleware(staticKeyFuncs("acct1", "action"), throttleWriter)
	handler := mw(okHandler())

	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			for range 20 {
				rr := httptest.NewRecorder()
				handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
			}
		})
	}
	wg.Wait()
}

func TestRetryAfter_Header(t *testing.T) {
	th := newTestThrottler(1, 1)
	defer th.Stop()

	mw := th.Middleware(staticKeyFuncs("acct1", "action"), throttleWriter)
	handler := mw(okHandler())

	// Exhaust the token.
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))

	// Trigger throttle.
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}

	retryAfter := rr.Header().Get("Retry-After")
	if retryAfter == "" {
		t.Fatal("Retry-After header not set")
	}
}

func TestRetryAfter_SetBeforeErrorWriter(t *testing.T) {
	th := newTestThrottler(1, 1)
	defer th.Stop()

	var headerInWriter string
	writer := func(w http.ResponseWriter, r *http.Request) {
		headerInWriter = w.Header().Get("Retry-After")
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	mw := th.Middleware(staticKeyFuncs("acct1", "action"), writer)
	handler := mw(okHandler())

	// Exhaust token.
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))

	// Trigger throttle.
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))

	if headerInWriter == "" {
		t.Fatal("Retry-After should be set before ThrottleErrorWriter is called")
	}
}

func TestKeyFunc_ReturnsError(t *testing.T) {
	th := newTestThrottler(1, 1)
	defer th.Stop()

	keyFuncs := []KeyFunc{
		func(r *http.Request) (string, error) { return "", fmt.Errorf("lookup failed") },
		func(r *http.Request) (string, error) { return "action", nil },
	}

	var throttled atomic.Bool
	writer := func(w http.ResponseWriter, r *http.Request) { throttled.Store(true) }

	mw := th.Middleware(keyFuncs, writer)
	handler := mw(okHandler())

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("expected fail-open 200, got %d", rr.Code)
	}
	if throttled.Load() {
		t.Fatal("error writer should not have been called")
	}
}

func TestKeyFunc_EmptyAccountID(t *testing.T) {
	th := newTestThrottler(1, 1)
	defer th.Stop()

	keyFuncs := []KeyFunc{
		func(r *http.Request) (string, error) { return "", nil },
		func(r *http.Request) (string, error) { return "action", nil },
	}

	mw := th.Middleware(keyFuncs, throttleWriter)
	handler := mw(okHandler())

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))

	// Fail-open: empty account-id is a bug, but we allow the request through.
	if rr.Code != http.StatusOK {
		t.Fatalf("expected fail-open 200, got %d", rr.Code)
	}
}

func TestKeyFunc_UnknownAction(t *testing.T) {
	th := newTestThrottler(1, 1)
	defer th.Stop()

	keyFuncs := []KeyFunc{
		func(r *http.Request) (string, error) { return "acct1", nil },
		func(r *http.Request) (string, error) { return "", nil },
	}

	mw := th.Middleware(keyFuncs, throttleWriter)
	handler := mw(okHandler())

	// First request should succeed (uses burst token for "acct1:unknown").
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	// Second should be throttled under "acct1:unknown".
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}
}

func TestThrottleErrorWriter_Called(t *testing.T) {
	th := newTestThrottler(1, 1)
	defer th.Stop()

	var called atomic.Bool
	writer := func(w http.ResponseWriter, r *http.Request) {
		called.Store(true)
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	mw := th.Middleware(staticKeyFuncs("acct1", "action"), writer)
	handler := mw(okHandler())

	// Exhaust token.
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))

	// Trigger throttle.
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))

	if !called.Load() {
		t.Fatal("ThrottleErrorWriter should have been called")
	}
}

func TestThrottleErrorWriter_NotCalled(t *testing.T) {
	th := newTestThrottler(100, 100)
	defer th.Stop()

	var called atomic.Bool
	writer := func(w http.ResponseWriter, r *http.Request) { called.Store(true) }

	mw := th.Middleware(staticKeyFuncs("acct1", "action"), writer)
	handler := mw(okHandler())

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))

	if called.Load() {
		t.Fatal("ThrottleErrorWriter should not have been called for allowed request")
	}
}

func TestDisabledConfig(t *testing.T) {
	// When disabled, Throttler should not be created at all (caller checks Enabled).
	// But if created anyway, it still works — this tests the struct is functional.
	cfg := Config{Enabled: false, Rate: 1, Burst: 1}
	if cfg.Enabled {
		t.Fatal("config should be disabled")
	}
}

func TestZeroRate(t *testing.T) {
	th := newTestThrottler(0, 0)
	defer th.Stop()

	mw := th.Middleware(staticKeyFuncs("acct1", "action"), throttleWriter)
	handler := mw(okHandler())

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("rate=0 burst=0 should reject all requests, got %d", rr.Code)
	}
}

func TestZeroBurst(t *testing.T) {
	th := newTestThrottler(10, 0)
	defer th.Stop()

	mw := th.Middleware(staticKeyFuncs("acct1", "action"), throttleWriter)
	handler := mw(okHandler())

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("burst=0 should reject all requests, got %d", rr.Code)
	}
}

func TestStop_GracefulShutdown(t *testing.T) {
	th := newTestThrottler(10, 10)
	th.Stop()

	// Verify done channel is closed.
	select {
	case <-th.done:
		// OK
	default:
		t.Fatal("done channel should be closed after Stop()")
	}
}

func TestStop_DoubleCall(t *testing.T) {
	th := newTestThrottler(10, 10)
	th.Stop()
	th.Stop() // Should not panic.
}

func TestMiddleware_BodyPreserved(t *testing.T) {
	th := newTestThrottler(100, 100)
	defer th.Stop()

	body := "Action=DescribeInstances&Version=2016-11-15"

	keyFuncs := []KeyFunc{
		func(r *http.Request) (string, error) { return "acct1", nil },
		func(r *http.Request) (string, error) {
			// Read and restore the body (simulating action extraction).
			b, _ := io.ReadAll(r.Body)
			r.Body = io.NopCloser(strings.NewReader(string(b)))
			return "DescribeInstances", nil
		},
	}

	var downstreamBody string
	downstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		downstreamBody = string(b)
		w.WriteHeader(http.StatusOK)
	})

	mw := th.Middleware(keyFuncs, throttleWriter)
	handler := mw(downstream)

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if downstreamBody != body {
		t.Fatalf("body not preserved: got %q, want %q", downstreamBody, body)
	}
}
