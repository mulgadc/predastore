package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func probe(t *testing.T, h http.Handler, path string) (int, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode %s response %q: %v", path, rec.Body.String(), err)
	}
	return rec.Code, body
}

func ok(name string) Check {
	return Check{Name: name, Probe: func(context.Context) error { return nil }}
}

func failing(name string, err error) Check {
	return Check{Name: name, Probe: func(context.Context) error { return err }}
}

// sampled builds a Server and runs one probe cycle synchronously, standing in
// for the background sampler so a test can assert on readyz without waiting
// on a ticker.
func sampled(checks []Check) *Server {
	s := New("", checks)
	s.runCycle(context.Background())
	return s
}

// Liveness is not readiness: a process whose cluster is broken is still alive,
// and answering 503 to /healthz would have an orchestrator restart a node that
// is waiting for its peers.
func TestHealthzAnswersWhileChecksFail(t *testing.T) {
	h := New("", []Check{failing("meta_leader", errors.New("no leader"))}).Handler()

	status, body := probe(t, h, "/healthz")

	if status != http.StatusOK {
		t.Errorf("healthz status = %d, want %d", status, http.StatusOK)
	}
	if body["status"] != "ok" {
		t.Errorf("healthz status field = %v, want ok", body["status"])
	}
}

func TestReadyzReportsEveryCheck(t *testing.T) {
	h := sampled([]Check{ok("meta_leader"), ok("meta_reachable"), ok("blob_nodes")}).Handler()

	status, body := probe(t, h, "/readyz")

	if status != http.StatusOK {
		t.Errorf("readyz status = %d, want %d", status, http.StatusOK)
	}
	if body["status"] != "ready" {
		t.Errorf("readyz status field = %v, want ready", body["status"])
	}
	checks, isMap := body["checks"].(map[string]any)
	if !isMap {
		t.Fatalf("checks is %T, want an object", body["checks"])
	}
	for _, name := range []string{"meta_leader", "meta_reachable", "blob_nodes"} {
		if checks[name] != "ok" {
			t.Errorf("check %q = %v, want ok", name, checks[name])
		}
	}
}

// One failed check makes the process unready. A load balancer reads the status
// code, so a body that named the failure while returning 200 would still send
// traffic to a node that cannot serve it.
func TestReadyzIsUnreadyWhenAnyCheckFails(t *testing.T) {
	h := sampled([]Check{ok("meta_reachable"), failing("blob_nodes", errors.New("1 of 4 blob nodes answered, need 2"))}).Handler()

	status, body := probe(t, h, "/readyz")

	if status != http.StatusServiceUnavailable {
		t.Errorf("readyz status = %d, want %d", status, http.StatusServiceUnavailable)
	}
	if body["status"] != "unready" {
		t.Errorf("readyz status field = %v, want unready", body["status"])
	}
	checks, _ := body["checks"].(map[string]any)
	if checks["blob_nodes"] != "failed" {
		t.Errorf("failed check reported %v, want failed", checks["blob_nodes"])
	}
	if checks["meta_reachable"] != "ok" {
		t.Errorf("passing check reported %v, want ok", checks["meta_reachable"])
	}
}

// The probe endpoint is unauthenticated, and a check error carries addresses,
// node ids and object names. The reason belongs in the log, not the response.
func TestReadyzBodyCarriesNoFailureDetail(t *testing.T) {
	secret := "meta node 7 at 10.0.0.4:6660 rejected key tenant-acme/secrets"
	h := sampled([]Check{failing("meta_reachable", errors.New(secret))}).Handler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	for _, leak := range []string{"10.0.0.4", "tenant-acme", "node 7", secret} {
		if strings.Contains(rec.Body.String(), leak) {
			t.Errorf("readyz body leaked %q: %s", leak, rec.Body.String())
		}
	}
}

// Every check runs even once one has failed: an operator reading the response
// wants the whole picture, not the first thing to break.
func TestReadyzRunsEveryCheckAfterAFailure(t *testing.T) {
	var ran atomic.Int64
	counting := func(name string, err error) Check {
		return Check{Name: name, Probe: func(context.Context) error {
			ran.Add(1)
			return err
		}}
	}
	s := sampled([]Check{
		counting("first", errors.New("down")),
		counting("second", nil),
		counting("third", errors.New("down")),
	})

	probe(t, s.Handler(), "/readyz")

	if got := ran.Load(); got != 3 {
		t.Errorf("checks run = %d, want 3", got)
	}
}

// A process with nothing to check is ready. A blob-only host runs no client
// for either plane, and reporting it unready forever would take it out of
// service for a question it was never able to answer.
func TestReadyzWithNoChecksIsReady(t *testing.T) {
	h := sampled(nil).Handler()

	status, body := probe(t, h, "/readyz")

	if status != http.StatusOK {
		t.Errorf("readyz status = %d, want %d", status, http.StatusOK)
	}
	if body["status"] != "ready" {
		t.Errorf("readyz status field = %v, want ready", body["status"])
	}
}

// The listener carries health and nothing else. Anything more is surface on a
// port that answers before authentication exists.
func TestAdminListenerServesNothingElse(t *testing.T) {
	h := New("", nil).Handler()

	for _, path := range []string{"/", "/metrics", "/debug/pprof/", "/bucket/object.txt"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))

		if rec.Code != http.StatusNotFound {
			t.Errorf("GET %s = %d, want %d", path, rec.Code, http.StatusNotFound)
		}
	}
}

// Probes are reads. A POST that ran the checks would let anything that reaches
// the port drive work on the cluster.
func TestReadyzRejectsNonGET(t *testing.T) {
	var ran atomic.Int64
	h := New("", []Check{{Name: "counted", Probe: func(context.Context) error {
		ran.Add(1)
		return nil
	}}}).Handler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/readyz", nil))

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /readyz = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
	if ran.Load() != 0 {
		t.Error("a POST ran the readiness checks")
	}
}

// A request arriving before the sampler's first cycle completes must not be
// told the process is ready: there is no information yet to base that on.
func TestReadyzUnreadyBeforeFirstCycle(t *testing.T) {
	release := make(chan struct{})
	s := New("127.0.0.1:0", []Check{{Name: "slow", Probe: func(context.Context) error {
		<-release
		return nil
	}}})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	status, body := probe(t, s.Handler(), "/readyz")
	if status != http.StatusServiceUnavailable {
		t.Errorf("readyz status before first cycle = %d, want %d", status, http.StatusServiceUnavailable)
	}
	if body["status"] != "unready" {
		t.Errorf("readyz status field before first cycle = %v, want unready", body["status"])
	}
	checks, isMap := body["checks"].(map[string]any)
	if !isMap || len(checks) != 0 {
		t.Errorf("readyz checks before first cycle = %v, want an empty object", body["checks"])
	}

	close(release)
	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run returned %v, want nil after cancellation", err)
	}
}

// readyz always reflects the sampler's latest cycle, including a check that
// recovers after failing.
func TestReadyzReflectsLatestCycle(t *testing.T) {
	var broken atomic.Bool
	s := sampled([]Check{{Name: "flaky", Probe: func(context.Context) error {
		if broken.Load() {
			return errors.New("down")
		}
		return nil
	}}})
	h := s.Handler()

	if _, body := probe(t, h, "/readyz"); body["status"] != "ready" {
		t.Fatalf("initial readyz status = %v, want ready", body["status"])
	}

	broken.Store(true)
	s.runCycle(context.Background())
	if _, body := probe(t, h, "/readyz"); body["status"] != "unready" {
		t.Fatalf("readyz status after the check broke = %v, want unready", body["status"])
	}

	broken.Store(false)
	s.runCycle(context.Background())
	if _, body := probe(t, h, "/readyz"); body["status"] != "ready" {
		t.Fatalf("readyz status after the check recovered = %v, want ready", body["status"])
	}
}

// The whole point of sampling in the background is that a request never runs
// a check: hammering the endpoint must not run the checks any more than the
// cycles that actually happened did.
func TestReadyzHammeringDoesNotReprobe(t *testing.T) {
	var ran atomic.Int64
	s := sampled([]Check{{Name: "counted", Probe: func(context.Context) error {
		ran.Add(1)
		return nil
	}}})
	h := s.Handler()

	// t.Fatalf inside probe is only safe from the test goroutine, so the
	// workers below hit the handler directly rather than sharing that helper.
	const requests = 500
	var wg sync.WaitGroup
	for range requests {
		wg.Go(func() {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		})
	}
	wg.Wait()

	if got := ran.Load(); got != 1 {
		t.Errorf("checks ran %d times across %d requests, want 1", got, requests)
	}
}

func TestRunStopsWithTheContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	srv := New("127.0.0.1:0", nil)

	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run returned %v, want nil after cancellation", err)
	}
}

func TestRunReportsAnUnusableAddress(t *testing.T) {
	err := New("256.256.256.256:1", nil).Run(context.Background())

	if err == nil {
		t.Fatal("Run returned nil for an address it cannot bind")
	}
}

// The sampler is a goroutine of its own, and it must not survive the ctx that
// governs it: a cancelled context stops the next cycle from ever starting.
func TestSampleLoopStopsWithContext(t *testing.T) {
	var ran atomic.Int64
	s := New("", []Check{{Name: "counted", Probe: func(context.Context) error {
		ran.Add(1)
		return nil
	}}})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.sampleLoop(ctx)
	}()

	deadline := time.Now().Add(time.Second)
	for ran.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if ran.Load() == 0 {
		t.Fatal("sampleLoop did not run its immediate cycle")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sampleLoop did not stop after its context was cancelled")
	}
}

// TestAdvisoryCheckIsReportedWithoutGatingReadiness is what advisory exists
// for: a cluster tolerates losing one peer, so naming the peer that is gone
// must not report the whole process unready and take it out of service.
func TestAdvisoryCheckIsReportedWithoutGatingReadiness(t *testing.T) {
	down := Check{
		Name:     "blob_node_5",
		Advisory: true,
		Probe:    func(context.Context) error { return errors.New("did not answer") },
	}
	h := sampled([]Check{ok("meta_leader"), down}).Handler()

	status, body := probe(t, h, "/readyz")

	if status != http.StatusOK {
		t.Errorf("readyz status = %d, want %d: an advisory failure is not unreadiness", status, http.StatusOK)
	}
	if body["status"] != "ready" {
		t.Errorf("readyz status field = %v, want ready", body["status"])
	}
	checks, _ := body["checks"].(map[string]any)
	if checks["blob_node_5"] != "failed" {
		t.Errorf("checks[blob_node_5] = %v, want failed: the unreachable peer must be named", checks["blob_node_5"])
	}
}

// A non-advisory failure alongside an advisory one still reports unready, so
// advisory cannot be a way for a real failure to go unnoticed.
func TestAdvisoryDoesNotMaskARealFailure(t *testing.T) {
	advisory := Check{Name: "blob_node_5", Advisory: true, Probe: func(context.Context) error { return nil }}
	h := sampled([]Check{advisory, failing("meta_leader", errors.New("no leader"))}).Handler()

	status, body := probe(t, h, "/readyz")

	if status != http.StatusServiceUnavailable {
		t.Errorf("readyz status = %d, want %d", status, http.StatusServiceUnavailable)
	}
	if body["status"] != "unready" {
		t.Errorf("readyz status field = %v, want unready", body["status"])
	}
}
