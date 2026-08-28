package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
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
	h := New("", []Check{ok("meta_leader"), ok("meta_reachable"), ok("blob_nodes")}).Handler()

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
	h := New("", []Check{ok("meta_reachable"), failing("blob_nodes", errors.New("1 of 4 blob nodes answered, need 2"))}).Handler()

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
	h := New("", []Check{failing("meta_reachable", errors.New(secret))}).Handler()

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
	h := New("", []Check{
		counting("first", errors.New("down")),
		counting("second", nil),
		counting("third", errors.New("down")),
	}).Handler()

	probe(t, h, "/readyz")

	if got := ran.Load(); got != 3 {
		t.Errorf("checks run = %d, want 3", got)
	}
}

// A process with nothing to check is ready. A blob-only host runs no client
// for either plane, and reporting it unready forever would take it out of
// service for a question it was never able to answer.
func TestReadyzWithNoChecksIsReady(t *testing.T) {
	h := New("", nil).Handler()

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
