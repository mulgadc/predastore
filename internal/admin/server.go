// Package admin serves a process's liveness and readiness over plain HTTP on
// the host's cluster plane. It is deliberately not part of the S3 gate: that
// listener is public by design, and health is operator traffic.
//
// It answers what this process can answer for itself. A host contributes the
// checks its roles can run, so a probe describes the process it reached rather
// than the cluster as a whole.
package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

// probeInterval is how often the background sampler re-runs every check.
// Every current consumer of /readyz polls at 4 seconds or slower, so a stored
// answer this fresh is never behind what a poller would see anyway.
const probeInterval = 5 * time.Second

// probeTimeout bounds a whole probe cycle and must stay strictly less than
// probeInterval: a cycle that ran into its own next tick would leave the
// sampler always mid-cycle instead of idle between them.
const probeTimeout = 3 * time.Second

// shutdownGrace is how long an in-flight probe has to finish once the process
// is stopping. Probes are small and there is nothing to drain.
const shutdownGrace = 2 * time.Second

// Check is one readiness question, named for the response body. The name is
// operator-facing and must describe the condition rather than the topology:
// probe output is the one place a private detail would escape.
type Check struct {
	Name  string
	Probe func(context.Context) error
}

// probeSnapshot is the immutable outcome of one probe cycle: whether the
// process was ready and the ok/failed result of every check. The sampler
// goroutine is the only writer; readyz only ever loads a pointer to one.
type probeSnapshot struct {
	ready  bool
	checks map[string]string
}

// Server answers /healthz and /readyz.
type Server struct {
	addr   string
	checks []Check
	sample atomic.Pointer[probeSnapshot]
}

func New(addr string, checks []Check) *Server {
	return &Server{addr: addr, checks: checks}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	return mux
}

// healthz answers for the process itself. Reaching this handler is the whole
// check: a process that can serve it has not deadlocked its listener.
func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

// readyz reports the sampler's most recent result. It does no probing of its
// own, so its cost no longer scales with the checks it answers for or with
// how often it is asked.
func (s *Server) readyz(w http.ResponseWriter, _ *http.Request) {
	snap := s.sample.Load()
	if snap == nil {
		// No cycle has completed yet. Reporting ready on the basis of no
		// information would send traffic to a process that has never actually
		// answered the question.
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "unready", "checks": map[string]string{}})
		return
	}

	status, state := http.StatusOK, "ready"
	if !snap.ready {
		status, state = http.StatusServiceUnavailable, "unready"
	}
	writeJSON(w, status, map[string]any{"status": state, "checks": snap.checks})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Debug("failed to write probe response", "error", err)
	}
}

// runCycle runs every check once, exactly as readyz used to run them inline,
// and stores the outcome for readyz to read. Every check runs even after one
// has failed, because an operator reading the response wants the whole
// picture rather than the first thing to break.
func (s *Server) runCycle(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()

	results := make(map[string]string, len(s.checks))
	ready := true
	for _, check := range s.checks {
		if err := check.Probe(ctx); err != nil {
			// The reason stays in the log. A probe response is unauthenticated
			// and an error carries addresses, keys and object names.
			slog.WarnContext(ctx, "readiness check failed", "check", check.Name, "error", err)
			results[check.Name] = "failed"
			ready = false
			continue
		}
		results[check.Name] = "ok"
	}

	s.sample.Store(&probeSnapshot{ready: ready, checks: results})
}

// sampleLoop is the background sampler: it owns every readiness probe, so a
// request never runs one. It samples once immediately, so a process is never
// left reporting on the basis of no information, then again on every tick
// until ctx is cancelled.
func (s *Server) sampleLoop(ctx context.Context) {
	s.runCycle(ctx)

	ticker := time.NewTicker(probeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.runCycle(ctx)
		}
	}
}

// Run serves until ctx is cancelled or the listener fails. A process that
// cannot bind its admin port keeps serving: losing the probes is worse than
// losing nothing, but it is not worth taking a healthy node down for.
func (s *Server) Run(ctx context.Context) error {
	// The sampler gets its own cancellation, not just ctx's: Run can also
	// return because the listener failed, and the sampler must stop then too
	// rather than outlive Run on a ctx nothing else is going to cancel.
	sampleCtx, cancelSample := context.WithCancel(ctx)
	sampleDone := make(chan struct{})
	go func() {
		defer close(sampleDone)
		s.sampleLoop(sampleCtx)
	}()
	defer func() { <-sampleDone }()
	defer cancelSample()

	httpSrv := &http.Server{
		Addr:              s.addr,
		Handler:           s.Handler(),
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		MaxHeaderBytes:    1 << 16,
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.addr, err)
	}

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- httpSrv.Serve(ln)
		close(serveErr)
	}()

	defer func() {
		drainCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
		defer cancel()
		if err := httpSrv.Shutdown(drainCtx); err != nil {
			slog.Error("admin listener did not drain within grace period", "error", err)
		}
		<-serveErr
	}()

	slog.Info("Starting admin listener", "addr", s.addr, "checks", len(s.checks))

	select {
	case <-ctx.Done():
		slog.Info("Shutting down admin listener...")
		return nil
	case err := <-serveErr:
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
