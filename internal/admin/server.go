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
	"time"
)

// probeTimeout bounds a whole readiness probe. A check that has not answered by
// then is a check that has failed for the purposes of a load balancer, which
// will have given up long before any of these do.
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

// Server answers /healthz and /readyz.
type Server struct {
	addr   string
	checks []Check
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

// readyz reports whether this process can serve. Every check runs even once one
// has failed, because an operator reading the response wants the whole picture
// rather than the first thing to break.
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), probeTimeout)
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

	status, state := http.StatusOK, "ready"
	if !ready {
		status, state = http.StatusServiceUnavailable, "unready"
	}
	writeJSON(w, status, map[string]any{"status": state, "checks": results})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Debug("failed to write probe response", "error", err)
	}
}

// Run serves until ctx is cancelled or the listener fails. A process that
// cannot bind its admin port keeps serving: losing the probes is worse than
// losing nothing, but it is not worth taking a healthy node down for.
func (s *Server) Run(ctx context.Context) error {
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
