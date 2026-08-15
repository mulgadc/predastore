package gate

import (
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5/middleware"
)

// slowRequestThreshold is the point past which a request is worth reporting
// even though it succeeded. A gate that only logs failures cannot show which
// node is degrading before it takes something down.
const slowRequestThreshold = 5 * time.Second

// inFlightReportInterval is how often a request already reported as stalled is
// reported again, so a hang that outlives one log line stays visible.
const inFlightReportInterval = 15 * time.Second

// requestIDHeader is what S3 clients expect to quote when reporting a fault,
// and what ties a gate log line to the caller's own record of the request.
// Header keys are canonicalised on the wire, so the case here is cosmetic.
const requestIDHeader = "X-Amz-Request-Id"

// requestLog records the outcome of every request. Failures and slow requests
// are logged unconditionally: the alternative is an outage whose only trace is
// on the client side, which is how a stalled blob node stayed invisible.
func requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := newRequestID()
		w.Header().Set(requestIDHeader, id)

		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		start := time.Now()

		watch := inFlightWatch{
			logger: slog.Default(),
			after:  slowRequestThreshold,
			every:  inFlightReportInterval,
		}
		stopWatch := watch.start(r, id, start)
		next.ServeHTTP(ww, r)
		stopWatch()
		elapsed := time.Since(start)

		attrs := []any{
			"request_id", id,
			"method", r.Method,
			"path", r.URL.Path,
			"status", ww.Status(),
			"bytes", ww.BytesWritten(),
			"duration_ms", elapsed.Milliseconds(),
		}

		switch {
		case ww.Status() >= http.StatusInternalServerError:
			slog.ErrorContext(r.Context(), "S3 request failed", attrs...)
		case elapsed >= slowRequestThreshold:
			slog.WarnContext(r.Context(), "S3 request slow", attrs...)
		case ww.Status() >= http.StatusBadRequest:
			slog.InfoContext(r.Context(), "S3 request rejected", attrs...)
		default:
			slog.DebugContext(r.Context(), "S3 request", attrs...)
		}
	})
}

// inFlightWatch reports a request that is taking too long while it is still
// running. A handler that never returns produces no completion log at all, so
// without this a stall is invisible from the gate and shows up only as a reset
// stream on the client.
type inFlightWatch struct {
	logger *slog.Logger
	after  time.Duration
	every  time.Duration
}

// start arms the watch and returns a function that stops the reporting.
func (w inFlightWatch) start(r *http.Request, id string, begun time.Time) (stop func()) {
	var (
		mu    sync.Mutex
		timer *time.Timer
	)

	report := func() {
		mu.Lock()
		defer mu.Unlock()
		// Stopped between the timer firing and this taking the lock.
		if timer == nil {
			return
		}
		w.logger.WarnContext(r.Context(), "S3 request still running",
			"request_id", id,
			"method", r.Method,
			"path", r.URL.Path,
			"elapsed_ms", time.Since(begun).Milliseconds(),
		)
		timer.Reset(w.every)
	}

	mu.Lock()
	timer = time.AfterFunc(w.after, report)
	mu.Unlock()

	return func() {
		mu.Lock()
		defer mu.Unlock()
		timer.Stop()
		timer = nil
	}
}

// newRequestID returns an opaque identifier. It only has to be unique enough
// to correlate one request across services, so a read failure degrades to an
// empty id rather than failing the request.
func newRequestID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return ""
	}
	return hex.EncodeToString(b[:])
}
