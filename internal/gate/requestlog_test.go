package gate

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// syncBuffer collects log output from the watchdog's timer goroutine while the
// test reads it, so the two do not race.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// A request that never returns must still be reported. Completion logging
// cannot see a hang, which is how a stalled read stayed invisible on the gate
// while clients saw only a reset stream.
func TestWatchInFlightReportsWhileStillRunning(t *testing.T) {
	t.Parallel()

	var out syncBuffer
	logger := slog.New(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelWarn}))

	r := httptest.NewRequest(http.MethodGet, "/predastore/stuck.json", nil)
	watch := inFlightWatch{logger: logger, after: 50 * time.Millisecond, every: time.Hour}
	stop := watch.start(r, "req-1", time.Now())
	defer stop()

	require.Eventually(t, func() bool {
		return strings.Contains(out.String(), "S3 request still running")
	}, 2*time.Second, 20*time.Millisecond, "a hung request must be reported before it finishes")

	assert.Contains(t, out.String(), "/predastore/stuck.json", "the report must name the request")
	assert.Contains(t, out.String(), "req-1", "the report must carry the request id")
}

// The report repeats, so a hang outliving one line stays visible.
func TestWatchInFlightRepeatsReport(t *testing.T) {
	t.Parallel()

	var out syncBuffer
	logger := slog.New(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelWarn}))

	r := httptest.NewRequest(http.MethodGet, "/predastore/stuck.json", nil)
	watch := inFlightWatch{logger: logger, after: 50 * time.Millisecond, every: 50 * time.Millisecond}
	stop := watch.start(r, "req-2", time.Now())
	defer stop()

	require.Eventually(t, func() bool {
		return strings.Count(out.String(), "S3 request still running") >= 2
	}, 2*time.Second, 20*time.Millisecond, "a hang must be reported more than once")
}

// Stopping the watchdog must silence it, so a request that finishes in time
// costs nothing but a timer.
func TestWatchInFlightSilentWhenStopped(t *testing.T) {
	t.Parallel()

	var out syncBuffer
	logger := slog.New(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelWarn}))

	r := httptest.NewRequest(http.MethodGet, "/predastore/quick.json", nil)
	watch := inFlightWatch{logger: logger, after: 100 * time.Millisecond, every: time.Hour}
	stop := watch.start(r, "req-3", time.Now())
	stop()

	time.Sleep(300 * time.Millisecond)
	assert.Empty(t, out.String(), "a completed request must not be reported as still running")
}
