// Copyright 2025 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

package ratelimit

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

const (
	gcInterval = 60 * time.Second
	staleAfter = 5 * time.Minute
)

// KeyFunc extracts a rate-limit dimension from the request.
// The first KeyFunc returns the account-id, the second returns the action.
// Results are joined with ":" to form the limiter map key.
type KeyFunc func(r *http.Request) (string, error)

// ThrottleErrorWriter writes a service-appropriate throttle rejection response.
// Called after the middleware sets the Retry-After header.
type ThrottleErrorWriter func(w http.ResponseWriter, r *http.Request)

type entry struct {
	limiter  *rate.Limiter
	lastSeen atomic.Int64 // unix nano
}

// Throttler manages per-key token bucket rate limiters with background GC.
type Throttler struct {
	cfg     Config
	mu      sync.RWMutex
	entries map[string]*entry
	cancel  context.CancelFunc
	done    chan struct{}
	once    sync.Once
}

// New creates a Throttler from config and starts the GC goroutine.
func New(cfg Config) *Throttler {
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // G118: cancel is called in Stop()
	t := &Throttler{
		cfg:     cfg,
		entries: make(map[string]*entry),
		cancel:  cancel,
		done:    make(chan struct{}),
	}

	slog.Info("Rate limit config loaded",
		"default_rate", cfg.Rate,
		"default_burst", cfg.Burst,
		"overrides", len(cfg.Action),
	)

	go t.gc(ctx)
	return t
}

// Stop cancels the GC goroutine and waits for it to exit.
func (t *Throttler) Stop() {
	t.once.Do(func() {
		t.cancel()
		<-t.done
	})
}

func (t *Throttler) gc(ctx context.Context) {
	defer close(t.done)
	ticker := time.NewTicker(gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-staleAfter).UnixNano()
			t.mu.Lock()
			for key, e := range t.entries {
				if e.lastSeen.Load() < cutoff {
					delete(t.entries, key)
					slog.Debug("GC evicted stale entry", "key", key)
				}
			}
			t.mu.Unlock()
		}
	}
}

// getOrCreate returns an existing limiter for the key or creates a new one.
func (t *Throttler) getOrCreate(key, action string) *rate.Limiter {
	now := time.Now().UnixNano()

	t.mu.RLock()
	if e, ok := t.entries[key]; ok {
		e.lastSeen.Store(now)
		t.mu.RUnlock()
		return e.limiter
	}
	t.mu.RUnlock()

	r, burst := t.cfg.ResolveLimit(action)
	limiter := rate.NewLimiter(rate.Limit(r), burst)

	t.mu.Lock()
	// Double-check after acquiring write lock.
	if e, ok := t.entries[key]; ok {
		e.lastSeen.Store(now)
		t.mu.Unlock()
		return e.limiter
	}
	e := &entry{limiter: limiter}
	e.lastSeen.Store(now)
	t.entries[key] = e
	t.mu.Unlock()
	return limiter
}

// Middleware returns net/http middleware that enforces rate limits.
// keyFuncs extract the account-id and action from each request.
// onThrottle writes the service-appropriate error response on rejection.
func (t *Throttler) Middleware(keyFuncs []KeyFunc, onThrottle ThrottleErrorWriter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			parts := make([]string, len(keyFuncs))
			for i, kf := range keyFuncs {
				val, err := kf(r)
				if err != nil {
					slog.Error("Rate limit key extraction failed", "error", err)
					next.ServeHTTP(w, r)
					return
				}
				if i == 0 && val == "" {
					slog.Error("Rate limit: empty account-id, allowing request through")
					next.ServeHTTP(w, r)
					return
				}
				parts[i] = val
			}

			// Use second part (action) for config resolution; fall back to "unknown".
			action := "unknown"
			if len(parts) > 1 && parts[1] != "" {
				action = parts[1]
			}

			key := parts[0] + ":" + action
			limiter := t.getOrCreate(key, action)

			reservation := limiter.Reserve()
			delay := reservation.Delay()

			if delay > 0 {
				reservation.Cancel()
				w.Header().Set("Retry-After", strconv.Itoa(int(delay.Seconds())+1))
				slog.Warn("Request throttled",
					"account_id", parts[0],
					"action", action,
					"ip", r.RemoteAddr,
				)
				onThrottle(w, r)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
