package transport

import (
	"errors"
	"io"
	"sync/atomic"
	"time"
)

// ErrIdleTimeout reports that a transfer stopped making progress and was
// aborted locally. It is distinct from a peer-initiated abort so a caller can
// say which side gave up.
var ErrIdleTimeout = errors.New("transfer stalled: idle timeout")

// StreamCodeIdle is sent to the peer when an idle timeout aborts a stream.
const StreamCodeIdle StreamErrorCode = 2

// StreamCodeCallerGone aborts a stream the caller no longer wants: a deadline
// it set itself, or a result it has already got elsewhere. It is distinct from
// every other abort because it is the one that carries no verdict on the peer,
// which is what stops a caller's own timeouts being read as peer failures.
const StreamCodeCallerGone StreamErrorCode = 3

// IdleGuard bounds a transfer by progress rather than by total duration. It
// wraps the reader driving the transfer and aborts the stream when a single
// read makes no progress within the idle timeout.
//
// Progress, not duration, is the right bound for bulk data: a transfer that
// keeps moving is never cut off however large it is, while one that stalls is
// aborted promptly. A total cap belongs on the small fixed exchanges either
// side of the body, where the size is known in advance.
type IdleGuard struct {
	r     io.Reader
	abort func()
	idle  time.Duration
	// hold keeps the timer armed between reads. A read guard wants it off,
	// because the gap between reads is the caller's own processing and must
	// not count against the peer. A write guard wants it on, because that gap
	// is the write being pushed to the peer, which is exactly what is bounded.
	hold bool

	timer *time.Timer
	fired atomic.Bool
}

// NewReadGuard guards a body being read from stream: a stall aborts the read
// side. Only the read itself is bounded, so a caller that pauses between
// reads is never punished for it.
func NewReadGuard(r io.Reader, stream Stream, idle time.Duration) *IdleGuard {
	return &IdleGuard{r: r, idle: idle, abort: func() { stream.CancelRead(StreamCodeIdle) }}
}

// NewWriteGuard guards a body being written to stream. It wraps the source
// the copy pulls from: a source read happens only once the previous write has
// landed, so a write that blocks shows up as a read that never comes. The
// timer therefore stays armed across the write and is reset by the next read.
func NewWriteGuard(src io.Reader, stream Stream, idle time.Duration) *IdleGuard {
	return &IdleGuard{
		r: src, idle: idle, hold: true,
		abort: func() { stream.CancelWrite(StreamCodeIdle) },
	}
}

// Read passes through to the wrapped reader under the idle bound. A read
// returns as soon as any bytes arrive, so bounding each read individually is
// what makes this an idle timeout rather than a total one.
func (g *IdleGuard) Read(p []byte) (int, error) {
	if g.fired.Load() {
		return 0, ErrIdleTimeout
	}
	if g.idle <= 0 {
		return g.r.Read(p)
	}

	// Created on first use rather than in the constructor, so a guard that is
	// never read from never arms a timer.
	if g.timer == nil {
		g.timer = time.AfterFunc(g.idle, g.trip)
	} else {
		g.timer.Reset(g.idle)
	}

	n, err := g.r.Read(p)
	if !g.hold {
		g.timer.Stop()
	}

	// The abort surfaces as a peer-style stream error; report what it was.
	if err != nil && g.fired.Load() {
		return n, ErrIdleTimeout
	}
	return n, err
}

func (g *IdleGuard) trip() {
	g.fired.Store(true)
	g.abort()
}

// Stop releases the guard. It is safe to call more than once, and must be
// called once the caller is done with the stream so no timer outlives it.
func (g *IdleGuard) Stop() {
	if g.timer != nil {
		g.timer.Stop()
	}
}

// Expired reports whether the guard aborted the stream.
func (g *IdleGuard) Expired() bool { return g.fired.Load() }
