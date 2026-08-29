package handlers

import "time"

// clock is the time the hedge policy is judged against. Production reads the
// wall clock; a test supplies its own so that whether a shard was given up on
// is a property of what that shard did rather than of how loaded the machine is.
type clock interface {
	Now() time.Time
	// NewTimer and NewTicker return the channel and the func that stops it,
	// rather than the concrete types, so an implementation is free to be
	// something other than a runtime timer.
	NewTimer(d time.Duration) (<-chan time.Time, func())
	NewTicker(d time.Duration) (<-chan time.Time, func())
}

// wallClock is the clock every production read runs on.
type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

func (wallClock) NewTimer(d time.Duration) (<-chan time.Time, func()) {
	t := time.NewTimer(d)

	return t.C, func() { t.Stop() }
}

func (wallClock) NewTicker(d time.Duration) (<-chan time.Time, func()) {
	t := time.NewTicker(d)

	return t.C, func() { t.Stop() }
}

var _ clock = wallClock{}
