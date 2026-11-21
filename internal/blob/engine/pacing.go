package engine

import (
	"errors"
	"sync"
	"time"
)

const (
	// DefaultCompactionMaxBytesPerSec caps a routine pass's copy rate.
	DefaultCompactionMaxBytesPerSec = 64 << 20

	// DefaultCompactionMaxYield bounds how long one pass waits out foreground
	// pressure before it proceeds at the byte cap regardless.
	DefaultCompactionMaxYield = 60 * time.Second

	// yieldStep is how long a pass waits between pressure checks.
	yieldStep = 100 * time.Millisecond

	// urgencyCheckInterval spaces the pass's free-space checks, each of which
	// takes store.mutex.
	urgencyCheckInterval = 100 * time.Millisecond

	// maxPaceStep bounds one uninterrupted rate wait, so a low cap still sees a
	// full store and keeps the retained sweep's cadence.
	maxPaceStep = time.Second
)

// errCompactionStopped ends a pass at an extent boundary because the store is
// closing. The segment in progress is not dropped; a later pass resumes it.
var errCompactionStopped = errors.New("compaction stopped")

// clock is the time source pacing reads and waits on, injected by tests.
type clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type wallClock struct{}

func (wallClock) Now() time.Time                         { return time.Now() }
func (wallClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// nextCompactionStart is the first instant after now at which a routine pass
// of the given slot starts: interval-aligned to the wall clock, offset by the
// slot's share of the interval, so the nodes of a cluster start apart.
func nextCompactionStart(now time.Time, interval time.Duration, slot, slots int) time.Time {
	if slots < 1 || slot < 0 || slot >= slots {
		slot, slots = 0, 1
	}
	phase := interval * time.Duration(slot) / time.Duration(slots)
	t := now.Truncate(interval).Add(phase)
	for !t.After(now) {
		t = t.Add(interval)
	}
	return t
}

// latencyMeter keeps the highest foreground segment-read and sync latency of
// the current and previous second, which is what a pass yields on.
type latencyMeter struct {
	clock clock

	mu   sync.Mutex
	sec  int64
	cur  [2]time.Duration
	prev [2]time.Duration
}

const (
	latencyRead = iota
	latencySync
)

func (m *latencyMeter) rotate(sec int64) {
	switch sec {
	case m.sec:
		return
	case m.sec + 1:
		m.prev = m.cur
	default:
		m.prev = [2]time.Duration{}
	}
	m.cur = [2]time.Duration{}
	m.sec = sec
}

// record notes one foreground operation's latency. A nil meter records nothing.
func (m *latencyMeter) record(kind int, d time.Duration) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.rotate(m.clock.Now().Unix())
	m.cur[kind] = max(m.cur[kind], d)
	m.mu.Unlock()
}

// worst is the highest latency of either kind seen in the last second.
func (m *latencyMeter) worst() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rotate(m.clock.Now().Unix())
	return max(m.cur[0], m.cur[1], m.prev[0], m.prev[1])
}

// timed runs fn and records how long it took.
func (m *latencyMeter) timed(kind int, fn func() error) error {
	if m == nil {
		return fn()
	}
	start := m.clock.Now()
	err := fn()
	m.record(kind, m.clock.Now().Sub(start))
	return err
}

// pacer is one pass's rate and pressure state. A nil pacer, or one marked
// urgent, never waits.
type pacer struct {
	store *Store
	c     *compactor
	clock clock

	urgent bool
	// stopping is set once the store closes: the extent in flight finishes
	// unpaced and the pass ends at the next boundary.
	stopping bool
	// rekick asks the loop for a fresh unpaced pass once this one ends, so a
	// kick consumed mid-pass reselects candidates at the new watermark.
	rekick bool

	next        time.Time // when the bytes copied so far are paid for
	yielded     time.Duration
	urgencyAt   time.Time
	waitedTotal time.Duration
}

func (store *Store) newPacer(c *compactor, urgent bool) *pacer {
	now := store.clock.Now()
	return &pacer{store: store, c: c, clock: store.clock, urgent: urgent, next: now, urgencyAt: now}
}

// markUrgent makes the rest of the pass unpaced.
func (p *pacer) markUrgent() {
	p.urgent = true
	p.rekick = true
}

func (p *pacer) stop() {
	p.stopping = true
	p.urgent = true
}

// stopped reports whether the store is closing, for the extent boundary check.
func (p *pacer) stopped() bool {
	if p == nil {
		return false
	}
	if p.c != nil && !p.stopping {
		select {
		case <-p.c.done:
			p.stop()
		default:
		}
	}
	return p.stopping
}

// checkUrgency turns the pass unpaced on a close, a kick, or a store below
// its nearfull watermark.
func (p *pacer) checkUrgency() {
	if p.c != nil {
		select {
		case <-p.c.done:
			p.stop()
		case <-p.c.kick:
			p.markUrgent()
		default:
		}
	}
	if p.urgent {
		return
	}
	now := p.clock.Now()
	if now.Sub(p.urgencyAt) >= urgencyCheckInterval {
		p.urgencyAt = now
		if p.store.belowNearfull() {
			p.markUrgent()
		}
	}
}

// wait blocks for d unless the store closes or the pass turns urgent.
func (p *pacer) wait(d time.Duration) {
	if d <= 0 {
		return
	}
	var done <-chan struct{}
	var kick <-chan struct{}
	if p.c != nil {
		done, kick = p.c.done, p.c.kick
	}
	p.waitedTotal += d
	select {
	case <-done:
		p.stop()
	case <-kick:
		p.markUrgent()
	case <-p.clock.After(d):
	}
}

// copied accounts n bytes just copied and waits until the cap allows more,
// yielding first while foreground latency is over the threshold.
func (p *pacer) copied(n int64) {
	if p == nil {
		return
	}
	p.checkUrgency()
	p.c.maybeRetainedTick()
	if p.urgent {
		return
	}
	p.yield()
	rate := p.store.compactionMaxBytesPerSec
	if p.urgent || rate <= 0 {
		return
	}
	now := p.clock.Now()
	if p.next.Before(now) {
		p.next = now
	}
	p.next = p.next.Add(time.Duration(float64(n) / float64(rate) * float64(time.Second)))
	for !p.urgent {
		left := p.next.Sub(p.clock.Now())
		if left <= 0 {
			return
		}
		p.wait(min(left, maxPaceStep))
		p.checkUrgency()
		p.c.maybeRetainedTick()
	}
}

// yield waits in steps while foreground latency is over the threshold, until
// the pass has yielded its budget.
func (p *pacer) yield() {
	limit := p.store.compactionYieldLatency
	if limit <= 0 || p.store.latency == nil {
		return
	}
	for !p.urgent && p.yielded < p.store.compactionMaxYield && p.store.latency.worst() > limit {
		p.wait(yieldStep)
		p.yielded += yieldStep
		p.checkUrgency()
		p.c.maybeRetainedTick()
	}
}

// belowNearfull reports whether free space is under the nearfull watermark,
// full band included, which is when reclaim must not be paced.
func (store *Store) belowNearfull() bool {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	frac, err := store.freeSpaceFraction()
	if err != nil {
		return false
	}
	return frac < store.nearfullFreeFrac
}

// maybeRetainedTick runs the retained sweep if its cadence has passed, so a
// long pass does not starve it. A nil compactor runs nothing.
func (c *compactor) maybeRetainedTick() {
	if c == nil {
		return
	}
	now := c.store.clock.Now()
	if now.Sub(c.lastRetained) < retainedSweepInterval {
		return
	}
	c.lastRetained = now
	c.store.retainedTick()
}
