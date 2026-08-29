package handlers

import (
	"sync"
	"time"
)

// testClock is a clock that only moves when a test moves it. Time passing then
// becomes something a shard does rather than something the machine does, which
// is what makes a hedge decision reproducible under -race and under load.
type testClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []*clockWaiter
}

// clockWaiter is one timer or ticker. A zero period is a one-shot timer.
type clockWaiter struct {
	ch     chan time.Time
	due    time.Time
	period time.Duration
	dead   bool
}

// testClockEpoch is arbitrary but not the zero time: progress is recorded as
// UnixNano, so a base near the present keeps those values realistic.
var testClockEpoch = time.Date(2026, time.August, 30, 0, 0, 0, 0, time.UTC)

func newTestClock() *testClock {
	return &testClock{now: testClockEpoch}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *testClock) NewTimer(d time.Duration) (<-chan time.Time, func()) {
	return c.add(d, 0)
}

func (c *testClock) NewTicker(d time.Duration) (<-chan time.Time, func()) {
	return c.add(d, d)
}

func (c *testClock) add(d, period time.Duration) (<-chan time.Time, func()) {
	c.mu.Lock()
	w := &clockWaiter{ch: make(chan time.Time, 1), due: c.now.Add(d), period: period}
	c.waiters = append(c.waiters, w)
	c.mu.Unlock()

	return w.ch, func() {
		c.mu.Lock()
		w.dead = true
		c.mu.Unlock()
	}
}

// Advance moves the clock and fires whatever that made due. Sends are dropped
// when the channel is already full, which is what a real ticker does to a
// consumer that fell behind.
func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	var fire []*clockWaiter
	live := c.waiters[:0]
	for _, w := range c.waiters {
		if w.dead {
			continue
		}
		if !w.due.After(now) {
			fire = append(fire, w)
			if w.period == 0 {
				w.dead = true

				continue
			}
			for !w.due.After(now) {
				w.due = w.due.Add(w.period)
			}
		}
		live = append(live, w)
	}
	c.waiters = live
	c.mu.Unlock()

	for _, w := range fire {
		select {
		case w.ch <- now:
		default:
		}
	}
}

var _ clock = (*testClock)(nil)

// shardPacer drives a stripe's virtual clock from what its shards are doing. It
// moves time only on behalf of a shard that is deliberately slow or silent, and
// never far enough to catch a peer, so which shard gets hedged is decided by
// the test rather than by the scheduler.
type shardPacer struct {
	clk *testClock

	bound chan struct{}
	r     *stripeReader
}

func newShardPacer() *shardPacer {
	return &shardPacer{clk: newTestClock(), bound: make(chan struct{})}
}

// bind hands the pacer the reader whose progress it gates on. It is a stripe
// option so the pacer has the reader before the first block is read.
func (p *shardPacer) bind() stripeOption {
	return func(r *stripeReader) {
		p.r = r
		close(p.bound)
	}
}

// pacerWait bounds how long a gate waits for its peers. Reaching it means the
// stripe is not making progress at all, and the test's own assertions are a
// better report of that than a hang is.
const pacerWait = 10 * time.Second

// awaitPeersFed blocks until no other shard can be hedged for silence: each has
// either delivered bytes into this block or is not reading one. It is what a
// shard holds before moving the clock by less than the stall window.
func (p *shardPacer) awaitPeersFed(self int) {
	p.await(self, func(s *shardProgress) bool {
		return !s.reading.Load() || s.blockBytes.Load() > 0
	})
}

// awaitPeersIdle blocks until no other shard is reading a block at all, which
// is the only state in which the clock may be moved past a stall window.
func (p *shardPacer) awaitPeersIdle(self int) {
	p.await(self, func(s *shardProgress) bool { return !s.reading.Load() })
}

func (p *shardPacer) await(self int, ok func(*shardProgress) bool) {
	select {
	case <-p.bound:
	case <-time.After(pacerWait):
		return
	}
	deadline := time.Now().Add(pacerWait)
	for time.Now().Before(deadline) {
		settled := true
		for i := range p.r.total {
			if i != self && !ok(p.r.progress[i]) {
				settled = false

				break
			}
		}
		if settled {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

// frozenClock is for reads whose subject is not the hedge. Time never moves, so
// no shard can be given up on for being slow and the test states only the thing
// it is about.
func frozenClock() stripeOption {
	return withClock(newTestClock())
}
