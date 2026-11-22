package engine

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// fakeClock advances only when something waits on it. onAfter, if set, runs
// instead of the default, which advances by d and fires at once.
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	onAfter func(d time.Duration) <-chan time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	if c.onAfter != nil {
		return c.onAfter(d)
	}
	c.advance(d)
	ch := make(chan time.Time, 1)
	ch <- c.Now()
	return ch
}

func TestCompactionStartsAreStaggeredAndKeepTheirPhase(t *testing.T) {
	interval := 5 * time.Minute
	at := func(h, m, s int) time.Time { return time.Date(2026, 11, 22, h, m, s, 0, time.UTC) }

	now := at(12, 0, 10)
	want := []time.Time{at(12, 5, 0), at(12, 1, 40), at(12, 3, 20)}
	for slot, w := range want {
		if got := nextCompactionStart(now, interval, slot, 3); !got.Equal(w) {
			t.Errorf("slot %d of 3 from %v: got %v, want %v", slot, now, got, w)
		}
	}
	// A restart mid-interval lands on the same phase.
	if got := nextCompactionStart(at(12, 2, 0), interval, 1, 3); !got.Equal(at(12, 6, 40)) {
		t.Errorf("restart keeps phase: got %v", got)
	}
	// A pass that ends past its next slot skips to the one after.
	if got := nextCompactionStart(at(12, 7, 0), interval, 1, 3); !got.Equal(at(12, 11, 40)) {
		t.Errorf("long pass skips a slot: got %v", got)
	}
	// One node is plain interval alignment.
	if got := nextCompactionStart(now, interval, 0, 1); !got.Equal(at(12, 5, 0)) {
		t.Errorf("slot 0 of 1: got %v", got)
	}
}

func TestWithCompactionSlotRejectsAnOutOfRangeSlot(t *testing.T) {
	if _, err := Open(t.TempDir(), WithAEAD(testAEAD(t)), WithCompactionSlot(3, 3)); err == nil {
		t.Fatal("slot 3 of 3 accepted")
	}
}

// pacedStore holds one segment of 1 MiB values with one deleted, so a pass
// relocates the rest, several buffers each.
func pacedStore(t *testing.T, clk *fakeClock, opts ...Option) (*Store, string, [32]byte, int) {
	t.Helper()
	opts = append([]Option{WithMaxSegSize(5 * MiB), withClock(clk)}, opts...)
	st, dir := openTestStore(t, opts...)
	oh := [32]byte{0x51}
	body := bytes.Repeat([]byte{0x5a}, 1*MiB)
	for i := range uint32(6) {
		putValue(t, st, oh, i, body)
	}
	if extentOf(t, st, oh, 0).SegNum != 0 || extentOf(t, st, oh, 3).SegNum != 0 {
		t.Fatalf("layout: values 0 and 3 not in segment 0")
	}
	for _, i := range []uint32{0, 1} {
		if _, err := st.Delete(oh, i); err != nil {
			t.Fatalf("delete: %v", err)
		}
	}
	return st, dir, oh, 6
}

func TestAPacedPassCopiesAtTheCap(t *testing.T) {
	clk := newFakeClock()
	const rate = 1 * MiB
	st, _, oh, n := pacedStore(t, clk, WithCompactionPacing(rate, 0, 0))

	p := st.newPacer(nil, false)
	start := clk.Now()
	if err := st.compactPass(p); err != nil {
		t.Fatalf("compact: %v", err)
	}
	moved := st.Snapshot().BytesRelocated
	if moved == 0 {
		t.Fatal("nothing relocated")
	}
	elapsed := clk.Now().Sub(start)
	want := time.Duration(float64(moved) / rate * float64(time.Second))
	// Rounded per buffer, so allow a millisecond.
	if elapsed < want-time.Millisecond {
		t.Fatalf("relocated %d bytes in %v, faster than the cap allows (%v)", moved, elapsed, want)
	}
	if elapsed > want+time.Second {
		t.Fatalf("relocated %d bytes in %v, well beyond the cap's %v", moved, elapsed, want)
	}
	for i := uint32(2); i < uint32(n); i++ {
		if got := readValue(t, st, oh, i); len(got) != 1*MiB {
			t.Fatalf("value %d reads back %d bytes", i, len(got))
		}
	}
}

func TestAPassStartingNearFullIsUnpaced(t *testing.T) {
	clk := newFakeClock()
	st, _, _, _ := pacedStore(t, clk, WithCompactionPacing(1*MiB, 0, 0))
	st.nearfullFreeFrac = 1.01

	if err := st.compactOnce(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if st.Snapshot().BytesRelocated == 0 {
		t.Fatal("nothing relocated")
	}
	if waited := clk.Now().Sub(newFakeClock().now); waited != 0 {
		t.Fatalf("a near-full pass waited %v", waited)
	}
}

func testCompactor(st *Store) *compactor {
	c := &compactor{store: st, done: make(chan struct{}), kick: make(chan struct{}, 1), lastRetained: st.clock.Now()}
	st.compactor = c
	return c
}

func TestAKickDuringAPacingWaitUnpacesTheRestAndRearms(t *testing.T) {
	clk := newFakeClock()
	st, _, _, _ := pacedStore(t, clk, WithCompactionPacing(1*MiB, 0, 0))
	c := testCompactor(st)

	waits := 0
	clk.onAfter = func(time.Duration) <-chan time.Time {
		waits++
		c.kick <- struct{}{}
		return make(chan time.Time)
	}
	p := st.newPacer(c, false)
	if err := st.compactPass(p); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if waits != 1 {
		t.Fatalf("pass waited %d times after the kick, want 1", waits)
	}
	if !p.urgent || !p.rekick {
		t.Fatalf("urgent %v rekick %v after a kick", p.urgent, p.rekick)
	}
}

func TestAStoreCrossingNearfullMidPassUnpacesIt(t *testing.T) {
	clk := newFakeClock()
	st, _, _, _ := pacedStore(t, clk, WithCompactionPacing(1*MiB, 0, 0))
	p := st.newPacer(nil, false)

	waits := 0
	clk.onAfter = func(d time.Duration) <-chan time.Time {
		waits++
		st.nearfullFreeFrac = 1.01
		clk.advance(d)
		ch := make(chan time.Time, 1)
		ch <- clk.Now()
		return ch
	}
	if err := st.compactPass(p); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if !p.urgent || !p.rekick {
		t.Fatalf("a pass whose store went near full stayed paced")
	}
	// The check is rate-limited, so the pass may wait until it next looks.
	if limit := int(urgencyCheckInterval/(256*time.Millisecond)) + 2; waits > limit {
		t.Fatalf("pass waited %d times after crossing nearfull", waits)
	}
}

func TestCloseDuringAPacingWaitStopsThePassAndKeepsTheSegment(t *testing.T) {
	clk := newFakeClock()
	st, dir, oh, n := pacedStore(t, clk, WithCompactionPacing(1*MiB, 0, 0))
	c := testCompactor(st)

	clk.onAfter = func(time.Duration) <-chan time.Time {
		select {
		case <-c.done:
		default:
			close(c.done)
		}
		return make(chan time.Time)
	}
	err := st.compactPass(st.newPacer(c, false))
	if !errors.Is(err, errCompactionStopped) {
		t.Fatalf("pass returned %v, want a stop", err)
	}
	if _, err := os.Stat(filepath.Join(dir, fmt.Sprintf(segFilename, 0))); err != nil {
		t.Fatalf("segment 0 dropped by a stopped pass: %v", err)
	}
	for i := uint32(2); i < uint32(n); i++ {
		if got := readValue(t, st, oh, i); len(got) != 1*MiB {
			t.Fatalf("value %d reads back %d bytes", i, len(got))
		}
	}
	// The next pass resumes and drops it.
	st.compactor = nil
	clk.onAfter = nil
	if err := st.compactOnce(); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, fmt.Sprintf(segFilename, 0))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("segment 0 not dropped by the resumed pass: %v", err)
	}
}

func TestTheRetainedSweepKeepsItsCadenceThroughALongPass(t *testing.T) {
	clk := newFakeClock()
	// 32 KiB/s makes the 2 MiB pass take about a minute of fake time.
	st, _, _, _ := pacedStore(t, clk, WithCompactionPacing(32*KiB, 0, 0))
	c := testCompactor(st)
	start := c.lastRetained

	var sweeps []time.Time
	prev := c.lastRetained
	clk.onAfter = func(d time.Duration) <-chan time.Time {
		if !c.lastRetained.Equal(prev) {
			sweeps = append(sweeps, c.lastRetained)
			prev = c.lastRetained
		}
		clk.advance(d)
		ch := make(chan time.Time, 1)
		ch <- clk.Now()
		return ch
	}
	if err := st.compactPass(st.newPacer(c, false)); err != nil {
		t.Fatalf("compact: %v", err)
	}
	elapsed := clk.Now().Sub(start)
	if elapsed < 45*time.Second {
		t.Fatalf("pass took %v of fake time; too short to test the cadence", elapsed)
	}
	if len(sweeps) < int(elapsed/retainedSweepInterval)-1 {
		t.Fatalf("%d retained sweeps in a %v pass", len(sweeps), elapsed)
	}
	last := start
	for _, at := range sweeps {
		// One buffer's wait is the most a sweep can slip past its cadence.
		if gap := at.Sub(last); gap > retainedSweepInterval+5*time.Second {
			t.Fatalf("retained sweep gap %v", gap)
		}
		last = at
	}
}

func TestAPassYieldsToForegroundLatencyWithinItsBudget(t *testing.T) {
	clk := newFakeClock()
	st, _, _, _ := pacedStore(t, clk, WithCompactionPacing(1*MiB, 25*time.Millisecond, 2*time.Second))

	// Pressure that lasts: every wait sees a slow foreground sync again.
	clk.onAfter = func(d time.Duration) <-chan time.Time {
		st.latency.record(latencySync, 80*time.Millisecond)
		clk.advance(d)
		ch := make(chan time.Time, 1)
		ch <- clk.Now()
		return ch
	}
	st.latency.record(latencyRead, 80*time.Millisecond)
	p := st.newPacer(nil, false)
	if err := st.compactPass(p); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if p.yielded != 2*time.Second {
		t.Fatalf("yielded %v under unbroken pressure, want the 2s budget", p.yielded)
	}
	if st.Snapshot().BytesRelocated == 0 {
		t.Fatal("the pass made no progress past its yield budget")
	}
}

func TestAPassResumesWhenForegroundLatencyFalls(t *testing.T) {
	clk := newFakeClock()
	st, _, _, _ := pacedStore(t, clk, WithCompactionPacing(1*MiB, 25*time.Millisecond, time.Minute))
	st.latency.record(latencyRead, 80*time.Millisecond)

	p := st.newPacer(nil, false)
	if err := st.compactPass(p); err != nil {
		t.Fatalf("compact: %v", err)
	}
	// One slow read ages out of the two one-second buckets.
	if p.yielded == 0 || p.yielded > 2*time.Second {
		t.Fatalf("yielded %v for one slow read", p.yielded)
	}
}

func TestOnlyForegroundIOFeedsTheLatencyMeter(t *testing.T) {
	st, _, oh, _ := pacedStore(t, newFakeClock(), WithCompactionPacing(1*MiB, 25*time.Millisecond, 0))
	// Real time, so a measured read is not zero.
	st.clock = wallClock{}
	st.latency.clock = wallClock{}
	st.latency.mu.Lock()
	st.latency.cur, st.latency.prev = [2]time.Duration{}, [2]time.Duration{}
	st.latency.mu.Unlock()

	if err := st.compactPass(st.newPacer(nil, true)); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if w := st.latency.worst(); w != 0 {
		t.Fatalf("compaction's own IO fed the meter: %v", w)
	}
	r, err := st.Lookup(oh, 2)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if _, err := io.Copy(io.Discard, r); err != nil {
		t.Fatalf("read: %v", err)
	}
	_ = r.Close()
	if w := st.latency.worst(); w == 0 {
		t.Fatal("a foreground read did not feed the meter")
	}
}

func TestTheYieldIsOffByDefault(t *testing.T) {
	st, _ := openTestStore(t)
	if st.latency != nil || st.compactionYieldLatency != 0 {
		t.Fatal("the foreground yield is on by default")
	}
	if st.compactionMaxBytesPerSec != DefaultCompactionMaxBytesPerSec {
		t.Fatalf("default cap %d", st.compactionMaxBytesPerSec)
	}
}
