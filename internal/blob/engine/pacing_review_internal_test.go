package engine

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A writer reserved its slot in a segment that then rolled, and closes and
// commits while a pass is pacing over the segment's other extents. Its value
// must survive the pass and a reopen.
func TestAWriterClosingMidPassKeepsItsValue(t *testing.T) {
	clk := newFakeClock()
	st, dir := openTestStore(t, WithMaxSegSize(40*KiB), withClock(clk), WithCompactionPacing(1*MiB, 0, 0))
	oh := [32]byte{0x61}
	body := bytes.Repeat([]byte{0x6a}, 8*KiB)

	epoch := nextTestEpoch()
	w, err := st.Append(oh, 9, int64(len(body)), epoch)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := w.Write(body); err != nil {
		t.Fatalf("write: %v", err)
	}
	putValue(t, st, oh, 0, body)
	putValue(t, st, oh, 1, body)
	putValue(t, st, oh, 2, body)
	putValue(t, st, oh, 3, bytes.Repeat([]byte{0x6b}, 16*KiB))
	if extentOf(t, st, oh, 0).SegNum != 0 || extentOf(t, st, oh, 3).SegNum == 0 {
		t.Fatal("layout: the writer's segment did not roll")
	}
	for _, i := range []uint32{1, 2} {
		if _, err := st.Delete(oh, i); err != nil {
			t.Fatalf("delete: %v", err)
		}
	}

	closed := false
	clk.onAfter = func(d time.Duration) <-chan time.Time {
		if !closed {
			closed = true
			if err := w.Close(); err != nil {
				t.Errorf("close writer: %v", err)
			}
			if _, err := st.Commit(oh, 9, epoch); err != nil {
				t.Errorf("commit: %v", err)
			}
		}
		clk.advance(d)
		ch := make(chan time.Time, 1)
		ch <- clk.Now()
		return ch
	}
	if err := st.compactPass(st.newPacer(nil, false)); err != nil {
		t.Fatalf("compact: %v", err)
	}
	clk.onAfter = nil
	if !closed {
		if err := w.Close(); err != nil {
			t.Fatalf("close writer: %v", err)
		}
		if _, err := st.Commit(oh, 9, epoch); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}
	if err := st.compactOnce(); err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if got := readValue(t, st, oh, 9); !bytes.Equal(got, body) {
		t.Fatal("the writer's value does not read back after compaction")
	}
	st = reopen(t, st, dir)
	if got := readValue(t, st, oh, 9); !bytes.Equal(got, body) {
		t.Fatal("the writer's value does not read back after reopen")
	}
}

// A cap low enough that one buffer's wait runs for minutes still sees the
// store drop into the full band, with no kick, within a pacing step.
func TestALongRateWaitSeesAFullStoreAndKeepsSweeping(t *testing.T) {
	clk := newFakeClock()
	st, _, _, _ := pacedStore(t, clk, WithCompactionPacing(1*KiB, 0, 0))
	c := testCompactor(st)
	start := clk.Now()

	waits, fullAt := 0, time.Time{}
	clk.onAfter = func(d time.Duration) <-chan time.Time {
		waits++
		if d > maxPaceStep {
			t.Errorf("one rate wait of %v", d)
		}
		if waits == 40 {
			st.nearfullFreeFrac, st.fullFreeFrac = 1.01, 1.0
			fullAt = clk.Now()
		}
		clk.advance(d)
		ch := make(chan time.Time, 1)
		ch <- clk.Now()
		return ch
	}
	p := st.newPacer(c, false)
	if err := st.compactPass(p); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if !p.urgent {
		t.Fatal("the pass never saw the full store")
	}
	if late := clk.Now().Sub(fullAt); late > 2*maxPaceStep {
		t.Fatalf("the pass kept pacing %v after the store went full", late)
	}
	if c.lastRetained.Sub(start) < 30*time.Second {
		t.Fatalf("no retained sweep through %v of pacing", fullAt.Sub(start))
	}
}

// Store.Close during the final extent's waits finishes that extent, keeps the
// segment, and a reopened store reclaims it.
func TestCloseDuringTheLastExtentKeepsTheSegment(t *testing.T) {
	for _, tc := range []struct {
		name  string
		yield bool
	}{{"rate wait", false}, {"yield wait", true}} {
		t.Run(tc.name, func(t *testing.T) {
			clk := newFakeClock()
			yieldAt := time.Duration(0)
			if tc.yield {
				yieldAt = 25 * time.Millisecond
			}
			st, dir, oh, _ := pacedStore(t, clk, WithCompactionPacing(1*MiB, yieldAt, time.Minute))
			// Leave one live extent, so the one in flight is the last.
			for _, i := range []uint32{2} {
				if _, err := st.Delete(oh, i); err != nil {
					t.Fatalf("delete: %v", err)
				}
			}
			want := readValue(t, st, oh, 3)
			if tc.yield {
				st.latency.record(latencySync, 80*time.Millisecond)
			}

			c := testCompactor(st)
			closed := make(chan error, 1)
			clk.onAfter = func(time.Duration) <-chan time.Time {
				go func() { closed <- st.Close() }()
				return make(chan time.Time)
			}
			c.wg.Add(1)
			passErr := make(chan error, 1)
			go func() {
				defer c.wg.Done()
				passErr <- st.compactPass(st.newPacer(c, false))
			}()
			if err := <-passErr; !errors.Is(err, errCompactionStopped) {
				t.Fatalf("pass returned %v, want a stop", err)
			}
			if err := <-closed; err != nil {
				t.Fatalf("close: %v", err)
			}
			if _, err := os.Stat(filepath.Join(dir, fmt.Sprintf(segFilename, 0))); err != nil {
				t.Fatalf("segment 0 dropped on close: %v", err)
			}

			st2, err := Open(dir, WithFreeSpaceWatermark(0, 0), WithAEAD(testAEAD(t)), WithMaxSegSize(5*MiB))
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			t.Cleanup(func() { _ = st2.Close() })
			if got := readValue(t, st2, oh, 3); !bytes.Equal(got, want) {
				t.Fatal("the last extent's value does not read back")
			}
			if err := st2.compactOnce(); err != nil {
				t.Fatalf("resume: %v", err)
			}
			if _, err := os.Stat(filepath.Join(dir, fmt.Sprintf(segFilename, 0))); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("segment 0 not reclaimed after reopen: %v", err)
			}
			if got := readValue(t, st2, oh, 3); !bytes.Equal(got, want) {
				t.Fatal("the value does not read back after the resumed pass")
			}
		})
	}
}
