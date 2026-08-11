package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A fullFreeFrac this close to 1 sits above any real disk's free-space
// fraction, so this rejects deterministically without mocking statfs.
func TestAppendRejectsWhenFull(t *testing.T) {
	st, _ := openTestStore(t, WithFreeSpaceWatermark(0.9999, 0.9999))

	_, err := st.Append([32]byte{0x1}, 0, 16)
	if !errors.Is(err, ErrStoreFull) {
		t.Fatalf("Append err = %v, want ErrStoreFull", err)
	}
}

// Zero thresholds accept any positive free-space fraction.
func TestAppendAcceptsWhenPermissive(t *testing.T) {
	st, _ := openTestStore(t, WithFreeSpaceWatermark(0, 0))

	w, err := st.Append([32]byte{0x2}, 0, 16)
	if err != nil {
		t.Fatalf("Append err = %v, want nil", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
}

// Opens directly (bypassing openTestStore's watermark override) to check the
// real package defaults against a dev disk.
func TestAppendNotFullByDefaultOnDevDisk(t *testing.T) {
	st, err := Open(t.TempDir(), WithAEAD(testAEAD(t)))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	w, err := st.Append([32]byte{0x3}, 0, 16)
	if err != nil {
		t.Fatalf("Append err = %v, want nil (dev disk should not be at the 5%% free default)", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
}

func TestWithFreeSpaceWatermarkRejectsInvertedThresholds(t *testing.T) {
	_, err := Open(t.TempDir(), WithAEAD(testAEAD(t)), WithFreeSpaceWatermark(0.05, 0.15))
	if err == nil {
		t.Fatalf("Open succeeded with full (0.15) > nearfull (0.05), want error")
	}
}

func TestWithFreeSpaceWatermarkRejectsOutOfRange(t *testing.T) {
	_, err := Open(t.TempDir(), WithAEAD(testAEAD(t)), WithFreeSpaceWatermark(0.15, -0.1))
	if err == nil {
		t.Fatalf("Open succeeded with a negative fraction, want error")
	}
}

func TestFreeSpaceFractionThrottled(t *testing.T) {
	st, _ := openTestStore(t)

	calls := 0
	orig := statfsFree
	statfsFree = func(string) (float64, error) {
		calls++
		return 0.5, nil
	}
	defer func() { statfsFree = orig }()

	if _, err := st.freeSpaceFraction(); err != nil {
		t.Fatalf("freeSpaceFraction: %v", err)
	}
	if _, err := st.freeSpaceFraction(); err != nil {
		t.Fatalf("freeSpaceFraction: %v", err)
	}
	if calls != 1 {
		t.Fatalf("statfsFree called %d times for 2 calls within %s, want 1 (throttle not applied)", calls, statfsThrottleInterval)
	}
}

func TestFreeSpaceFractionRefreshesAfterInterval(t *testing.T) {
	st, _ := openTestStore(t)

	calls := 0
	orig := statfsFree
	statfsFree = func(string) (float64, error) {
		calls++
		return 0.5, nil
	}
	defer func() { statfsFree = orig }()

	if _, err := st.freeSpaceFraction(); err != nil {
		t.Fatalf("freeSpaceFraction: %v", err)
	}
	// Force the cache to look stale without sleeping the test.
	st.statfsAt = st.statfsAt.Add(-2 * statfsThrottleInterval)
	if _, err := st.freeSpaceFraction(); err != nil {
		t.Fatalf("freeSpaceFraction: %v", err)
	}
	if calls != 2 {
		t.Fatalf("statfsFree called %d times across the throttle boundary, want 2", calls)
	}
}

// Crossing statfsBytesInterval must force a re-measure even though
// statfsThrottleInterval has not elapsed.
func TestFreeSpaceFractionRefreshesAfterByteThreshold(t *testing.T) {
	st, _ := openTestStore(t)

	calls := 0
	orig := statfsFree
	statfsFree = func(string) (float64, error) {
		calls++
		return 0.5, nil
	}
	defer func() { statfsFree = orig }()

	if _, err := st.freeSpaceFraction(); err != nil {
		t.Fatalf("freeSpaceFraction: %v", err)
	}
	// statfsAt is fresh, so only the byte bound can force the second measure.
	st.bytesSinceStatfs = statfsBytesInterval
	if _, err := st.freeSpaceFraction(); err != nil {
		t.Fatalf("freeSpaceFraction: %v", err)
	}
	if calls != 2 {
		t.Fatalf("statfsFree called %d times, want 2 (byte threshold must force a refresh)", calls)
	}
	if st.bytesSinceStatfs != 0 {
		t.Fatalf("bytesSinceStatfs = %d after refresh, want 0", st.bytesSinceStatfs)
	}
}

func TestAppendAccumulatesBytesSinceStatfs(t *testing.T) {
	st, _ := openTestStore(t)

	w, err := st.Append([32]byte{0x7}, 0, 16)
	if err != nil {
		t.Fatalf("Append err = %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	// Append re-measures at entry (resetting the counter), then reserves an
	// extent, so the counter must reflect that reservation.
	if st.bytesSinceStatfs == 0 {
		t.Fatalf("bytesSinceStatfs = 0 after Append, want the reserved extent size")
	}
}

// Append holds store.mutex while calling kickCompaction, so a blocking send
// here would deadlock.
func TestKickCompactionNonBlockingSend(t *testing.T) {
	st, _ := openTestStore(t)
	st.compactor = &compactor{store: st, done: make(chan struct{}), kick: make(chan struct{}, 1)}

	st.kickCompaction()

	select {
	case <-st.compactor.kick:
	default:
		t.Fatalf("kickCompaction did not send on the kick channel")
	}
}

func TestKickCompactionCoalescesWithoutBlocking(t *testing.T) {
	st, _ := openTestStore(t)
	st.compactor = &compactor{store: st, done: make(chan struct{}), kick: make(chan struct{}, 1)}

	st.kickCompaction() // fills the buffered channel

	done := make(chan struct{})
	go func() {
		st.kickCompaction() // must not block even though the channel is full
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("second kickCompaction blocked instead of coalescing")
	}
}

func TestKickCompactionNoopWithoutCompactor(t *testing.T) {
	st, _ := openTestStore(t)
	st.kickCompaction() // must not panic
}

// nearfull is advisory pressure, not a reject.
func TestAppendKicksCompactorOnNearfullButAccepts(t *testing.T) {
	st, _ := openTestStore(t, WithFreeSpaceWatermark(0.9999, 0))
	st.compactor = &compactor{store: st, done: make(chan struct{}), kick: make(chan struct{}, 1)}

	w, err := st.Append([32]byte{0x4}, 0, 16)
	if err != nil {
		t.Fatalf("Append err = %v, want nil (nearfull must still accept)", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	select {
	case <-st.compactor.kick:
	default:
		t.Fatalf("nearfull Append did not kick the compactor")
	}
}

func TestAppendDoesNotKickCompactorWhenPermissive(t *testing.T) {
	st, _ := openTestStore(t, WithFreeSpaceWatermark(0, 0))
	st.compactor = &compactor{store: st, done: make(chan struct{}), kick: make(chan struct{}, 1)}

	w, err := st.Append([32]byte{0x5}, 0, 16)
	if err != nil {
		t.Fatalf("Append err = %v, want nil", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	select {
	case <-st.compactor.kick:
		t.Fatalf("permissive Append kicked the compactor, want no-op")
	default:
	}
}

// Exercises the real select-case wiring in loop(), not just the helper that
// sends on the channel.
func TestCompactorLoopRunsOnKick(t *testing.T) {
	st, dir := openTestStore(t, WithMaxSegSize(40*KiB), WithCompaction(time.Hour))
	oh := [32]byte{0x66}
	body := make([]byte, 12*KiB)
	for i := range body {
		body[i] = 0xaa
	}
	putValue(t, st, oh, 0, body)
	putValue(t, st, oh, 1, body)
	putValue(t, st, oh, 2, body) // rolls into segment 1

	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// The ticker won't fire for an hour; only the kick drives this pass.
	st.compactor.kick <- struct{}{}

	segPath := filepath.Join(dir, fmt.Sprintf(segFilename, uint64(0)))
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(segPath); os.IsNotExist(err) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("kicked compaction did not drop segment 0 in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
