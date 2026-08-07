package engine

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestGetSegmentReadPathErrorsOnMissingSegment asserts that a read-path
// lookup of a segment that does not exist on disk returns an error and
// creates no file. Before the read/create split, getSegment opened every
// segment with os.O_CREATE regardless of caller intent, so a lookup against a
// dropped or absent segment silently fabricated a header-only stand-in
// instead of reporting it gone.
func TestGetSegmentReadPathErrorsOnMissingSegment(t *testing.T) {
	st, dir := openTestStore(t)

	const ghost = 99
	path := filepath.Join(dir, fmt.Sprintf(segFilename, ghost))
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("precondition: segment %d must not exist, stat gave %v", ghost, err)
	}

	st.mutex.Lock()
	_, err := st.getSegment(ghost, false)
	st.mutex.Unlock()

	if err == nil {
		t.Fatalf("getSegment(%d, false) returned no error for a missing segment", ghost)
	}

	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("getSegment(%d, false) fabricated %s despite erroring", ghost, filepath.Base(path))
	}
}

// TestCompactOnceSkipsUnopenableSegment asserts that one segment failing to
// open does not strand the rest of the cycle. candidateSegments and
// compactOnce must log the failing segment and continue with the remaining
// candidates rather than aborting selection or the whole cycle on the first
// error.
func TestCompactOnceSkipsUnopenableSegment(t *testing.T) {
	// Small segments so each shard lands in its own, giving several independent
	// candidates rather than one shared segment.
	st, dir := openTestStore(t, WithMaxSegSize(40*KiB))

	body := bytes.Repeat([]byte{0xcc}, 12*KiB)
	const shards = 5
	for i := range shards {
		oh := [32]byte{byte(0xA0 + i)}
		putShard(t, st, oh, 0, body)
		// Delete each one so its segment is all-dead and therefore a candidate.
		if _, err := st.Delete(oh, 0); err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
	}

	cands, failed, err := st.candidateSegments()
	if err != nil {
		t.Fatalf("candidates (clean): %v", err)
	}
	if failed != 0 {
		t.Fatalf("clean candidate scan reported %d failures, want 0", failed)
	}
	if len(cands) < 2 {
		t.Fatalf("test needs >=2 candidates to show one poisoning the rest, got %v", cands)
	}
	t.Logf("clean candidate set: %v", cands)

	// Poison exactly one candidate's opener. Everything else opens normally, so
	// anything left uncompacted is attributable to this one segment.
	victim := cands[0]
	victimPath := filepath.Join(dir, fmt.Sprintf(segFilename, victim))
	sentinel := errors.New("injected open failure")

	// Drop it from the cache first, otherwise getSegment never reaches openFile.
	st.mutex.Lock()
	delete(st.segCache, victim)
	st.mutex.Unlock()

	restore := SetOpenFile(func(path string, create bool) (File, error) {
		if path == victimPath {
			return nil, sentinel
		}
		flags := os.O_RDWR
		if create {
			flags |= os.O_CREATE
		}
		return os.OpenFile(path, flags, 0600)
	})
	defer restore()

	if err := st.CompactOnce(); err != nil {
		t.Fatalf("CompactOnce with segment %d unopenable: %v", victim, err)
	}

	// Every other, perfectly good candidate must still have been reclaimed.
	var stranded []uint64
	for _, c := range cands {
		if c == victim {
			continue
		}
		if _, err := os.Stat(filepath.Join(dir, fmt.Sprintf(segFilename, c))); err == nil {
			stranded = append(stranded, c)
		}
	}
	if len(stranded) != 0 {
		t.Errorf("one unopenable segment (%d) stranded %d of %d good candidates: %v",
			victim, len(stranded), len(cands)-1, stranded)
	}
}

// TestCompactOnceSkipsDroppedButStillTombstonedSegment covers the interaction
// between the read/create split and per-segment error isolation: a segment
// whose file is already gone but whose tombstones are still live in the index
// (e.g. a crash between dropSegment and deleteTombstones leaves this exact
// state) must be skipped, not stall the cycle.
//
// Before the read/create split, getSegment fabricated a header-only stand-in
// for the missing segment, which candidateSegments then discarded as
// all-header (size <= segHeaderSize) — masking the gap. Once reads stop
// creating, this segment starts erroring instead, and without per-segment
// isolation that single error would abort selection before any of the other
// candidates were even looked at. This regresses if either fix is reverted on
// its own.
func TestCompactOnceSkipsDroppedButStillTombstonedSegment(t *testing.T) {
	st, dir := openTestStore(t, WithMaxSegSize(40*KiB))

	body := bytes.Repeat([]byte{0xcc}, 12*KiB)
	const shards = 5
	for i := range shards {
		oh := [32]byte{byte(0xB0 + i)}
		putShard(t, st, oh, 0, body)
		if _, err := st.Delete(oh, 0); err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
	}

	cands, failed, err := st.candidateSegments()
	if err != nil {
		t.Fatalf("candidates (clean): %v", err)
	}
	if failed != 0 {
		t.Fatalf("clean candidate scan reported %d failures, want 0", failed)
	}
	if len(cands) < 2 {
		t.Fatalf("test needs >=2 candidates, got %v", cands)
	}
	t.Logf("clean candidate set: %v", cands)

	// Simulate a crash between dropSegment and deleteTombstones: unlink the
	// segment and its sidecar directly, leaving the tombstones that made it a
	// candidate still in the index.
	victim := cands[0]
	st.mutex.Lock()
	delete(st.segCache, victim)
	st.mutex.Unlock()
	if err := os.Remove(filepath.Join(dir, fmt.Sprintf(segFilename, victim))); err != nil {
		t.Fatalf("remove victim segment: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, fmt.Sprintf(idxSegFilename, victim))); err != nil {
		t.Fatalf("remove victim idx: %v", err)
	}

	if err := st.CompactOnce(); err != nil {
		t.Fatalf("CompactOnce with segment %d already dropped: %v", victim, err)
	}

	// The dropped segment must not be resurrected by the cycle that just skipped it.
	if _, err := os.Stat(filepath.Join(dir, fmt.Sprintf(segFilename, victim))); !os.IsNotExist(err) {
		t.Errorf("dropped segment %d was resurrected on disk, stat gave %v", victim, err)
	}

	// Every other candidate must still have been reclaimed: the missing one
	// must not stall the cycle.
	var stranded []uint64
	for _, c := range cands {
		if c == victim {
			continue
		}
		if _, err := os.Stat(filepath.Join(dir, fmt.Sprintf(segFilename, c))); err == nil {
			stranded = append(stranded, c)
		}
	}
	if len(stranded) != 0 {
		t.Errorf("already-dropped segment %d stalled the cycle: %d good candidates left uncompacted: %v",
			victim, len(stranded), stranded)
	}
}
