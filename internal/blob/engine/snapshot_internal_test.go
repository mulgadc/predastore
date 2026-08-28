package engine

import (
	"bytes"
	"testing"
)

// A store nothing has been written to has taken no free-space measurement and
// run no compaction scan, and must say so rather than report zeroes that read
// as a full disk holding no live data.
func TestSnapshotReportsNothingMeasuredOnAFreshStore(t *testing.T) {
	st, _ := openTestStore(t)

	s := st.Snapshot()
	if s.Measured {
		t.Errorf("Measured = true on a store that has never appended")
	}
	if s.Scanned {
		t.Errorf("Scanned = true before any compaction cycle")
	}
	if s.LiveSegments != 1 {
		t.Errorf("LiveSegments = %d on a fresh store, want 1", s.LiveSegments)
	}
}

func TestSnapshotMeasuresFreeSpaceOnceWritten(t *testing.T) {
	st, _ := openTestStore(t)
	putValue(t, st, [32]byte{0x42}, 0, []byte("value"))

	s := st.Snapshot()
	if !s.Measured {
		t.Fatalf("Measured = false after an append, want true")
	}
	if s.TotalBytes == 0 {
		t.Errorf("TotalBytes = 0, want the size of the backing filesystem")
	}
	if s.FreeBytes > s.TotalBytes {
		t.Errorf("FreeBytes %d exceeds TotalBytes %d", s.FreeBytes, s.TotalBytes)
	}
	if s.FreeFrac <= 0 || s.FreeFrac > 1 {
		t.Errorf("FreeFrac = %v, want a fraction in (0,1]", s.FreeFrac)
	}
}

// The band must follow the same comparison Append gates on, so the metric and
// the rejection cannot disagree about whether a store is full.
func TestSnapshotReportsPressureBand(t *testing.T) {
	orig := statfsFree
	defer func() { statfsFree = orig }()

	for _, tc := range []struct {
		name  string
		free  uint64
		total uint64
		want  string
	}{
		{name: "ok", free: 500, total: 1000, want: PressureOK},
		{name: "nearfull", free: 100, total: 1000, want: PressureNearFull},
		{name: "full", free: 10, total: 1000, want: PressureFull},
	} {
		t.Run(tc.name, func(t *testing.T) {
			statfsFree = func(string) (uint64, uint64, error) { return tc.free, tc.total, nil }

			// Watermarks left at their defaults, which is what production runs.
			st, _ := openTestStore(t, WithFreeSpaceWatermark(defaultNearfullFreeFrac, defaultFullFreeFrac))
			// A full store rejects the append, which is the point; the
			// measurement it took on the way to rejecting is what is under
			// test. A writer that was handed out holds a segment reference, so
			// it has to be closed or the store never finishes closing.
			if w, err := st.Append([32]byte{0x42}, 0, 16); err == nil {
				if err := w.Close(); err != nil {
					t.Fatalf("close writer: %v", err)
				}
			}

			if got := st.Snapshot().Pressure; got != tc.want {
				t.Errorf("Pressure = %q at %d/%d free, want %q", got, tc.free, tc.total, tc.want)
			}
		})
	}
}

// Segments are counted as they are created and dropped, never by walking the
// directory on the reporting path.
func TestSnapshotTracksLiveSegmentsAcrossRollAndDrop(t *testing.T) {
	st, _, oh := segment0LayoutStore(t)

	// Three ~12 KiB values under a 40 KiB cap: two in segment 0, one in segment 1.
	if got := st.Snapshot().LiveSegments; got != 2 {
		t.Fatalf("LiveSegments = %d after rolling, want 2", got)
	}

	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := st.compactOnce(); err != nil {
		t.Fatalf("compact: %v", err)
	}

	if got := st.Snapshot().LiveSegments; got != 1 {
		t.Errorf("LiveSegments = %d after segment 0 was drained, want 1", got)
	}
}

// A store reopened from an existing directory must count what is already there,
// or the gauge would restart at zero and read as a store that lost its data.
func TestSnapshotCountsExistingSegmentsAtOpen(t *testing.T) {
	st, dir, _ := segment0LayoutStore(t)
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(dir, WithAEAD(testAEAD(t)), WithFreeSpaceWatermark(0, 0))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	if got := reopened.Snapshot().LiveSegments; got != 2 {
		t.Errorf("LiveSegments = %d after reopening a two-segment store, want 2", got)
	}
}

func TestSnapshotRecordsCompactionCycle(t *testing.T) {
	st, _, oh := segment0LayoutStore(t)
	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := st.compactOnce(); err != nil {
		t.Fatalf("compact: %v", err)
	}

	s := st.Snapshot()
	if s.CompactionCycles != 1 {
		t.Errorf("CompactionCycles = %d, want 1", s.CompactionCycles)
	}
	if s.CompactionCyclesFailed != 0 {
		t.Errorf("CompactionCyclesFailed = %d on a clean cycle, want 0", s.CompactionCyclesFailed)
	}
	if s.SegmentsScanned != 1 || s.SegmentsDropped != 1 {
		t.Errorf("segments scanned/dropped = %d/%d, want 1/1", s.SegmentsScanned, s.SegmentsDropped)
	}
	if s.BytesRelocated <= 0 {
		t.Errorf("BytesRelocated = %d, want the live value moved out of segment 0", s.BytesRelocated)
	}
	// Reclaimed is the whole source file, so it must exceed what was relocated
	// out of it — otherwise the cycle gave back nothing and only did work.
	if s.BytesReclaimed <= s.BytesRelocated {
		t.Errorf("BytesReclaimed = %d, want more than the %d relocated", s.BytesReclaimed, s.BytesRelocated)
	}
	if !s.Scanned {
		t.Errorf("Scanned = false after a cycle, want true")
	}
	if s.LiveBytes <= 0 {
		t.Errorf("LiveBytes = %d after a cycle, want the surviving values", s.LiveBytes)
	}
}

// Corruption is counted where it is detected, because the error travels up
// through callers that cannot tell it from a missing key.
func TestSnapshotCountsIntegrityFailures(t *testing.T) {
	st, dir, oh := segment0LayoutStore(t)

	before := st.Snapshot().IntegrityFailures

	// Flip a ciphertext byte in place: the header still parses, so the read
	// reaches the tag check rather than failing earlier.
	ext := extentOf(t, st, oh, 1)
	corruptFragmentBody(t, dir, ext)

	if _, err := readValueErr(st, oh, 1); err == nil {
		t.Fatalf("read of a corrupted value succeeded, want an integrity failure")
	}

	if got := st.Snapshot().IntegrityFailures; got != before+1 {
		t.Errorf("IntegrityFailures = %d, want %d", got, before+1)
	}
}

// corruptFragmentBody flips one ciphertext byte of an extent's first fragment,
// writing straight to the segment file so the store never sees it happen.
func corruptFragmentBody(t *testing.T, dir string, ext extent) {
	t.Helper()
	seg, _, err := openSegment(dir, ext.SegNum, false)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	defer seg.Close()

	off := ext.Off + fragHeaderSize
	buf := make([]byte, 1)
	if _, err := seg.ReadAt(buf, off); err != nil {
		t.Fatalf("read fragment body: %v", err)
	}
	buf[0] ^= 0xff
	if _, err := seg.WriteAt(buf, off); err != nil {
		t.Fatalf("write fragment body: %v", err)
	}
	if err := seg.Sync(); err != nil {
		t.Fatalf("sync segment: %v", err)
	}
}

// readValueErr reads a value and returns the error rather than failing, which
// a corruption test needs.
func readValueErr(st *Store, oh [32]byte, idx uint32) ([]byte, error) {
	r, err := st.Lookup(oh, idx)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(r); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
