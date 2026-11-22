package engine

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// holdSegmentUnlink makes the next unlink of a .seg file block until release
// is called, and reports on entered once it is blocked. release is idempotent
// and runs at cleanup, so a failing test never leaves the drop hung.
func holdSegmentUnlink(t *testing.T) (entered <-chan struct{}, release func()) {
	t.Helper()
	in := make(chan struct{})
	out := make(chan struct{})
	var enter, leave sync.Once
	prev := removeFile
	removeFile = func(path string) error {
		if filepath.Ext(path) == ".seg" {
			enter.Do(func() { close(in) })
			<-out
		}
		return prev(path)
	}
	release = func() { leave.Do(func() { close(out) }) }
	t.Cleanup(func() {
		release()
		removeFile = prev
	})
	return in, release
}

// A drop's unlink can take seconds on a sealed segment. Shard writes and reads
// on the node must not wait behind it, or every PUT striped across the node
// stalls with them.
func TestCompaction_DropDoesNotBlockAppend(t *testing.T) {
	st, _, oh := segment0LayoutStore(t)
	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}

	entered, release := holdSegmentUnlink(t)
	compacted := make(chan error, 1)
	go func() { compacted <- st.compactOnce() }()
	select {
	case <-entered:
	case err := <-compacted:
		t.Fatalf("compaction finished without unlinking a segment: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("compaction never reached the segment unlink")
	}

	body := bytes.Repeat([]byte{0xab}, 4*KiB)
	served := make(chan error, 1)
	go func() {
		w, err := st.Append([32]byte{0x77}, 0, int64(len(body)), nextTestEpoch())
		if err == nil {
			_, err = w.Write(body)
		}
		if err == nil {
			err = w.Close()
		}
		if err == nil {
			var r Reader
			if r, err = st.Lookup(oh, 1); err == nil {
				_, err = io.Copy(io.Discard, r)
				_ = r.Close()
			}
		}
		served <- err
	}()

	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("write or read during the drop's unlink: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a write and a read waited behind the drop's unlink")
	}

	release()
	if err := <-compacted; err != nil {
		t.Fatalf("compact: %v", err)
	}
}

// failUnlink makes unlinks of files with the given extension fail until the
// returned restore runs, which it also does at cleanup.
func failUnlink(t *testing.T, ext string) (restore func()) {
	t.Helper()
	prev := removeFile
	removeFile = func(path string) error {
		if filepath.Ext(path) == ext {
			return errors.New("injected unlink failure")
		}
		return prev(path)
	}
	var once sync.Once
	restore = func() { once.Do(func() { removeFile = prev }) }
	t.Cleanup(restore)
	return restore
}

func segmentFiles(t *testing.T, dir string, num uint64) (seg, idx bool) {
	t.Helper()
	exists := func(name string) bool {
		_, err := os.Stat(filepath.Join(dir, fmt.Sprintf(name, num)))
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("stat: %v", err)
		}
		return err == nil
	}
	return exists(segFilename), exists(idxSegFilename)
}

// An unlink that fails leaves the segment's files and tombstones, so the next
// cycle drops it again; nothing is counted reclaimed until the files are gone.
func TestCompaction_FailedUnlinkIsRetriedNextCycle(t *testing.T) {
	st, dir, oh := segment0LayoutStore(t)
	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}

	restore := failUnlink(t, ".seg")
	if err := st.compactOnce(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if seg, idx := segmentFiles(t, dir, 0); !seg || !idx {
		t.Fatalf("failed unlink left seg=%v idx=%v, want both", seg, idx)
	}
	if n := tombstoneCount(t, st, 0); n == 0 {
		t.Fatal("tombstones went with the drop unfinished, so nothing retries it")
	}
	if got := st.Snapshot().BytesReclaimed; got != 0 {
		t.Fatalf("a failed unlink counted %d bytes reclaimed", got)
	}

	restore()
	if err := st.compactOnce(); err != nil {
		t.Fatalf("compact again: %v", err)
	}
	if seg, idx := segmentFiles(t, dir, 0); seg || idx {
		t.Fatalf("retried drop left seg=%v idx=%v", seg, idx)
	}
	if n := tombstoneCount(t, st, 0); n != 0 {
		t.Fatalf("retried drop left %d tombstones", n)
	}
	if st.Snapshot().BytesReclaimed == 0 {
		t.Fatal("the completed drop counted nothing reclaimed")
	}
	if got := readValue(t, st, oh, 1); len(got) != 12*KiB {
		t.Fatalf("relocated value read back %d bytes", len(got))
	}
}

// A drop interrupted after the .seg went but before the sidecar did leaves the
// sidecar alone on disk. The next cycle removes it with the tombstones.
func TestCompaction_SidecarLeftByAnInterruptedDropIsRemoved(t *testing.T) {
	st, dir, oh := segment0LayoutStore(t)
	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}

	restore := failUnlink(t, ".idx")
	if err := st.compactOnce(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if seg, idx := segmentFiles(t, dir, 0); seg || !idx {
		t.Fatalf("interrupted drop left seg=%v idx=%v, want only the sidecar", seg, idx)
	}

	restore()
	if err := st.compactOnce(); err != nil {
		t.Fatalf("compact again: %v", err)
	}
	if _, idx := segmentFiles(t, dir, 0); idx {
		t.Fatal("the sidecar of a dropped segment survived the next cycle")
	}
	if n := tombstoneCount(t, st, 0); n != 0 {
		t.Fatalf("%d tombstones survived the cleanup", n)
	}
}

// A segment whose sidecar alone is missing still holds its data on disk, so
// its tombstones are not reclaimed as if it had been dropped.
func TestCandidateSegments_MissingSidecarIsNotADroppedSegment(t *testing.T) {
	st, dir, oh := segment0LayoutStore(t)
	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}

	st.mutex.Lock()
	seg := st.segCache[0]
	delete(st.segCache, 0)
	st.mutex.Unlock()
	if err := seg.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := seg.idx.Close(); err != nil {
		t.Fatalf("close idx: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, fmt.Sprintf(idxSegFilename, 0))); err != nil {
		t.Fatalf("remove sidecar: %v", err)
	}

	_, failed, err := st.candidateSegments()
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if failed != 1 {
		t.Fatalf("a segment missing only its sidecar reported %d failures, want 1", failed)
	}
	if n := tombstoneCount(t, st, 0); n == 0 {
		t.Fatal("tombstones were reclaimed for a segment still on disk")
	}
}

// A drop interrupted at either unlink, then a restart: the reopened store
// finishes the drop from the tombstones left behind, and the data it
// relocated still reads.
func TestCompaction_InterruptedDropRecoversAfterReopen(t *testing.T) {
	for _, tc := range []struct {
		failing          string
		leftSeg, leftIdx bool
	}{
		{failing: ".seg", leftSeg: true, leftIdx: true},
		{failing: ".idx", leftSeg: false, leftIdx: true},
	} {
		t.Run("failing="+tc.failing, func(t *testing.T) {
			st, dir, oh := segment0LayoutStore(t)
			if _, err := st.Delete(oh, 0); err != nil {
				t.Fatalf("delete: %v", err)
			}
			restore := failUnlink(t, tc.failing)
			if err := st.compactOnce(); err != nil {
				t.Fatalf("compact: %v", err)
			}
			restore()
			if seg, idx := segmentFiles(t, dir, 0); seg != tc.leftSeg || idx != tc.leftIdx {
				t.Fatalf("interrupted drop left seg=%v idx=%v, want seg=%v idx=%v", seg, idx, tc.leftSeg, tc.leftIdx)
			}
			if err := st.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			re, err := Open(dir, WithFreeSpaceWatermark(0, 0), WithMaxSegSize(40*KiB), WithAEAD(testAEAD(t)))
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			t.Cleanup(func() { _ = re.Close() })
			if n := tombstoneCount(t, re, 0); n == 0 {
				t.Fatal("the restart lost the tombstones that retry the drop")
			}
			if err := re.compactOnce(); err != nil {
				t.Fatalf("compact after reopen: %v", err)
			}
			if seg, idx := segmentFiles(t, dir, 0); seg || idx {
				t.Fatalf("after reopen and a cycle, seg=%v idx=%v remain", seg, idx)
			}
			if n := tombstoneCount(t, re, 0); n != 0 {
				t.Fatalf("after reopen and a cycle, %d tombstones remain", n)
			}
			if got := readValue(t, re, oh, 1); len(got) != 12*KiB {
				t.Fatalf("relocated value read back %d bytes after reopen", len(got))
			}
		})
	}
}
