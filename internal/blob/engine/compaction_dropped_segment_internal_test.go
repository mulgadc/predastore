package engine

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

// tombstoneCount reports how many tombstones name the given segment.
func tombstoneCount(t *testing.T, st *Store, segNum uint64) int {
	t.Helper()
	var n int
	err := st.indexScan([]byte{tombstonePrefix}, func(k, v []byte) error {
		if len(k) == tombstoneKeySize && tombstoneSegNum(k) == segNum {
			n++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan tombstones: %v", err)
	}
	return n
}

// TestCompactionReclaimsTombstoneOfDroppedSegment pins that a tombstone naming
// a segment the store has already unlinked is reclaimed on the cycle that finds
// it, rather than re-reported as an unreadable candidate on every later cycle.
func TestCompactionReclaimsTombstoneOfDroppedSegment(t *testing.T) {
	st, dir, oh := segment0LayoutStore(t)

	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := st.compactOnce(); err != nil {
		t.Fatalf("compact: %v", err)
	}

	path := filepath.Join(dir, fmt.Sprintf(segFilename, 0))
	if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("segment 0 was not dropped, so the layout assumption is broken: %v", err)
	}
	if n := tombstoneCount(t, st, 0); n != 0 {
		t.Fatalf("drop left %d tombstones for segment 0, want 0", n)
	}

	// What a delete racing the unlocked relocation copy commits: a tombstone
	// naming the segment whose file the drop has already unlinked.
	err := st.index.Update(func(txn *badger.Txn) error {
		return txn.Set(tombstoneKey(0, int64(segHeaderSize)), tombstoneValue(4096))
	})
	if err != nil {
		t.Fatalf("inject tombstone: %v", err)
	}

	if err := st.compactOnce(); err != nil {
		t.Fatalf("compact after injection: %v", err)
	}
	if n := tombstoneCount(t, st, 0); n != 0 {
		t.Fatalf("tombstone for dropped segment 0 survived a compaction cycle (%d left), so every later cycle rescans it", n)
	}

	before := st.Snapshot().CompactionCyclesFailed
	if err := st.compactOnce(); err != nil {
		t.Fatalf("compact again: %v", err)
	}
	if after := st.Snapshot().CompactionCyclesFailed; after != before {
		t.Fatalf("cycle still counted a failure after the dropped-segment tombstone was seen: %d -> %d", before, after)
	}
}

// TestCandidateSegmentsReportsSegmentThatExistsButWillNotOpen asserts the
// reclaim above is scoped to a missing file. A segment that is present and
// fails to open is the disk fault the ERROR exists for, and must still count.
func TestCandidateSegmentsReportsSegmentThatExistsButWillNotOpen(t *testing.T) {
	st, dir, oh := segment0LayoutStore(t)

	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Drop it from the cache first, or getSegment never reaches openFile.
	st.mutex.Lock()
	delete(st.segCache, 0)
	st.mutex.Unlock()

	victim := filepath.Join(dir, fmt.Sprintf(segFilename, 0))
	sentinel := errors.New("injected open failure")
	restore := SetOpenFile(func(path string, create bool) (File, error) {
		if path == victim {
			return nil, sentinel
		}
		flags := os.O_RDWR
		if create {
			flags |= os.O_CREATE
		}
		return os.OpenFile(path, flags, 0600)
	})
	defer restore()

	_, failed, err := st.candidateSegments()
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if failed != 1 {
		t.Fatalf("candidate scan reported %d failures for an unopenable segment, want 1", failed)
	}
	if n := tombstoneCount(t, st, 0); n == 0 {
		t.Fatal("tombstones were reclaimed for a segment that still exists on disk")
	}
}
