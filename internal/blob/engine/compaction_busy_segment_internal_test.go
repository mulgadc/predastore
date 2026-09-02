package engine

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A reader holds a reference to its segment until it is closed, and a reader
// blocked on a peer that stopped draining holds one indefinitely. Waiting for
// that reference under store.mutex would stall every other read and write on
// the node, so a busy segment is deferred to the next cycle instead.
func TestCompactionDefersSegmentWithLiveReader(t *testing.T) {
	st, dir, oh := segment0LayoutStore(t)

	if seg := extentOf(t, st, oh, 1).SegNum; seg != 0 {
		t.Fatalf("expected value 1 in segment 0, got %d", seg)
	}

	held, err := st.Lookup(oh, 1)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}

	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- st.compactOnce() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("compact: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("compactOnce blocked on a segment with a live reader")
	}

	seg0 := filepath.Join(dir, fmt.Sprintf(segFilename, 0))
	if _, err := os.Stat(seg0); err != nil {
		t.Fatalf("segment 0 should survive while a reader holds it: %v", err)
	}

	// The store must still be usable: the deferred drop is only correct if it
	// released the lock every other caller needs.
	if got := readValue(t, st, oh, 2); len(got) == 0 {
		t.Fatal("read of an unrelated value returned nothing")
	}
	putValue(t, st, oh, 9, bytes.Repeat([]byte{0x5a}, KiB))

	if err := held.Close(); err != nil {
		t.Fatalf("close held reader: %v", err)
	}

	if err := st.compactOnce(); err != nil {
		t.Fatalf("second compact: %v", err)
	}
	if _, err := os.Stat(seg0); !os.IsNotExist(err) {
		t.Errorf("segment 0 should be dropped once its reader closed, stat err=%v", err)
	}
}

// dropSegment must decline rather than wait, so the decision is exact under
// the caller's lock and never turns into a blocking one.
func TestDropSegmentReportsBusyWithoutWaiting(t *testing.T) {
	st, _, oh := segment0LayoutStore(t)

	held, err := st.Lookup(oh, 1)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	defer held.Close()

	st.mutex.Lock()
	err = st.dropSegment(0)
	st.mutex.Unlock()

	if err == nil {
		t.Fatal("dropSegment dropped a segment with a live reader")
	}
	if _, ok := st.segCache[0]; !ok {
		t.Error("a declined drop must leave the segment cached")
	}
}
