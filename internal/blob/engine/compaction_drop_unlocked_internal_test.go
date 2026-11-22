package engine

import (
	"bytes"
	"io"
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
