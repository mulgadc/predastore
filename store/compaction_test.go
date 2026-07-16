package store_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/storetest"
	"github.com/mulgadc/predastore/store"
)

func readAll(t *testing.T, st *store.Store, oh [32]byte, idx uint32) ([]byte, error) {
	t.Helper()
	r, err := st.Lookup(oh, idx)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

func openStore(t *testing.T, opts ...store.Option) (*store.Store, string) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(dir, append(opts, store.WithAEAD(storetest.TestAEAD()))...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return st, dir
}

func write(t *testing.T, st *store.Store, oh [32]byte, idx uint32, body []byte) {
	t.Helper()
	w, err := st.Append(oh, idx, int64(len(body)))
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := w.Write(body); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
}

func dirBytes(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		total += info.Size()
	}
	return total
}

func TestCompactionDropsDrainedSegmentAndShrinks(t *testing.T) {
	st, dir := openStore(t, store.WithMaxSegSize(20*store.KiB))
	defer st.Close()

	oh := [32]byte{0x1}
	body := bytes.Repeat([]byte{0xaa}, 12*store.KiB) // one ~12 KiB shard fills a 20 KiB segment
	write(t, st, oh, 0, body)                        // segment 0
	write(t, st, oh, 1, body)                        // rolls to segment 1
	write(t, st, oh, 2, body)                        // rolls to segment 2 (active)

	seg0 := filepath.Join(dir, fmt.Sprintf("%016d.seg", 0))
	idx0 := filepath.Join(dir, fmt.Sprintf("%016d.idx", 0))
	if _, err := os.Stat(seg0); err != nil {
		t.Fatalf("segment 0 should exist: %v", err)
	}

	before := dirBytes(t, dir)
	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := st.CompactOnce(); err != nil {
		t.Fatalf("compact: %v", err)
	}

	if _, err := os.Stat(seg0); !os.IsNotExist(err) {
		t.Errorf("segment 0 .seg should be unlinked, stat err=%v", err)
	}
	if _, err := os.Stat(idx0); !os.IsNotExist(err) {
		t.Errorf("segment 0 .idx should be unlinked, stat err=%v", err)
	}
	if after := dirBytes(t, dir); after >= before {
		t.Errorf("on-disk bytes should shrink: before=%d after=%d", before, after)
	}

	for _, idx := range []uint32{1, 2} {
		got, err := readAll(t, st, oh, idx)
		if err != nil {
			t.Fatalf("read surviving shard %d: %v", idx, err)
		}
		if !bytes.Equal(got, body) {
			t.Errorf("surviving shard %d corrupted", idx)
		}
	}
}

// persistedSegNum reads the active segment counter back out of state.json.
func persistedSegNum(t *testing.T, dir string) uint64 {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatalf("read state.json: %v", err)
	}
	var s struct {
		SegNum uint64 `json:"segNum"`
	}
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatalf("unmarshal state.json: %v", err)
	}
	return s.SegNum
}

// A compaction that drops a segment must persist the advanced segNum before the
// unlink. The append path flushes state.json only when it crosses a fragNum
// reservation boundary (every 1<<20 fragNums), so after a handful of writes the
// persisted segNum is still the value Open wrote while the active segment has
// rolled forward. If a dropped segment's number is at or above that stale value,
// an ungraceful restart reads it back and getSegment recreates an empty segment
// below the live data. Graceful Close would mask this by flushing on the way
// out, so the check is on state.json directly, not a reopen.
func TestCompactionPersistsSegNumBeforeDrop(t *testing.T) {
	st, dir := openStore(t, store.WithMaxSegSize(20*store.KiB))
	defer st.Close()

	oh := [32]byte{0x4}
	body := bytes.Repeat([]byte{0xaa}, 12*store.KiB)
	write(t, st, oh, 0, body) // segment 0
	write(t, st, oh, 1, body) // rolls to segment 1
	write(t, st, oh, 2, body) // rolls to segment 2 (active)

	// The writes never crossed a reservation boundary, so state.json is still
	// pinned at the segNum Open persisted while the active segment is now 2.
	if got := persistedSegNum(t, dir); got != 0 {
		t.Fatalf("precondition: expected stale persisted segNum 0, got %d", got)
	}

	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := st.CompactOnce(); err != nil {
		t.Fatalf("compact: %v", err)
	}

	seg0 := filepath.Join(dir, fmt.Sprintf("%016d.seg", 0))
	if _, err := os.Stat(seg0); !os.IsNotExist(err) {
		t.Fatalf("segment 0 should be unlinked by compaction, stat err=%v", err)
	}

	// The drop unlinked segment 0; the persisted segNum must now sit above it so a
	// restart resumes at the live active segment instead of recreating segment 0.
	if got := persistedSegNum(t, dir); got != 2 {
		t.Errorf("compaction must persist active segNum before dropping: state.json segNum=%d, want 2", got)
	}
}

// A relocation racing an overwrite must never resurrect the old bytes: the CAS
// aborts when the slot moved, so the final read is always the newest write.
func TestConcurrentOverwriteDuringCompaction(t *testing.T) {
	st, _ := openStore(t, store.WithMaxSegSize(40*store.KiB))
	defer st.Close()

	oh := [32]byte{0x2}
	filler := bytes.Repeat([]byte{0xbb}, 12*store.KiB)
	write(t, st, oh, 0, filler) // segment 0
	write(t, st, oh, 1, filler) // segment 0
	write(t, st, oh, 2, filler) // rolls; segment 0 now drainable once a shard dies

	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}

	final := bytes.Repeat([]byte{0xff}, 16*store.KiB)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 50 {
			_ = st.CompactOnce()
		}
	}()
	go func() {
		defer wg.Done()
		for range 50 {
			write(t, st, oh, 1, filler)
		}
		write(t, st, oh, 1, final)
	}()
	wg.Wait()

	got, err := readAll(t, st, oh, 1)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, final) {
		t.Fatalf("expected newest write, got stale bytes")
	}
}

func TestCompactorLifecycleStartsAndStops(t *testing.T) {
	st, _ := openStore(t, store.WithCompaction(time.Hour))

	done := make(chan error, 1)
	go func() { done <- st.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not join the compactor goroutine")
	}
}

func TestNoCompactorWithoutOption(t *testing.T) {
	st, _ := openStore(t)
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// A fault mid-compaction must leave every live shard readable on reopen — the
// drained segment is only dropped after all relocations commit, so an aborted
// cycle loses nothing.
func TestCompactionFaultLeavesLiveDataReadable(t *testing.T) {
	var armed bool
	restore := store.SetOpenFile(func(path string, create bool) (store.File, error) {
		flags := os.O_RDWR
		if create {
			flags |= os.O_CREATE
		}
		f, err := os.OpenFile(path, flags, 0600)
		if err != nil {
			return nil, err
		}
		return &flakyFile{File: f, armed: &armed}, nil
	})
	defer restore()

	dir := t.TempDir()

	oh := [32]byte{0x3}
	body := bytes.Repeat([]byte{0xcc}, 12*store.KiB)

	st, err := store.Open(dir, store.WithMaxSegSize(40*store.KiB), store.WithAEAD(storetest.TestAEAD()))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	write(t, st, oh, 0, body)
	write(t, st, oh, 1, body)
	write(t, st, oh, 2, body)
	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}

	armed = true
	_ = st.CompactOnce() // expected to fail somewhere mid-cycle
	armed = false
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	st2, err := store.Open(dir, store.WithMaxSegSize(40*store.KiB), store.WithAEAD(storetest.TestAEAD()))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()

	for _, idx := range []uint32{1, 2} {
		got, err := readAll(t, st2, oh, idx)
		if err != nil {
			t.Fatalf("live shard %d unreadable after interrupted compaction: %v", idx, err)
		}
		if !bytes.Equal(got, body) {
			t.Fatalf("live shard %d corrupted after interrupted compaction", idx)
		}
	}
}

// flakyFile fails the first WriteAt issued while armed, simulating a crash
// partway through a relocation copy.
type flakyFile struct {
	*os.File

	armed *bool
}

func (f *flakyFile) WriteAt(p []byte, off int64) (int, error) {
	if f.armed != nil && *f.armed {
		*f.armed = false
		return 0, fmt.Errorf("injected write fault")
	}
	return f.File.WriteAt(p, off)
}
