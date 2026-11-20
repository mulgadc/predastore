package engine

import (
	"bytes"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
)

// Costs of the shard delete and release paths. Run with -bench and compare
// across revisions with benchstat; fixtures are built outside the timer.

var benchSeq atomic.Uint64

// benchPosition commits a live generation at a fresh position, writes
// retained rows behind it and releases the first released of them.
func benchPosition(st *Store, retained, released int) ([32]byte, error) {
	var oh [32]byte
	n := benchSeq.Add(1)
	copy(oh[:16], fmt.Appendf(nil, "bench%011x", n))
	live := nextTestEpoch()
	w, err := st.Append(oh, 0, 4, live)
	if err != nil {
		return oh, err
	}
	if _, err := w.Write([]byte("live")); err != nil {
		return oh, err
	}
	if err := w.Close(); err != nil {
		return oh, err
	}
	if _, err := st.Commit(oh, 0, live); err != nil {
		return oh, err
	}
	if retained == 0 {
		return oh, nil
	}

	idxKey := MakeKey(oh, 0)
	wb := st.index.NewWriteBatch()
	for i := range retained {
		e := uint64(i + 1)
		ext := extent{SegNum: n<<20 | e, Off: int64(i) * 4096, PSize: 4096}
		if err := wb.Set(retainedKey(idxKey, e), encodeIndexValue(ext, e)); err != nil {
			return oh, err
		}
	}
	if err := wb.Flush(); err != nil {
		return oh, err
	}
	for i := range released {
		if err := st.ReleaseGeneration(oh, 0, uint64(i+1)); err != nil {
			return oh, err
		}
	}

	return oh, nil
}

func mustBenchPosition(b *testing.B, st *Store, retained, released int) [32]byte {
	b.Helper()
	oh, err := benchPosition(st, retained, released)
	if err != nil {
		b.Fatalf("fixture: %v", err)
	}

	return oh
}

func openBenchStore(b *testing.B) *Store {
	b.Helper()
	// The store logs on open, which splits benchmark result lines.
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.DiscardHandler))
	b.Cleanup(func() { slog.SetDefault(prev) })
	aead, err := masterkey.NewAEAD(bytes.Repeat([]byte{0x42}, 32))
	if err != nil {
		b.Fatalf("aead: %v", err)
	}
	st, err := Open(b.TempDir(), WithFreeSpaceWatermark(0, 0), WithAEAD(aead))
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	b.Cleanup(func() { _ = st.Close() })

	return st
}

// One Store.Delete per op, over a position with the given history.
func BenchmarkDelete(b *testing.B) {
	for _, c := range []struct{ retained, released int }{
		{0, 0}, {10, 0}, {10, 10}, {1000, 0}, {1000, 100}, {10000, 0}, {10000, 1000},
	} {
		b.Run(fmt.Sprintf("retained=%d/released=%d", c.retained, c.released), func(b *testing.B) {
			st := openBenchStore(b)
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				oh := mustBenchPosition(b, st, c.retained, c.released)
				b.StartTimer()
				if _, err := st.Delete(oh, 0); err != nil {
					b.Fatalf("delete: %v", err)
				}
			}
		})
	}
}

// Releases from many goroutines at once, as a publisher releasing superseded
// generations across many objects would.
func BenchmarkReleaseGenerationParallel(b *testing.B) {
	st := openBenchStore(b)
	oh := mustBenchPosition(b, st, b.N, 0)
	var next atomic.Uint64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := st.ReleaseGeneration(oh, 0, next.Add(1)); err != nil {
				b.Errorf("release: %v", err)
				return
			}
		}
	})
}

// Lookup latency while one goroutine deletes and another releases. Measures
// what the delete and release locks cost unrelated readers.
func BenchmarkLookupUnderDeleteAndRelease(b *testing.B) {
	st := openBenchStore(b)
	reader := mustBenchPosition(b, st, 0, 0)

	const pool = 50
	held := make([][32]byte, pool)
	for i := range held {
		held[i] = mustBenchPosition(b, st, 100, 0)
	}

	// One goroutine deletes fresh positions with 100 retained rows, 10 of them
	// released; another keeps releasing generations of positions it holds.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			oh, err := benchPosition(st, 100, 10)
			if err != nil {
				b.Errorf("fixture: %v", err)
				return
			}
			if _, err := st.Delete(oh, 0); err != nil {
				b.Errorf("delete: %v", err)
				return
			}
		}
	})
	wg.Go(func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := st.ReleaseGeneration(held[i%pool], 0, uint64(i/pool%100+1)); err != nil {
				b.Errorf("release: %v", err)
				return
			}
		}
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r, err := st.Lookup(reader, 0)
			if err != nil {
				b.Errorf("lookup: %v", err)
				return
			}
			_ = r.Close()
		}
	})
	b.StopTimer()
	close(stop)
	wg.Wait()
}
