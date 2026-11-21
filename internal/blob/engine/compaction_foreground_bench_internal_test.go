package engine

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"io"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
)

// Foreground latency while a compaction pass runs, against the same store
// idle. fsync cost is what this measures, so point PREDASTORE_BENCH_DIR at a
// real disk: on tmpfs both cases sync for free and show no difference.
// PREDASTORE_BENCH_VALUE sets the foreground value size, default 64 KiB.
const (
	fgBenchSegSize   = 32 << 20
	fgBenchFillValue = 256 << 10
	fgBenchFillBytes = 192 << 20
	fgBenchOps       = 150
)

func fgBenchValue() int {
	if v, err := strconv.Atoi(os.Getenv("PREDASTORE_BENCH_VALUE")); err == nil && v > 0 {
		return v
	}
	return 64 << 10
}

func fgBenchDir(b *testing.B) string {
	if d := os.Getenv("PREDASTORE_BENCH_DIR"); d != "" {
		b.Setenv("TMPDIR", d)
	}
	return b.TempDir()
}

func fgBenchKey(prefix byte, i int) [32]byte {
	var k [32]byte
	k[0] = prefix
	binary.BigEndian.PutUint64(k[1:9], uint64(i))
	return k
}

func fgBenchPut(b *testing.B, st *Store, key [32]byte, body []byte) {
	epoch := nextTestEpoch()
	w, err := st.Append(key, 0, int64(len(body)), epoch)
	if err != nil {
		b.Fatalf("append: %v", err)
	}
	if _, err := w.Write(body); err != nil {
		b.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		b.Fatalf("close writer: %v", err)
	}
	if _, err := st.Commit(key, 0, epoch); err != nil {
		b.Fatalf("commit: %v", err)
	}
}

func fgBenchGet(b *testing.B, st *Store, key [32]byte) {
	r, err := st.Lookup(key, 0)
	if err != nil {
		b.Fatalf("lookup: %v", err)
	}
	defer r.Close()
	if _, err := io.Copy(io.Discard, r); err != nil {
		b.Fatalf("read: %v", err)
	}
}

// fgBenchStore fills several sealed segments and deletes two thirds of their
// values, so every sealed segment is a compaction candidate.
func fgBenchStore(b *testing.B) *Store {
	aead, err := masterkey.NewAEAD(bytes.Repeat([]byte{0x42}, 32))
	if err != nil {
		b.Fatalf("aead: %v", err)
	}
	opts := append([]Option{WithFreeSpaceWatermark(0, 0), WithMaxSegSize(fgBenchSegSize), WithAEAD(aead)}, fgBenchOptions()...)
	st, err := Open(fgBenchDir(b), opts...)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	b.Cleanup(func() { _ = st.Close() })
	body := make([]byte, fgBenchFillValue)
	_, _ = rand.Read(body)
	n := fgBenchFillBytes / fgBenchFillValue
	for i := range n {
		fgBenchPut(b, st, fgBenchKey(0x01, i), body)
	}
	for i := range n {
		if i%3 != 0 {
			if _, err := st.Delete(fgBenchKey(0x01, i), 0); err != nil {
				b.Fatalf("delete: %v", err)
			}
		}
	}
	return st
}

func passFinished(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}

func percentile(d []time.Duration, p float64) time.Duration {
	s := slices.Clone(d)
	slices.Sort(s)
	return s[min(len(s)-1, int(float64(len(s))*p))]
}

// BenchmarkForegroundDuringCompaction reports foreground PUT and GET p50 and
// p99, in ms, with no compaction and while one pass relocates every candidate:
// sampling starts at the first relocation and runs until the pass ends.
func BenchmarkForegroundDuringCompaction(b *testing.B) {
	// The store logs every pass; benchmark output must parse.
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.DiscardHandler))
	b.Cleanup(func() { slog.SetDefault(prev) })
	for _, compacting := range []bool{false, true} {
		name := "idle"
		if compacting {
			name = "compacting"
		}
		b.Run(name, func(b *testing.B) {
			var puts, gets, passes []time.Duration
			body := make([]byte, fgBenchValue())
			_, _ = rand.Read(body)
			for b.Loop() {
				b.StopTimer()
				st := fgBenchStore(b)
				var wg sync.WaitGroup
				passDone := make(chan struct{})
				started := make(chan struct{})
				if compacting {
					var once sync.Once
					st.beforeRelocateSwap = func() { once.Do(func() { close(started) }) }
					wg.Go(func() {
						defer once.Do(func() { close(started) })
						defer close(passDone)
						t0 := time.Now()
						if err := st.compactOnce(); err != nil {
							b.Errorf("compact: %v", err)
						}
						passes = append(passes, time.Since(t0))
					})
				}
				if compacting {
					<-started
				}
				b.StartTimer()
				for i := 0; compacting || i < fgBenchOps; i++ {
					if compacting && passFinished(passDone) {
						break
					}
					key := fgBenchKey(0x02, i)
					t0 := time.Now()
					fgBenchPut(b, st, key, body)
					puts = append(puts, time.Since(t0))
					t0 = time.Now()
					fgBenchGet(b, st, key)
					gets = append(gets, time.Since(t0))
				}
				b.StopTimer()
				wg.Wait()
				b.StartTimer()
			}
			if len(puts) == 0 {
				b.Fatal("no foreground operations completed")
			}
			ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
			b.ReportMetric(ms(percentile(puts, 0.50)), "put-p50-ms")
			b.ReportMetric(ms(percentile(puts, 0.99)), "put-p99-ms")
			b.ReportMetric(ms(percentile(gets, 0.50)), "get-p50-ms")
			b.ReportMetric(ms(percentile(gets, 0.99)), "get-p99-ms")
			b.ReportMetric(float64(len(puts)), "ops")
			if len(passes) > 0 {
				b.ReportMetric(ms(percentile(passes, 0.50)), "pass-ms")
			}
		})
	}
}
