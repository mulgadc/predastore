package engine

import (
	"bytes"
	"crypto/rand"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
)

// dropBenchSegBytes is the size of the sealed segment the pass drops, from
// PREDASTORE_BENCH_DROP_BYTES, default 1 GiB. PREDASTORE_BENCH_DIR places the
// store; on tmpfs a file is freed for nothing, which hides the cost.
func dropBenchSegBytes() int64 {
	if v, err := strconv.ParseInt(os.Getenv("PREDASTORE_BENCH_DROP_BYTES"), 10, 64); err == nil && v > 0 {
		return v
	}
	return 1 << 30
}

// dropBenchStore seals one segment of about segBytes whose every value is
// deleted, so a pass has nothing to relocate and its time is the drop.
func dropBenchStore(b *testing.B, segBytes int64) *Store {
	aead, err := masterkey.NewAEAD(bytes.Repeat([]byte{0x42}, 32))
	if err != nil {
		b.Fatalf("aead: %v", err)
	}
	st, err := Open(fgBenchDir(b), WithFreeSpaceWatermark(0, 0), WithMaxSegSize(uint64(segBytes)), WithAEAD(aead))
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	b.Cleanup(func() { _ = st.Close() })
	body := make([]byte, 1<<20)
	_, _ = rand.Read(body)
	n := int(segBytes/int64(len(body))) + 1
	for i := range n {
		fgBenchPut(b, st, fgBenchKey(0x01, i), body)
	}
	for i := range n {
		if _, err := st.Delete(fgBenchKey(0x01, i), 0); err != nil {
			b.Fatalf("delete: %v", err)
		}
	}
	return st
}

// fgOp is one foreground operation's interval.
type fgOp struct{ start, end time.Time }

func (o fgOp) d() time.Duration { return o.end.Sub(o.start) }

// overlapping returns the durations of the ops whose interval overlaps [from, to].
func overlapping(ops []fgOp, from, to time.Time) []time.Duration {
	var out []time.Duration
	for _, o := range ops {
		if o.start.Before(to) && o.end.After(from) {
			out = append(out, o.d())
		}
	}
	return out
}

// BenchmarkForegroundDuringSegmentDrop reports foreground PUT and GET p99 and
// worst case, in ms, while a pass drops one large dead segment, both over the
// pass and over the ops whose interval overlaps the segment's unlink.
func BenchmarkForegroundDuringSegmentDrop(b *testing.B) {
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.DiscardHandler))
	b.Cleanup(func() { slog.SetDefault(prev) })
	segBytes := dropBenchSegBytes()
	b.Run("seg="+strconv.FormatInt(segBytes>>20, 10)+"MiB", func(b *testing.B) {
		var puts, gets, putsIn, getsIn, passes, unlinks []time.Duration
		body := make([]byte, fgBenchValue())
		_, _ = rand.Read(body)
		prevRemove := removeFile
		b.Cleanup(func() { removeFile = prevRemove })
		for b.Loop() {
			b.StopTimer()
			st := dropBenchStore(b, segBytes)
			// Warm the foreground key's segment so the first sample is not an open.
			fgBenchPut(b, st, fgBenchKey(0x03, 0), body)
			var unlinkFrom, unlinkTo atomic.Int64
			removeFile = func(path string) error {
				if filepath.Ext(path) != ".seg" {
					return prevRemove(path)
				}
				unlinkFrom.Store(time.Now().UnixNano())
				err := prevRemove(path)
				unlinkTo.Store(time.Now().UnixNano())
				return err
			}
			passDone := make(chan struct{})
			var wg sync.WaitGroup
			wg.Go(func() {
				defer close(passDone)
				t0 := time.Now()
				if err := st.compactOnce(); err != nil {
					b.Errorf("compact: %v", err)
				}
				passes = append(passes, time.Since(t0))
			})
			var putOps, getOps []fgOp
			b.StartTimer()
			for i := 0; i == 0 || !passFinished(passDone); i++ {
				key := fgBenchKey(0x02, i)
				t0 := time.Now()
				fgBenchPut(b, st, key, body)
				t1 := time.Now()
				fgBenchGet(b, st, key)
				t2 := time.Now()
				putOps = append(putOps, fgOp{t0, t1})
				getOps = append(getOps, fgOp{t1, t2})
			}
			b.StopTimer()
			wg.Wait()
			removeFile = prevRemove
			if unlinkTo.Load() == 0 {
				b.Fatal("the pass never unlinked a segment")
			}
			if st.segmentFileExists(0) {
				b.Fatal("the pass left the dead segment on disk")
			}
			from, to := time.Unix(0, unlinkFrom.Load()), time.Unix(0, unlinkTo.Load())
			unlinks = append(unlinks, to.Sub(from))
			inPut, inGet := overlapping(putOps, from, to), overlapping(getOps, from, to)
			if len(inPut)+len(inGet) == 0 {
				b.Fatal("no foreground operation overlapped the segment unlink")
			}
			putsIn = append(putsIn, inPut...)
			getsIn = append(getsIn, inGet...)
			for _, o := range putOps {
				puts = append(puts, o.d())
			}
			for _, o := range getOps {
				gets = append(gets, o.d())
			}
			b.StartTimer()
		}
		ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
		b.ReportMetric(ms(percentile(puts, 0.99)), "put-p99-ms")
		b.ReportMetric(ms(percentile(puts, 1)), "put-max-ms")
		b.ReportMetric(ms(percentile(gets, 0.99)), "get-p99-ms")
		b.ReportMetric(ms(percentile(gets, 1)), "get-max-ms")
		if len(putsIn) > 0 {
			b.ReportMetric(ms(percentile(putsIn, 0.99)), "unlink-put-p99-ms")
			b.ReportMetric(ms(percentile(putsIn, 1)), "unlink-put-max-ms")
		}
		b.ReportMetric(float64(len(putsIn)), "unlink-puts")
		if len(getsIn) > 0 {
			b.ReportMetric(ms(percentile(getsIn, 0.99)), "unlink-get-p99-ms")
			b.ReportMetric(ms(percentile(getsIn, 1)), "unlink-get-max-ms")
		}
		b.ReportMetric(float64(len(getsIn)), "unlink-gets")
		b.ReportMetric(ms(percentile(unlinks, 0.50)), "unlink-ms")
		b.ReportMetric(float64(len(puts)), "ops")
		b.ReportMetric(ms(percentile(passes, 0.50)), "pass-ms")
	})
}
