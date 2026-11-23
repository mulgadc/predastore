package meta

import (
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

// BenchmarkLogStoreAppendDuringTrim measures what a leader's appends wait
// while a snapshot trims the log ahead of them: one op is one whole trim,
// reported as the p99 and worst single-entry append made during it.
func BenchmarkLogStoreAppendDuringTrim(b *testing.B) {
	entries := uint64(2_000_000)
	if v, err := strconv.ParseUint(os.Getenv("PREDASTORE_BENCH_TRIM_ENTRIES"), 10, 64); err == nil && v > 0 {
		entries = v
	}
	var waits []time.Duration
	for b.Loop() {
		b.StopTimer()
		store, err := openLogStore(filepath.Join(b.TempDir(), "raft.db"))
		require.NoError(b, err)
		fillLog(b, store, 1, entries, 1<<10)
		b.StartTimer()

		var stop atomic.Bool
		appended := make(chan []time.Duration, 1)
		go func() {
			var w []time.Duration
			for next := entries + 1; !stop.Load(); next++ {
				start := time.Now()
				if err := store.StoreLogs([]*raft.Log{{Index: next, Term: 1, Type: raft.LogCommand, Data: []byte("v")}}); err != nil {
					break
				}
				w = append(w, time.Since(start))
			}
			appended <- w
		}()
		require.NoError(b, store.DeleteRange(1, entries-1))
		stop.Store(true)
		waits = append(waits, <-appended...)

		b.StopTimer()
		require.NoError(b, store.Close())
		b.StartTimer()
	}
	slices.Sort(waits)
	if len(waits) > 0 {
		b.ReportMetric(float64(waits[len(waits)*99/100].Microseconds()), "p99-append-µs")
		b.ReportMetric(float64(waits[len(waits)-1].Microseconds()), "worst-append-µs")
	}
}
