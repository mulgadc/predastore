package handlers

import (
	"bytes"
	"context"
	"io"
	"runtime"
	"sync"
	"testing"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/require"
)

// BenchmarkGetReadPath measures a whole-object GET the way serveObject runs
// one: open the stripe reader, then stream to the writer. The destination is
// io.Discard rather than a buffer, because a buffer the size of the object
// would be the allocation this measures.
//
// The healthy case is the one to watch. It opens k shards, so the parity
// buffers a reader used to take up front are pure waste in it.
func BenchmarkGetReadPath(b *testing.B) {
	sizes := []struct {
		name string
		size int64
	}{
		{name: "64KiB", size: 64 << 10},
		{name: "1MiB", size: 1 << 20},
		{name: "8MiB", size: 8 << 20},
		{name: "32MiB", size: 32 << 20},
	}

	for _, sz := range sizes {
		b.Run(sz.name, func(b *testing.B) {
			f := newWriteFixture(2, 1)
			ctx := context.Background()
			objectHash := model.ObjectHash("bucket", "object.txt")

			body := bytes.Repeat([]byte("x"), int(sz.size))
			place, _, err := f.write(ctx, objectHash, bytes.NewReader(body), sz.size)
			require.NoError(b, err)

			b.SetBytes(sz.size)
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				r, err := newStripeReader(ctx, f.bc, f.cfg, objectHash, place, 0)
				require.NoError(b, err)
				require.NoError(b, pipeObject(ctx, r, io.Discard, sz.size))
				r.close(ctx)
			}
		})
	}
}

// BenchmarkConcurrentSmallGets is the measurement bytes/op cannot make. A
// pooled block is capacity per shard per request, held for the life of the
// request, so what it costs is decided by how many requests are open at once
// and not by how many ran. peak_MiB is the heap with every read open; idle_MiB
// is what the pool is still holding after they finish and the collector runs.
func BenchmarkConcurrentSmallGets(b *testing.B) {
	sizes := []struct {
		name string
		size int64
	}{
		{name: "64KiB", size: 64 << 10},
		{name: "1MiB", size: 1 << 20},
	}
	const concurrency = 100

	for _, sz := range sizes {
		b.Run(sz.name, func(b *testing.B) {
			f := newWriteFixture(2, 1)
			ctx := context.Background()
			objectHash := model.ObjectHash("bucket", "object.txt")

			body := bytes.Repeat([]byte("x"), int(sz.size))
			place, _, err := f.write(ctx, objectHash, bytes.NewReader(body), sz.size)
			require.NoError(b, err)

			var peak, idle uint64
			b.SetBytes(sz.size * concurrency)
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				open := make([]*stripeReader, concurrency)
				for i := range open {
					r, rErr := newStripeReader(ctx, f.bc, f.cfg, objectHash, place, 0)
					require.NoError(b, rErr)
					open[i] = r
				}
				peak = max(peak, heapInUse())

				var wg sync.WaitGroup
				for _, r := range open {
					wg.Go(func() {
						if pErr := pipeObject(ctx, r, io.Discard, sz.size); pErr != nil {
							b.Error(pErr)
						}
						r.close(ctx)
					})
				}
				wg.Wait()
				idle = max(idle, heapInUse())
			}

			b.StopTimer()
			b.ReportMetric(float64(peak)/(1<<20), "peak_MiB")
			b.ReportMetric(float64(idle)/(1<<20), "idle_MiB")
		})
	}
}

// heapInUse settles the collector first, so the figure is what is retained
// rather than what has not been swept yet.
func heapInUse() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return m.HeapInuse
}
