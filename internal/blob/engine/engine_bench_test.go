package engine_test

import (
	"io"
	"testing"

	"github.com/mulgadc/predastore/internal/blob/engine"
	"github.com/mulgadc/predastore/internal/storetest"
)

// BenchmarkShardRoundTrip measures what one blob request costs the engine:
// Append and stream a shard in, then Lookup and stream it back out. Both sides
// take a fragment window, which was the second largest allocation in the
// cluster after the gate's shard blocks.
//
// The sizes bracket a real shard. At RS(2,1) a 4 MiB object has a 2 MiB shard,
// and anything from a few hundred KiB up fills the window.
func BenchmarkShardRoundTrip(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{name: "8KiB", size: 8 << 10},
		{name: "256KiB", size: 256 << 10},
		{name: "2MiB", size: 2 << 20},
		{name: "4MiB", size: 4 << 20},
	}

	for _, sz := range sizes {
		b.Run(sz.name, func(b *testing.B) {
			st, err := engine.Open(b.TempDir(), engine.WithAEAD(storetest.TestAEAD()))
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			b.Cleanup(func() { _ = st.Close() })

			body := make([]byte, sz.size)
			oh := [32]byte{1}

			b.SetBytes(int64(sz.size))
			b.ReportAllocs()
			b.ResetTimer()

			for i := range b.N {
				idx := uint32(i % 64)
				epoch := storetest.NextEpoch()

				w, wErr := st.Append(oh, idx, int64(len(body)), epoch)
				if wErr != nil {
					b.Fatalf("append: %v", wErr)
				}
				if _, wErr = w.Write(body); wErr != nil {
					b.Fatalf("write: %v", wErr)
				}
				if wErr = w.Close(); wErr != nil {
					b.Fatalf("close writer: %v", wErr)
				}
				if _, wErr = st.Commit(oh, idx, epoch); wErr != nil {
					b.Fatalf("commit: %v", wErr)
				}

				r, rErr := st.Lookup(oh, idx)
				if rErr != nil {
					b.Fatalf("lookup: %v", rErr)
				}
				if _, rErr = io.Copy(io.Discard, io.NewSectionReader(r, 0, r.Size())); rErr != nil {
					b.Fatalf("read: %v", rErr)
				}
				if rErr = r.Close(); rErr != nil {
					b.Fatalf("close reader: %v", rErr)
				}
			}
		})
	}
}
