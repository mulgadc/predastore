package engine

import (
	"encoding/binary"
	"fmt"
	"testing"
)

// One orphaned-release sweep over a release namespace holding the given
// orphans and in-use release rows. Fixtures are rebuilt outside the timer; past
// one window, each op reads the next window of in-use rows.
func BenchmarkSweepOrphanedReleases(b *testing.B) {
	for _, c := range []struct{ orphans, inUse int }{
		{0, 0}, {0, 1000}, {100, 1000}, {10000, 0}, {0, 100000},
	} {
		b.Run(fmt.Sprintf("orphans=%d/inuse=%d", c.orphans, c.inUse), func(b *testing.B) {
			st := openBenchStore(b)
			oh := mustBenchPosition(b, st, c.inUse, c.inUse)
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				wb := st.index.NewWriteBatch()
				for i := range c.orphans {
					var o [32]byte
					o[0] = 0xee
					binary.BigEndian.PutUint32(o[1:5], uint32(i))
					if err := wb.Set(releasedKeyFor(retainedKey(MakeKey(o, 0), 1)), encodeReleasedValue(1)); err != nil {
						b.Fatalf("stage orphan: %v", err)
					}
				}
				if err := wb.Flush(); err != nil {
					b.Fatalf("write orphans: %v", err)
				}
				b.StartTimer()
				n, err := st.sweepOrphanedReleases()
				if err != nil {
					b.Fatalf("sweep: %v", err)
				}
				if n != c.orphans {
					b.Fatalf("swept %d, want %d", n, c.orphans)
				}
			}
			_ = oh
		})
	}
}

func benchOrphanSweep(st *Store) { _, _ = st.sweepOrphanedReleases() }
