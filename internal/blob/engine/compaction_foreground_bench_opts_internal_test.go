package engine

import (
	"os"
	"strconv"
)

// fgBenchOptions applies PREDASTORE_BENCH_RATE, the compaction byte cap in
// bytes per second, when set.
func fgBenchOptions() []Option {
	if v, err := strconv.ParseInt(os.Getenv("PREDASTORE_BENCH_RATE"), 10, 64); err == nil && v > 0 {
		return []Option{WithCompactionPacing(v, 0, 0)}
	}
	return nil
}
