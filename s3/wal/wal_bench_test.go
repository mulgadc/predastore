package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// Benchmarks are designed to feel like `dd` throughput tests:
// - Each benchmark *iteration* writes/reads a fixed amount of logical data (bytes processed)
// - WAL setup/teardown is excluded from timing
// - We write enough per iteration to force multiple WAL files to be created
// - Read benchmark validates a tiny expected marker per record (first 16 bytes)
//
// Run:
//   go test ./s3/wal -bench BenchmarkWAL -benchmem
//
// Notes:
// - WAL currently uses O_SYNC; results will reflect sync-on-write durability.
// - On-disk bytes are larger than logical bytes due to 8KB chunk padding.

const (
	// For the read benchmark we pre-generate multiple records to:
	// - validate correctness against expected marker
	// - ensure multiple WAL files are created (especially for small record sizes)
	benchMaxRecords = 256

	// Keep shards small so rotation happens frequently during benchmarks.
	// (This includes WAL header + fragment headers + padded 8KB payloads.)
	benchShardSizeBytes = 1 << 20 // 1MiB
)

func BenchmarkWAL_Write(b *testing.B) {
	for _, sz := range benchSizes() {
		sz := sz
		b.Run(fmt.Sprintf("write/%s", humanBytes(sz)), func(b *testing.B) {
			b.ReportAllocs()

			// One record per operation (dd-like). Rotation happens naturally as b.N increases.
			b.SetBytes(int64(sz))

			// Isolate benchmark storage and WAL instance from timed section.
			walDir := mustMkdirTemp(b)
			b.Cleanup(func() { _ = os.RemoveAll(walDir) })
			stateFile := filepath.Join(walDir, "state.json")

			withBenchShardSize(func() {
				w, err := New(stateFile, walDir)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = w.Close() })

				// Deterministic payload; seed changes each op to avoid trivial cache effects.
				buf := make([]byte, sz)
				var r bytes.Reader

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					fillPayload(buf, uint64(i+1))
					r.Reset(buf)
					if _, err := w.Write(&r, sz); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkWAL_Read(b *testing.B) {
	for _, sz := range benchSizes() {
		sz := sz
		b.Run(fmt.Sprintf("read/%s", humanBytes(sz)), func(b *testing.B) {
			b.ReportAllocs()

			// One record read per operation (dd-like).
			b.SetBytes(int64(sz))

			walDir := mustMkdirTemp(b)
			b.Cleanup(func() { _ = os.RemoveAll(walDir) })
			stateFile := filepath.Join(walDir, "state.json")

			records := benchRecordsForSize(sz)
			results := make([]*WriteResult, records)
			seeds := make([]uint64, records)

			withBenchShardSize(func() {
				// Setup (not timed): write records once.
				w, err := New(stateFile, walDir)
				if err != nil {
					b.Fatal(err)
				}

				var r bytes.Reader
				for i := 0; i < records; i++ {
					seed := uint64(i + 1)
					buf := make([]byte, sz)
					fillPayload(buf, seed)
					seeds[i] = seed
					r.Reset(buf)
					wr, err := w.Write(&r, sz)
					if err != nil {
						_ = w.Close()
						b.Fatal(err)
					}
					results[i] = wr
				}
				_ = w.Close()

				// Reader instance.
				rw, err := New(stateFile, walDir)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = rw.Close() })

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					j := i % records
					data, err := rw.ReadFromWriteResult(results[j])
					if err != nil {
						b.Fatal(err)
					}
					if len(data) != sz {
						b.Fatalf("read size mismatch: got=%d want=%d", len(data), sz)
					}
					if !validateMarker(data, seeds[j]) {
						b.Fatalf("marker mismatch for record %d", j)
					}
				}
			})
		})
	}
}

func benchSizes() []int {
	// Full sweep by default. Use `-short` for a quick sanity run.
	if testing.Short() {
		return []int{
			4 << 10,
			64 << 10,
			1 << 20,
			8 << 20,
		}
	}

	return []int{
		1 << 10,
		2 << 10,
		4 << 10,
		8 << 10,
		16 << 10,
		32 << 10,
		64 << 10,
		128 << 10,
		256 << 10,
		512 << 10,
		1 << 20,
		2 << 20,
		4 << 20,
		8 << 20,
	}
}

func benchRecordsForSize(recordSize int) int {
	if recordSize <= 0 {
		return 2
	}
	// Cap to keep setup quick and memory bounded.
	records := (16 << 20) / recordSize // aim ~16MiB logical data in setup
	if records < 2 {
		records = 2
	}
	if records > benchMaxRecords {
		records = benchMaxRecords
	}
	return records
}

func humanBytes(n int) string {
	switch {
	case n%(1<<20) == 0:
		return fmt.Sprintf("%dMiB", n>>20)
	case n%(1<<10) == 0:
		return fmt.Sprintf("%dKiB", n>>10)
	default:
		return fmt.Sprintf("%dB", n)
	}
}

// fillPayload writes deterministic content into buf.
// The first 16 bytes are a marker: [seed][^seed] (big endian).
func fillPayload(buf []byte, seed uint64) {
	if len(buf) == 0 {
		return
	}
	if len(buf) >= 8 {
		binary.BigEndian.PutUint64(buf[0:8], seed)
	}
	if len(buf) >= 16 {
		binary.BigEndian.PutUint64(buf[8:16], ^seed)
	}
	// Fill remaining bytes with a cheap xorshift64* PRNG.
	x := seed
	for i := 16; i < len(buf); i++ {
		x ^= x >> 12
		x ^= x << 25
		x ^= x >> 27
		v := byte((x * 2685821657736338717) >> 56)
		buf[i] = v
	}
}

func validateMarker(buf []byte, seed uint64) bool {
	if len(buf) < 16 {
		return false
	}
	gotSeed := binary.BigEndian.Uint64(buf[0:8])
	gotInv := binary.BigEndian.Uint64(buf[8:16])
	return gotSeed == seed && gotInv == ^seed
}

func mustMkdirTemp(b *testing.B) string {
	b.Helper()
	// include timestamp to reduce any chance of reuse/collision
	dir, err := os.MkdirTemp("", fmt.Sprintf("wal-bench-%d-", time.Now().UnixNano()))
	if err != nil {
		b.Fatal(err)
	}
	return dir
}

var benchShardMu sync.Mutex

func withBenchShardSize(fn func()) {
	benchShardMu.Lock()
	defer benchShardMu.Unlock()

	orig := ShardSize
	ShardSize = benchShardSizeBytes
	defer func() { ShardSize = orig }()

	fn()
}

