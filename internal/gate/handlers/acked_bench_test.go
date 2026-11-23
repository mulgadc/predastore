package handlers

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"testing"
)

// quietLogs discards logging for one benchmark. The blob stores log as they
// open, and a line landing between a benchmark's name and its result is a
// result benchstat cannot parse.
func quietLogs(b *testing.B) {
	b.Helper()
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	b.Cleanup(func() { slog.SetDefault(prev) })
}

// benchSizes span a small object, the common viperblock chunk, and the largest
// the in-process fixtures carry.
var benchSizes = []struct {
	name string
	size int
}{
	{"4KiB", 4 << 10},
	{"64KiB", 64 << 10},
	{"1MiB", 1 << 20},
	{"4MiB", 4 << 20},
	{"32MiB", 32 << 20},
}

// BenchmarkPutObjectRealStore is the whole PutObject handler over real blob
// stores: the stream to every holder, the commits and the floor they are held
// to, then the placement swap. Each iteration writes a fresh key, as viperblock
// does with a chunk, so no write overwrites another.
func BenchmarkPutObjectRealStore(b *testing.B) {
	for _, p := range rsProfiles {
		for _, sz := range benchSizes {
			b.Run(p.String()+"/"+sz.name, func(b *testing.B) {
				quietLogs(b)
				g := newRealStoreGate(b, p)
				body := bytes.Repeat([]byte("x"), sz.size)
				b.SetBytes(int64(sz.size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					if rr := g.put(fmt.Sprintf("k%09d", i), body); rr.Code != http.StatusOK {
						b.Fatalf("put: %d %s", rr.Code, rr.Body.String())
					}
				}
			})
		}
	}
}

// BenchmarkPutObjectRealStoreDegraded is the same write with one shard lost
// after sending, so every iteration runs at the floor rather than full width.
func BenchmarkPutObjectRealStoreDegraded(b *testing.B) {
	for _, p := range rsProfiles {
		for _, sz := range benchSizes[2:4] {
			b.Run(p.String()+"/"+sz.name, func(b *testing.B) {
				quietLogs(b)
				g := newRealStoreGate(b, p)
				g.bc.arm(shardFaults{failPut: []uint32{1}})
				body := bytes.Repeat([]byte("x"), sz.size)
				b.SetBytes(int64(sz.size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					if rr := g.put(fmt.Sprintf("k%09d", i), body); rr.Code != http.StatusOK {
						b.Fatalf("put: %d %s", rr.Code, rr.Body.String())
					}
				}
			})
		}
	}
}

// BenchmarkCopyObjectRealStore copies one source to a fresh destination each
// iteration: the read of every source shard, then the write and its floor.
func BenchmarkCopyObjectRealStore(b *testing.B) {
	for _, p := range rsProfiles {
		for _, sz := range benchSizes[2:4] {
			b.Run(p.String()+"/"+sz.name, func(b *testing.B) {
				quietLogs(b)
				g := newRealStoreGate(b, p)
				if rr := g.put("src", bytes.Repeat([]byte("s"), sz.size)); rr.Code != http.StatusOK {
					b.Fatalf("seed: %d %s", rr.Code, rr.Body.String())
				}
				b.SetBytes(int64(sz.size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					if rr := g.copy("src", fmt.Sprintf("d%09d", i)); !succeeded(rr) {
						b.Fatalf("copy: %d %s", rr.Code, rr.Body.String())
					}
				}
			})
		}
	}
}

// BenchmarkUploadPartRealStore uploads a fresh part number each iteration,
// which is the path whose commit moved ahead of its parts row.
func BenchmarkUploadPartRealStore(b *testing.B) {
	for _, p := range rsProfiles {
		for _, sz := range benchSizes[2:4] {
			b.Run(p.String()+"/"+sz.name, func(b *testing.B) {
				quietLogs(b)
				g := newRealStoreGate(b, p)
				upload := g.createUpload(b, "mp")
				body := bytes.Repeat([]byte("p"), sz.size)
				b.SetBytes(int64(sz.size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					if rr := g.uploadPart("mp", upload, i%10000+1, body); !succeeded(rr) {
						b.Fatalf("upload part: %d %s", rr.Code, rr.Body.String())
					}
				}
			})
		}
	}
}
