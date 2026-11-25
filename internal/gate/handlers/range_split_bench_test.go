package handlers

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// BenchmarkGetObjectRange measures ranged GETs through the handler on a
// viperblock-sized chunk under RS(2,1): one inside a block, which must not
// regress, and ranges across the shard boundary, which used to read the
// whole object.
func BenchmarkGetObjectRange(b *testing.B) {
	const (
		size  = 10 + 1024*4096
		block = 4096
	)
	boundary := int64((size + 1) / 2)
	cases := []struct {
		name       string
		start, end int64
	}{
		{name: "inside/4KiB", start: 10 + 100*block, end: 10 + 101*block - 1},
		{name: "crossing/4KiB", start: 10 + 511*block, end: 10 + 512*block - 1},
		{name: "crossing/2MiB", start: boundary - 1<<20, end: boundary + 1<<20 - 1},
	}

	f := newWriteFixture(2, 1)
	publishedObject(b, f, size)
	handler := GetObject(f.mc, f.bc, f.ring, testCache(), f.cfg)

	// get reports a wrong answer rather than failing, because the parallel
	// case calls it off the benchmark's goroutine.
	get := func(b *testing.B, start, end int64) {
		req := objectRequest(http.MethodGet, "k", "")
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != http.StatusPartialContent || int64(w.Body.Len()) != end-start+1 {
			b.Errorf("range %d-%d: status %d, %d bytes", start, end, w.Code, w.Body.Len())
		}
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.SetBytes(c.end - c.start + 1)
			b.ReportAllocs()
			for range b.N {
				get(b, c.start, c.end)
			}
		})
	}

	// Concurrent readers of the crossing block, the load under which each one
	// that reads the whole object costs the most.
	b.Run("crossing/4KiB/parallel", func(b *testing.B) {
		c := cases[1]
		b.SetBytes(c.end - c.start + 1)
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				get(b, c.start, c.end)
			}
		})
	})
}
