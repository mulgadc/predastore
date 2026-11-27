package handlers

import (
	"bytes"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
)

// condBenchSizes are the sizes the conditional write is compared at: a small
// object, the common chunk, and a body large enough that streaming dominates.
var condBenchSizes = []struct {
	name string
	size int
}{
	{"1KiB", 1 << 10},
	{"1MiB", 1 << 20},
	{"64MiB", 64 << 20},
}

// condBenchModes are the writes compared: the unconditional path, a create
// of a fresh key, and an overwrite of the writer's own key at its ETag.
var condBenchModes = []string{"unconditional", "if-none-match", "if-match"}

// BenchmarkPutObjectConditional is the whole PutObject handler over real blob
// stores, unconditional against conditional, from one writer and from many.
// The conditional cost is the held open, the intent and the transaction.
func BenchmarkPutObjectConditional(b *testing.B) {
	for _, sz := range condBenchSizes {
		for _, mode := range condBenchModes {
			b.Run(sz.name+"/"+mode+"/single", func(b *testing.B) {
				quietLogs(b)
				g := newRealStoreGate(b, rsProfiles[0])
				w, err := newBenchWriter(g, mode, sz.size, "w0")
				if err != nil {
					b.Fatal(err)
				}
				b.SetBytes(int64(sz.size))
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if err := w.write(); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run(sz.name+"/"+mode+"/concurrent", func(b *testing.B) {
				quietLogs(b)
				g := newRealStoreGate(b, rsProfiles[0])
				var writers atomic.Int64
				b.SetBytes(int64(sz.size))
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					w, err := newBenchWriter(g, mode, sz.size, fmt.Sprintf("w%d", writers.Add(1)))
					if err != nil {
						b.Error(err)
						return
					}
					for pb.Next() {
						if err := w.write(); err != nil {
							b.Error(err)
							return
						}
					}
				})
			})
		}
	}
}

// benchWriter writes one mode repeatedly. Creates use a fresh key each time;
// an If-Match writer overwrites its own key, so writers never conflict.
type benchWriter struct {
	g      *realStoreGate
	mode   string
	prefix string
	body   []byte
	etag   string
	n      int
}

func newBenchWriter(g *realStoreGate, mode string, size int, prefix string) (*benchWriter, error) {
	w := &benchWriter{g: g, mode: mode, prefix: prefix, body: bytes.Repeat([]byte("x"), size)}
	if mode == "if-match" {
		rr := g.put(prefix, w.body)
		if rr.Code != http.StatusOK {
			return nil, fmt.Errorf("seed: %d %s", rr.Code, rr.Body.String())
		}
		w.etag = rr.Header().Get("ETag")
	}
	return w, nil
}

func (w *benchWriter) write() error {
	w.n++
	key, header := fmt.Sprintf("%s-%09d", w.prefix, w.n), http.Header(nil)
	switch w.mode {
	case "if-none-match":
		header = http.Header{"If-None-Match": {"*"}}
	case "if-match":
		key, header = w.prefix, http.Header{"If-Match": {w.etag}}
	}
	rr := w.g.serve(PutObject(w.g.mc, w.g.bc, w.g.ring, w.g.cache, w.g.cfg), http.MethodPut, "/bkt/"+key, key, w.body, header)
	if rr.Code != http.StatusOK {
		return fmt.Errorf("%s put: %d %s", w.mode, rr.Code, rr.Body.String())
	}
	return nil
}
