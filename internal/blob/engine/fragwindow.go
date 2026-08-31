package engine

import "sync"

// fragWindowSize is the full fragment batch a reader opens or a writer seals in
// one syscall. Every value long enough to fill it asks for exactly this much,
// and a shard of a striped object always is, so it is the size worth pooling.
const fragWindowSize = bufLen * totalFragSize

// fragWindowPool holds those windows. One is taken per blob request, which made
// it the second largest allocation in the system behind the gate's shard
// blocks. A *[]byte is pooled rather than a []byte because Put takes an any,
// and boxing a slice header allocates the thing the pool exists to avoid.
var fragWindowPool = sync.Pool{
	New: func() any {
		b := make([]byte, fragWindowSize)

		return &b
	},
}

// takeFragWindow returns a window for a value of frags fragments, and the
// pooled buffer behind it if it came from the pool.
//
// A value shorter than the window gets its own exact buffer. Handing it a
// pooled one would pin the full window for a request that needs a fraction of
// it, and short values are the ones most likely to arrive in bulk.
func takeFragWindow(frags int64) ([]byte, *[]byte) {
	if frags < bufLen {
		return make([]byte, max(frags, 0)*totalFragSize), nil
	}

	held, _ := fragWindowPool.Get().(*[]byte)

	return *held, held
}

// dropFragWindow returns a pooled window. It is safe on the nil a caller gets
// back for a window that was never pooled.
func dropFragWindow(held *[]byte) {
	if held != nil {
		fragWindowPool.Put(held)
	}
}
