package blob

import "sync"

// heldKey names one held shard write: a position and the epoch writing it.
type heldKey struct {
	key   [32]byte
	index uint32
	epoch uint64
}

// heldWriter is one open held write. Before it closes, an abort marks it and
// cuts off its body, so it prepares nothing. Once it is closing, an abort
// waits for done and drops the row itself.
type heldWriter struct {
	cut     func()
	aborted bool
	closing bool
	done    chan struct{}
}

// heldWriters tracks the held writes that have not finished. The abort
// decision and the prepare never overlap: a writer either sees the abort
// before it closes, or the abort sees it closing and waits.
type heldWriters struct {
	mu   sync.Mutex
	open map[heldKey]*heldWriter
}

func newHeldWriters() *heldWriters {
	return &heldWriters{open: map[heldKey]*heldWriter{}}
}

// register records a held writer as open, with the action that ends its body
// read. It reports false when one is already open for the same epoch.
func (h *heldWriters) register(k heldKey, cut func()) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.open[k]; ok {
		return false
	}
	h.open[k] = &heldWriter{cut: cut, done: make(chan struct{})}
	return true
}

// beginClose reports whether an abort has marked the writer. If not, the
// writer is closing from here on, and a later abort waits for finish.
func (h *heldWriters) beginClose(k heldKey) (aborted bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	w := h.open[k]
	if w.aborted {
		return true
	}
	w.closing = true
	return false
}

// finish removes the writer and releases any abort waiting on its close.
func (h *heldWriters) finish(k heldKey) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if w, ok := h.open[k]; ok {
		delete(h.open, k)
		close(w.done)
	}
}

// markAborted handles an abort for an open writer. A writer not yet closing
// is marked and cut off, and open is true. A closing writer returns its done
// channel, and the caller drops the row once it is closed.
func (h *heldWriters) markAborted(k heldKey) (open bool, closing <-chan struct{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	w, ok := h.open[k]
	if !ok {
		return false, nil
	}
	if w.closing {
		return true, w.done
	}
	if !w.aborted {
		w.aborted = true
		w.cut()
	}
	return true, nil
}
