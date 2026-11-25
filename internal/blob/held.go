package blob

import "sync"

// heldKey names one held shard write: a position and the epoch writing it.
type heldKey struct {
	key   [32]byte
	index uint32
	epoch uint64
}

// heldWriters tracks the held writes that have not prepared their row yet.
// An abort that reaches one marks it and cuts off its body, since there is no
// row to drop, and the writer then prepares nothing.
type heldWriters struct {
	mu      sync.Mutex
	open    map[heldKey]func()
	aborted map[heldKey]bool
}

func newHeldWriters() *heldWriters {
	return &heldWriters{open: map[heldKey]func(){}, aborted: map[heldKey]bool{}}
}

// register records a held writer as open, with the action that ends its body
// read. It reports false when one is already open for the same epoch.
func (h *heldWriters) register(k heldKey, cut func()) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.open[k]; ok {
		return false
	}
	h.open[k] = cut
	return true
}

// finish removes an open writer and reports whether an abort marked it.
// The caller drops the writer's prepared row when it was marked.
func (h *heldWriters) finish(k heldKey) (aborted bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	aborted = h.aborted[k]
	delete(h.open, k)
	delete(h.aborted, k)
	return aborted
}

// isAborted reports whether an abort has marked the open writer.
func (h *heldWriters) isAborted(k heldKey) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.aborted[k]
}

// markAborted marks an open writer, cuts off its body, and reports whether
// one was open. When it reports false, no held writer for the epoch can
// still prepare a row.
func (h *heldWriters) markAborted(k heldKey) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	cut, ok := h.open[k]
	if !ok {
		return false
	}
	if !h.aborted[k] {
		h.aborted[k] = true
		cut()
	}
	return true
}
