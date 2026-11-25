package blob

import "sync"

// heldKey names one held shard write: a position and the epoch writing it.
type heldKey struct {
	key   [32]byte
	index uint32
	epoch uint64
}

// heldWriters tracks the held writes that have not prepared their row yet.
// An abort that reaches one marks it, since there is no row to drop, and the
// writer discards its extent when it closes.
type heldWriters struct {
	mu      sync.Mutex
	open    map[heldKey]bool
	aborted map[heldKey]bool
}

func newHeldWriters() *heldWriters {
	return &heldWriters{open: map[heldKey]bool{}, aborted: map[heldKey]bool{}}
}

// register records a held writer as open. It reports false when one is
// already open for the same epoch, which is a second put of the same shard.
func (h *heldWriters) register(k heldKey) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.open[k] {
		return false
	}
	h.open[k] = true
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

// markAborted marks an open writer and reports whether one was open. When it
// reports false, no held writer for the epoch can still prepare a row.
func (h *heldWriters) markAborted(k heldKey) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.open[k] {
		return false
	}
	h.aborted[k] = true
	return true
}
