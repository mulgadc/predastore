package store

import (
	"fmt"
	"log/slog"
)

// slotExtent represents a contiguous range of reserved slots within a single segment.
type slotExtent struct {
	seg    *segment
	offset int
	size   int
}

// Close decrements the segment reference count and closes the segment file when it reaches zero.
func (ext *slotExtent) Close() error {
	if ext.seg.refs.Add(-1) == 0 {
		return ext.seg.Close()
	}

	return nil
}

// reserve allocates a contiguous range of n slots in the active segment,
// rotating to a new segment when necessary. Must be called under store.mu.
func (store *Store) reserve(n int) (exts []*slotExtent, err error) {
	remaining := n
	for remaining > 0 {
		// Rotate to a new segment if the current one is full.
		if store.seg.freeSlots() < 1 {
			newSegNum := store.segNum.Load() + 1
			newSeg, err := openSegment(store.dir, newSegNum)
			if err != nil {
				return nil, fmt.Errorf("store: reserve: open segment %d: %w", newSegNum, err)
			}

			store.segNum.Store(newSegNum)
			store.seg = newSeg
		}

		// Clamp to available slots; remainder spills into the next segment.
		size := min(remaining, store.seg.freeSlots())

		exts = append(exts, &slotExtent{
			seg:    store.seg,
			offset: store.seg.slotsUsed,
			size:   size,
		})

		store.seg.slotsUsed += size
		// Add one ref for this extent; released by extent.Close.
		store.seg.refs.Add(1)

		remaining -= size
	}

	// Best-effort persist; counters are recoverable from segment files.
	if err := store.saveState(); err != nil {
		slog.Warn("store: reserve: save state", "error", err)
	}

	return exts, nil
}
