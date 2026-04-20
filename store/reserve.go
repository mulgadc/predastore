package store

import (
	"fmt"
	"log/slog"
)

// extent is a pointer to a contiguous range of slots within a single segment.
type extent struct {
	seg    *segment
	offset int
	size   int
}

func (ext *extent) Close() error {
	if ext.seg.refs.Add(-1) == 0 {
		return ext.seg.Close()
	}

	return nil
}

// Claim a contiguous range of n slots in the active segment under
// mu, rotating to a new segment if necessary.
func (store *Store) reserve(n int) (exts []*extent, err error) {
	remaining := n
	for remaining > 0 {
		if store.seg.freeSlots() < 1 {
			newSegNum := store.segNum.Load() + 1
			newSeg, err := openSegment(store.dir, newSegNum)
			if err != nil {
				return nil, fmt.Errorf("store: reserve: open segment %d: %w", newSegNum, err)
			}

			store.segNum.Store(newSegNum)
			store.seg = newSeg
		}

		size := min(remaining, store.seg.freeSlots())

		exts = append(exts, &extent{
			seg:    store.seg,
			offset: store.seg.slotsUsed,
			size:   size,
		})

		store.seg.slotsUsed += size
		store.seg.refs.Add(1)

		remaining -= size
	}

	if err := store.saveState(); err != nil {
		slog.Warn("store: reserve: save state", "error", err)
	}

	return exts, nil
}
