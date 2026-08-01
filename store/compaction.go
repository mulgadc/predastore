package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

const (
	// defaultCompactionInterval is the fallback cycle period used when the
	// caller does not supply one via WithCompaction.
	defaultCompactionInterval = 5 * time.Minute

	// compactionLiveThreshold is the live-byte fraction below which a segment
	// becomes a compaction candidate. A segment whose live data is under this
	// share of its file size is worth draining.
	compactionLiveThreshold = 0.70
)

// compactor runs compactOnce on an interval until stopped. Started only when
// WithCompaction is supplied; stopped and joined by Close before segments and
// the index are drained.
type compactor struct {
	store *Store
	done  chan struct{}

	// kick requests an out-of-cycle compaction pass. Buffered to 1 so a
	// non-blocking send never stalls the caller (Append holds store.mutex
	// while it sends); a pending kick already covers one that arrives
	// before the loop drains it.
	kick chan struct{}

	wg sync.WaitGroup
}

func (store *Store) startCompactor() {
	c := &compactor{store: store, done: make(chan struct{}), kick: make(chan struct{}, 1)}
	store.compactor = c
	c.wg.Add(1)
	slog.Info("compactor started")
	go c.loop()
}

func (c *compactor) loop() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.store.compactionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			if err := c.store.compactOnce(); err != nil {
				slog.Error("compaction cycle failed", "error", err)
			}
		case <-c.kick:
			if err := c.store.compactOnce(); err != nil {
				slog.Error("kicked compaction cycle failed", "error", err)
			}
		}
	}
}

// kickCompaction signals the compactor goroutine to run an immediate pass,
// without calling compactOnce inline — Append (its caller on the nearfull
// path) already holds store.mutex, which compactOnce also needs. A no-op if
// compaction is disabled or a kick is already pending.
func (store *Store) kickCompaction() {
	if store.compactor == nil {
		return
	}
	select {
	case store.compactor.kick <- struct{}{}:
	default:
	}
}

func (c *compactor) stop() {
	close(c.done)
	c.wg.Wait()
	slog.Info("compactor stopped")
}

// compactOnce runs one full compaction cycle: select under-occupied segments,
// relocate their live extents into the active append segment, and drop them.
func (store *Store) compactOnce() error {
	store.mutex.Lock()
	closed := store.closed
	store.mutex.Unlock()
	if closed {
		return ErrClosedStore
	}

	start := time.Now()
	slog.Info("compaction started", "interval", store.compactionInterval, "liveThreshold", compactionLiveThreshold)

	candidates, failed, err := store.candidateSegments()
	if err != nil {
		return fmt.Errorf("select candidates: %w", err)
	}

	// Persist the active segNum before dropping any source segment. Every
	// candidate is below the active segment (candidateSegments excludes it), and
	// the append path never unlinks segments, so it flushes segNum only lazily.
	// Without this flush a restart could read a stale, lower segNum and recreate
	// an empty segment at a number this cycle is about to drop, shadowing the live
	// data now sitting above it.
	if len(candidates) > 0 {
		store.mutex.Lock()
		err = store.saveState()
		store.mutex.Unlock()
		if err != nil {
			return fmt.Errorf("persist segment counter before compaction drops: %w", err)
		}
	}

	var totalExtents int
	var totalBytes int64
	for _, num := range candidates {
		segStart := time.Now()
		stats, err := store.compactSegment(num)
		if err != nil {
			// One bad segment must not strand the rest of the cycle: log it
			// loudly and move on rather than aborting every other candidate.
			slog.Error("compaction skipped segment", "segNum", num, "error", err)
			failed++
			continue
		}
		totalExtents += stats.extents
		totalBytes += stats.bytes
		slog.Info("compacted segment",
			"segNum", num,
			"extents", stats.extents,
			"bytes", stats.bytes,
			"duration", time.Since(segStart))
	}

	slog.Info("compaction finished",
		"segments", len(candidates),
		"extents", totalExtents,
		"bytes", totalBytes,
		"failed", failed,
		"duration", time.Since(start))
	return nil
}

// candidateSegments scans the tombstone namespace, sums dead bytes per segment,
// and returns segments whose live fraction is below the threshold, plus a
// count of segments that could not be inspected.
//
// The active append segment is never returned as a candidate in this pass —
// it is the relocation destination, and it may still be receiving writes. But
// if its own live fraction is below the threshold, it is sealed (the same
// markFull a size-triggered roll uses) and store.segNum is advanced past it,
// exactly like a normal roll. Without this, a store whose data never crosses
// maxSegSize keeps its only segment active forever, and its dead bytes are
// never reclaimable no matter how large. The sealed segment is left for the
// next cycle to pick up as an ordinary, no-longer-active candidate: any
// writer that reserved a slot in it before the roll is only guaranteed to
// have finished by dropSegment's addRef/waitForRefs pairing, not by elapsed
// time, but deferring candidacy by one cycle keeps its exposure no worse than
// any size-rolled segment gets.
//
// A segment that fails to open or stat is skipped rather than aborting the
// whole scan: one bad segment (e.g. dropped out from under a stale tombstone)
// must not strand every other reclaimable candidate. The skip is logged loudly
// so a persistently bad segment stays visible rather than silently dropped.
func (store *Store) candidateSegments() ([]uint64, int, error) {
	dead := make(map[uint64]int64)
	err := store.index.Scan([]byte{tombstonePrefix}, func(k, v []byte) error {
		// Shard keys carry no namespace prefix, so roughly one object hash in
		// 256 starts with tombstonePrefix and lands in this scan. Reading a
		// segment number out of the middle of a hash invents one, so match on
		// the fixed tombstone width before trusting the key.
		if len(k) != tombstoneKeySize {
			return nil
		}
		dead[tombstoneSegNum(k)] += int64(binary.BigEndian.Uint64(v)) //nolint:gosec // tombstone value is a non-negative byte count.
		return nil
	})
	if err != nil {
		return nil, 0, fmt.Errorf("scan tombstones: %w", err)
	}

	store.mutex.Lock()
	defer store.mutex.Unlock()

	var candidates []uint64
	var failed int
	for num, deadBytes := range dead {
		// Read path: a candidate must already exist. If it doesn't (e.g. a
		// tombstone survived a segment already dropped), skip it and keep
		// going rather than fabricating a stub or stalling the whole scan.
		seg, err := store.getSegment(num, false)
		if err != nil {
			slog.Error("compaction candidate segment unreadable, skipping", "segNum", num, "error", err)
			failed++
			continue
		}
		info, err := seg.Stat()
		if err != nil {
			slog.Error("compaction candidate segment unreadable, skipping", "segNum", num, "error", err)
			failed++
			continue
		}

		size := info.Size()
		if size <= segHeaderSize {
			continue
		}
		if float64(size-deadBytes)/float64(size) >= compactionLiveThreshold {
			continue
		}

		if num == store.segNum {
			// Seal and roll now so next cycle sees a normal, non-active
			// candidate; do not compact it in this same pass (see doc comment).
			if err := seg.markFull(); err != nil {
				slog.Error("compaction: failed to seal stale active segment, will retry next cycle", "segNum", num, "error", err)
				failed++
				continue
			}
			if _, err := store.rollSegment(); err != nil {
				slog.Error("compaction: failed to roll past sealed active segment, will retry next cycle", "segNum", num, "error", err)
				failed++
				continue
			}
			continue
		}

		candidates = append(candidates, num)
	}
	return candidates, failed, nil
}

// segmentStats summarises the live data relocated out of one drained segment.
type segmentStats struct {
	extents int
	bytes   int64
}

// compactSegment relocates a segment's live extents into the active append
// segment, then drops it and clears its tombstones.
func (store *Store) compactSegment(num uint64) (segmentStats, error) {
	var stats segmentStats

	entries, err := scanIdx(store.dir, num)
	if err != nil {
		return stats, fmt.Errorf("scan idx %d: %w", num, err)
	}

	for _, e := range entries {
		key := e.Key[:]
		raw, err := store.index.Get(key)
		if errors.Is(err, badger.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return stats, fmt.Errorf("get extent: %w", err)
		}
		cur, err := decodeExtent(raw)
		if err != nil {
			return stats, fmt.Errorf("decode extent: %w", err)
		}
		// A .idx row is only live if the index still points at this exact slot;
		// anything deleted or superseded is already dead.
		if cur.SegNum != num || cur.Off != e.Off {
			continue
		}
		if err := store.relocateExtent(key, cur); err != nil {
			return stats, fmt.Errorf("relocate extent: %w", err)
		}
		stats.extents++
		stats.bytes += cur.PSize
	}

	// The relocations above CAS-committed the repointed rows into badger, which
	// runs with SyncWrites off, so they may still be only in the OS page cache.
	// dropSegment durably unlinks the source, so without this fsync a power loss
	// or kernel panic between the two could revert the index to a segment that is
	// already gone. Skip it when nothing moved: an empty drop repoints nothing.
	if stats.extents > 0 {
		if err := store.index.Badger.Sync(); err != nil {
			return stats, fmt.Errorf("sync index before drop %d: %w", num, err)
		}
	}

	store.mutex.Lock()
	err = store.dropSegment(num)
	store.mutex.Unlock()
	if err != nil {
		return stats, fmt.Errorf("drop segment %d: %w", num, err)
	}

	return stats, store.deleteTombstones(num)
}

// relocateExtent moves an extent into the active append segment and repoints
// the index at it, unless a concurrent overwrite or delete of the same key gets
// there first.
func (store *Store) relocateExtent(key []byte, old extent) error {
	fragCount := uint64(old.PSize / totalFragSize) //nolint:gosec // PSize is a non-negative multiple of totalFragSize.

	store.mutex.Lock()
	// Read path: the extent's index entry claims this segment holds live data,
	// so a missing segment here must be reported rather than fabricated.
	srcSeg, err := store.getSegment(old.SegNum, false)
	if err != nil {
		store.mutex.Unlock()
		return fmt.Errorf("get source segment %d: %w", old.SegNum, err)
	}
	srcSeg.addRef()

	dstSeg, dstOff, err := store.reserveExtent(fragCount)
	if err != nil {
		srcSeg.releaseRef()
		store.mutex.Unlock()
		return fmt.Errorf("reserve destination: %w", err)
	}
	dstSeg.addRef()

	// dstSeg, not store.segNum: reserveExtent can roll onto a new segment, and
	// the destination the extent actually landed in is the one that must be
	// recorded, not whatever store.segNum happens to be afterward.
	newExt := extent{SegNum: dstSeg.num, Off: dstOff, PSize: old.PSize, LSize: old.LSize}
	if err := dstSeg.appendIdx(idxEntry{Off: dstOff, Key: [36]byte(key), PSize: old.PSize}); err != nil {
		srcSeg.releaseRef()
		dstSeg.releaseRef()
		store.mutex.Unlock()
		return fmt.Errorf("append destination idx: %w", err)
	}
	// Copy without the mutex so compaction never stalls the write path. This is
	// the window a racing overwrite or delete lands in.
	store.mutex.Unlock()

	defer srcSeg.releaseRef()
	defer dstSeg.releaseRef()

	// Verbatim is load-bearing: fragNum rides inside the copied bytes, so the
	// nonce moves with them and is never reissued.
	if err := copyExtent(srcSeg, old.Off, dstSeg, dstOff, old.PSize); err != nil {
		return fmt.Errorf("copy extent bytes: %w", err)
	}
	if err := dstSeg.Sync(); err != nil {
		return fmt.Errorf("sync destination segment: %w", err)
	}
	if err := dstSeg.syncIdx(); err != nil {
		return fmt.Errorf("sync destination idx: %w", err)
	}

	// Swap onto the new slot only if the key still holds the one we copied from.
	for {
		err := store.index.Badger.Update(func(txn *badger.Txn) error {
			var stale bool
			item, err := txn.Get(key)
			switch {
			case errors.Is(err, badger.ErrKeyNotFound):
				stale = true
			case err != nil:
				return err
			default:
				cur, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				stale = !bytes.Equal(cur, old.encode())
			}

			// Losing the race strands the copy for good — only this swap could have
			// referenced it, and slots are never reissued — so tombstone it here
			// rather than let it pad its segment's live count.
			if stale {
				return txn.Set(tombstoneKey(newExt.SegNum, newExt.Off), tombstoneValue(newExt.PSize))
			}
			return txn.Set(key, newExt.encode())
		})

		// The read above puts this txn under badger's conflict detection, which a
		// concurrent commit on the same key trips.
		if errors.Is(err, badger.ErrConflict) {
			continue
		}
		if err != nil {
			return fmt.Errorf("commit relocation: %w", err)
		}
		return nil
	}
}

// copyExtent streams size bytes from src@srcOff to dst@dstOff via disjoint
// ReadAt/WriteAt, so it runs lock-free against concurrent reads and writes.
func copyExtent(src *segment, srcOff int64, dst *segment, dstOff int64, size int64) error {
	if size == 0 {
		return nil
	}
	buf := make([]byte, min(size, int64(bufLen)*totalFragSize))
	for pos := int64(0); pos < size; {
		n := min(size-pos, int64(len(buf)))
		if _, err := src.ReadAt(buf[:n], srcOff+pos); err != nil {
			return err
		}
		if _, err := dst.WriteAt(buf[:n], dstOff+pos); err != nil {
			return err
		}
		pos += n
	}
	return nil
}

// deleteTombstones clears the d ‖ segNum ‖ * namespace after a segment is
// dropped. Stale tombstones left by a crash are harmless and reclaimed later.
func (store *Store) deleteTombstones(segNum uint64) error {
	prefix := make([]byte, 9)
	prefix[0] = tombstonePrefix
	binary.BigEndian.PutUint64(prefix[1:9], segNum)

	var keys [][]byte
	if err := store.index.Scan(prefix, func(k, _ []byte) error {
		// This scan feeds deletes, so a shard key colliding with the tombstone
		// namespace would cost live data rather than accuracy. Width-check it.
		if len(k) != tombstoneKeySize {
			return nil
		}
		keys = append(keys, append([]byte(nil), k...))
		return nil
	}); err != nil {
		return fmt.Errorf("scan tombstones %d: %w", segNum, err)
	}

	for _, k := range keys {
		if err := store.index.Delete(k); err != nil {
			return fmt.Errorf("delete tombstone: %w", err)
		}
	}
	return nil
}
