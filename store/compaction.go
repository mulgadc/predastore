package store

import (
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
	wg    sync.WaitGroup
}

func (store *Store) startCompactor() {
	c := &compactor{store: store, done: make(chan struct{})}
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
		}
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

	candidates, err := store.candidateSegments()
	if err != nil {
		return fmt.Errorf("select candidates: %w", err)
	}

	var totalExtents int
	var totalBytes int64
	for _, num := range candidates {
		segStart := time.Now()
		stats, err := store.compactSegment(num)
		if err != nil {
			return fmt.Errorf("compact segment %d: %w", num, err)
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
		"duration", time.Since(start))
	return nil
}

// candidateSegments scans the tombstone namespace, sums dead bytes per segment,
// and returns segments whose live fraction is below the threshold. The active
// append segment is never a candidate — it is the relocation destination.
func (store *Store) candidateSegments() ([]uint64, error) {
	dead := make(map[uint64]int64)
	err := store.index.Scan([]byte{tombstonePrefix}, func(k, v []byte) error {
		dead[tombstoneSegNum(k)] += int64(binary.BigEndian.Uint64(v)) //nolint:gosec // tombstone value is a non-negative byte count.
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scan tombstones: %w", err)
	}

	store.mutex.Lock()
	defer store.mutex.Unlock()

	var candidates []uint64
	for num, deadBytes := range dead {
		if num == store.segNum {
			continue
		}

		seg, err := store.getSegment(num)
		if err != nil {
			return nil, fmt.Errorf("get segment %d: %w", num, err)
		}
		info, err := seg.Stat()
		if err != nil {
			return nil, fmt.Errorf("stat segment %d: %w", num, err)
		}

		size := info.Size()
		if size <= segHeaderSize {
			continue
		}
		if float64(size-deadBytes)/float64(size) < compactionLiveThreshold {
			candidates = append(candidates, num)
		}
	}
	return candidates, nil
}

// segmentStats summarises the live data relocated out of one drained segment.
type segmentStats struct {
	extents int
	bytes   int64
}

// compactSegment relocates every live extent the segment holds into the active
// append segment, then drops the drained segment and clears its tombstones. An
// extent enumerated from .idx is live only if the index still points at this
// exact slot; anything deleted or superseded is skipped.
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
		if cur.SegNum != num || cur.Off != e.Off {
			continue
		}
		if err := store.relocateExtent(key, cur); err != nil {
			return stats, fmt.Errorf("relocate extent: %w", err)
		}
		stats.extents++
		stats.bytes += cur.PSize
	}

	store.mutex.Lock()
	err = store.dropSegment(num)
	store.mutex.Unlock()
	if err != nil {
		return stats, fmt.Errorf("drop segment %d: %w", num, err)
	}

	return stats, store.deleteTombstones(num)
}

// relocateExtent copies an extent's raw fragment bytes verbatim into a freshly
// reserved slot in the active append segment, then compare-and-swaps the index
// onto the new slot. Verbatim is load-bearing: fragNum rides along inside the
// copied bytes, so the GCM nonce is preserved and never reused.
func (store *Store) relocateExtent(key []byte, old extent) error {
	fragCount := uint64(old.PSize / totalFragSize) //nolint:gosec // PSize is a non-negative multiple of totalFragSize.

	store.mutex.Lock()
	srcSeg, err := store.getSegment(old.SegNum)
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

	newExt := extent{SegNum: store.segNum, Off: dstOff, PSize: old.PSize, LSize: old.LSize}
	if err := dstSeg.appendIdx(idxEntry{Off: dstOff, Key: [36]byte(key), PSize: old.PSize}); err != nil {
		srcSeg.releaseRef()
		dstSeg.releaseRef()
		store.mutex.Unlock()
		return fmt.Errorf("append destination idx: %w", err)
	}
	store.mutex.Unlock()

	defer srcSeg.releaseRef()
	defer dstSeg.releaseRef()

	if err := copyExtent(srcSeg, old.Off, dstSeg, dstOff, old.PSize); err != nil {
		return fmt.Errorf("copy extent bytes: %w", err)
	}
	if err := dstSeg.Sync(); err != nil {
		return fmt.Errorf("sync destination segment: %w", err)
	}
	if err := dstSeg.syncIdx(); err != nil {
		return fmt.Errorf("sync destination idx: %w", err)
	}

	if _, err := store.casExtent(key, old, newExt); err != nil {
		return fmt.Errorf("commit relocation: %w", err)
	}
	return nil
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
