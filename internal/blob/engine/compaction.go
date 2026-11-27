package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
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

	// When the retained sweep last ran, from the loop or from inside a pass.
	lastRetained time.Time
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
	store := c.store
	// A timer re-armed after each pass at this node's next aligned start, so a
	// long pass skips to the next slot rather than running back to back.
	routine := time.NewTimer(time.Until(nextCompactionStart(store.clock.Now(), store.compactionInterval, store.compactionSlot, store.compactionSlots)))
	defer routine.Stop()
	rearm := func() {
		rearmTimer(routine, time.Until(nextCompactionStart(store.clock.Now(), store.compactionInterval, store.compactionSlot, store.compactionSlots)))
	}
	c.lastRetained = store.clock.Now()

	// Retained generations are swept on their own, much faster cycle. A full
	// cycle relocates extents and rewrites segments; this is one scan of the
	// retained namespace, and it is what decides how long a writer's released
	// generation keeps holding disk.
	retained := time.NewTicker(retainedSweepInterval)
	defer retained.Stop()

	for {
		select {
		case <-c.done:
			return
		case <-retained.C:
			if store.clock.Now().Sub(c.lastRetained) >= retainedSweepInterval/2 {
				c.lastRetained = store.clock.Now()
				store.retainedTick()
			}
		case <-routine.C:
			c.runPass(false)
			rearm()
		case <-c.kick:
			// A kicked pass can run over this node's slot; the routine pass
			// then waits for the next one rather than starting at once.
			c.runPass(true)
			rearm()
		}
	}
}

// rearmTimer stops t, drains a fire it missed, and resets it to d.
func rearmTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

// runPass runs one pass, unpaced if kicked or the store starts near full, and
// asks for a fresh unpaced pass if this one turned urgent part way.
func (c *compactor) runPass(kicked bool) {
	p := c.store.newPacer(c, kicked || c.store.belowNearfull())
	err := c.store.compactPass(p)
	switch {
	case errors.Is(err, errCompactionStopped), errors.Is(err, ErrClosedStore):
		return
	case err != nil:
		slog.Error("compaction cycle failed", "kicked", kicked, "error", err)
	}
	if p.rekick {
		c.store.kickCompaction()
	}
}

// retainedTick runs the two sweeps of the retained cycle. Each logs its own
// failure, so neither stops the other or the compaction cycle, and the orphan
// sweep yields to a pending compaction kick.
func (store *Store) retainedTick() {
	released, err := store.sweepRetained(store.retainedMaxAge)
	if err != nil {
		slog.Error("retained sweep failed", "error", err)
	} else if released > 0 {
		slog.Debug("released superseded generations", "count", released)
	}
	// A pending kick means the disk is filling; reclamation goes first.
	if store.kickPending() {
		return
	}
	if orphans, err := store.sweepOrphanedReleases(); err != nil {
		slog.Error("orphaned release sweep failed", "error", err)
	} else if orphans > 0 {
		slog.Debug("dropped orphaned release rows", "count", orphans)
	}
}

// kickPending reports whether a compaction kick is waiting for the loop.
func (store *Store) kickPending() bool {
	return store.compactor != nil && len(store.compactor.kick) > 0
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

// compactOnce runs one full compaction cycle at the configured pacing:
// select under-occupied segments, relocate their live extents into the active
// append segment, and drop them.
func (store *Store) compactOnce() error {
	return store.compactPass(store.newPacer(store.compactor, store.belowNearfull()))
}

// compactPass is one cycle under pacer p.
func (store *Store) compactPass(p *pacer) error {
	store.mutex.Lock()
	closed := store.closed
	store.mutex.Unlock()
	if closed {
		return ErrClosedStore
	}

	start := time.Now()
	slog.Info("compaction started", "interval", store.compactionInterval, "liveThreshold", compactionLiveThreshold,
		"slot", store.compactionSlot, "slots", store.compactionSlots, "paced", !p.urgent,
		"maxBytesPerSec", store.compactionMaxBytesPerSec)

	// Before selecting candidates: a prepared row keeps its extent alive, so a
	// caller that died between prepare and commit would otherwise pin the space
	// forever and hide it from the live-fraction estimate below.
	reaped, err := store.sweepPrepared(preparedMaxAge)
	if err != nil {
		return fmt.Errorf("sweep prepared extents: %w", err)
	}
	if reaped > 0 {
		slog.Warn("reaped abandoned prepared extents",
			"count", reaped, "older_than_ms", preparedMaxAge.Milliseconds())
	}

	// Likewise for superseded generations: a commit demotes rather than
	// tombstones, so this is where that space comes back.
	released, err := store.sweepRetained(store.retainedMaxAge)
	if err != nil {
		return fmt.Errorf("sweep retained generations: %w", err)
	}
	if released > 0 {
		slog.Info("released superseded generations",
			"count", released, "older_than_ms", retainedMaxAge.Milliseconds())
	}
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
	var totalBytes, totalReclaimed int64
	var dropped int64
	for _, num := range candidates {
		segStart := time.Now()
		stats, err := store.compactSegment(num, p)
		switch {
		case errors.Is(err, errCompactionStopped):
			return err
		// A reader or writer still holds the segment. Its extents have already
		// been relocated, so the next cycle finds nothing to move and only
		// retries the drop.
		case errors.Is(err, errSegmentBusy):
			slog.Info("compaction deferred segment", "segNum", num)
			continue
		case err != nil:
			// One bad segment must not strand the rest of the cycle: log it
			// loudly and move on rather than aborting every other candidate.
			slog.Error("compaction skipped segment", "segNum", num, "error", err)
			failed++
			continue
		}
		dropped++
		totalExtents += stats.extents
		totalBytes += stats.bytes
		totalReclaimed += stats.reclaimed
		slog.Info("compacted segment",
			"segNum", num,
			"extents", stats.extents,
			"bytes", stats.bytes,
			"duration", time.Since(segStart))
	}

	elapsed := time.Since(start)

	store.mutex.Lock()
	store.stats.cycles++
	if failed > 0 {
		store.stats.cyclesFailed++
	}
	store.stats.segmentsScanned += int64(len(candidates))
	store.stats.segmentsDropped += dropped
	store.stats.bytesRelocated += totalBytes
	store.stats.bytesReclaimed += totalReclaimed
	store.stats.lastCycleSecs = elapsed.Seconds()
	store.mutex.Unlock()

	slog.Info("compaction finished",
		"paced", !p.urgent,
		"paceWait", p.waitedTotal,
		"yielded", p.yielded,
		"segments", len(candidates),
		"extents", totalExtents,
		"bytes", totalBytes,
		"reclaimed", totalReclaimed,
		"failed", failed,
		"duration", elapsed)
	return nil
}

// preparedMaxAge is how long a prepared extent may sit unpublished before it
// is treated as abandoned. A commit follows its prepare by one round trip, so
// this is orders of magnitude beyond any live write; it is sized for a caller
// that died, not for a slow one.
const preparedMaxAge = 30 * time.Minute

// sweepPrepared drops prepared rows older than maxAge, tombstoning the space
// they reserved. Ageing is the only available test: the store cannot ask
// whether the caller that prepared a row still intends to commit it.
func (store *Store) sweepPrepared(maxAge time.Duration) (int, error) {
	cutoff := time.Now().Add(-maxAge).UnixNano()

	var stale [][]byte
	if err := store.indexScan([]byte{preparedPrefix}, func(k, v []byte) error {
		// Keys carry no namespace prefix, so roughly one object hash in 256
		// starts with preparedPrefix and lands in this scan. The namespaces are
		// told apart by width, which is also what makes decodePreparedAt total
		// here.
		if len(v) != preparedValueSize {
			return nil
		}

		// A row in the superseded single-slot layout was never acknowledged to
		// anyone, so it is reclaimed on sight rather than aged.
		if len(k) == legacyPreparedKeySize {
			stale = append(stale, append([]byte(nil), k...))
			return nil
		}
		if len(k) != preparedKeySize {
			return nil
		}

		at, err := decodePreparedAt(v)
		if err != nil {
			return fmt.Errorf("decode prepared row: %w", err)
		}
		if at < cutoff {
			stale = append(stale, append([]byte(nil), k...))
		}
		return nil
	}); err != nil {
		return 0, fmt.Errorf("scan prepared: %w", err)
	}

	dropped := 0
	for _, k := range stale {
		// The row is re-read inside the drop's own transaction, so a commit
		// that published it between the scan and here simply finds nothing
		// left to drop rather than losing its extent underneath it.
		if err := store.dropGeneration(k, func(uint64) bool { return true }); err != nil {
			return dropped, err
		}
		dropped++
	}
	return dropped, nil
}

// retainedMaxAge is how long a superseded generation stays answerable. What it
// has to outlast is the gap between a writer publishing its shards and its
// placement record landing, which is one meta round trip — so this is four
// orders of magnitude of slack, not a fine judgement.
//
// It is not sized to cover a writer that died in that gap forever. That leaves
// a record naming a generation this will eventually release, and closing it
// needs repair to reconcile a record against what the nodes actually hold,
// which is not this change.
const retainedMaxAge = 5 * time.Minute

// retainedSweepInterval is how often superseded generations are reclaimed. It
// is far shorter than the compaction cycle because it bounds the disk a
// released generation holds after its writer has finished with it, and the
// scan it runs is cheap next to a compaction pass.
const retainedSweepInterval = 15 * time.Second

// releaseStamp reports when a retained generation was released, or zero if no
// publisher has released it.
func (store *Store) releaseStamp(retained []byte) (int64, error) {
	var at int64
	err := store.index.View(func(txn *badger.Txn) error {
		raw, err := readRaw(txn, releasedKeyFor(retained))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		at, err = decodeReleasedAt(raw)

		return err
	})
	if err != nil {
		return 0, fmt.Errorf("read release: %w", err)
	}

	return at, nil
}

// orphanScanPerSweep bounds the release rows one sweep reads. Every commit
// holds back new transactions until it is applied, so drops commit in small
// batches that request paths can interleave with.
const (
	orphanReleaseBatch = 100
	orphanScanPerSweep = 10000
)

// retainedKeyFor maps a release row's key back onto the retained row it marks.
func retainedKeyFor(released []byte) []byte {
	key := append([]byte(nil), released...)
	key[0] = retainedPrefix

	return key
}

// sweepOrphanedReleases drops release rows whose retained row is gone, which a
// shard delete leaves behind. A release is written only while its retained row
// exists, so a release row without one can never be read again.
func (store *Store) sweepOrphanedReleases() (int, error) {
	orphans, err := store.collectOrphanedReleases()
	if err != nil {
		return 0, err
	}

	return store.dropOrphanedReleases(orphans)
}

// collectOrphanedReleases reads up to orphanScanPerSweep release rows from
// where the last sweep stopped, wrapping at the end of the namespace, and
// returns those whose retained row is absent from one snapshot.
func (store *Store) collectOrphanedReleases() ([][]byte, error) {
	store.orphanMu.Lock()
	defer store.orphanMu.Unlock()

	start := store.orphanCursor
	if start == nil {
		start = []byte{releasedPrefix}
	}
	var orphans [][]byte
	var next []byte
	err := store.index.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{releasedPrefix}
		opts.PrefetchValues = false
		// Every version, deleted ones included, counts toward the cap, so the
		// tombstones a drop leaves cannot make a sweep's scan unbounded.
		opts.AllVersions = true
		it := txn.NewIterator(opts)
		defer it.Close()

		scanned := 0
		var key []byte
		for it.Seek(start); it.Valid(); it.Next() {
			item := it.Item()
			scanned++
			if key != nil && bytes.Equal(item.Key(), key) {
				continue
			}
			// Stop only between keys, so a resumed sweep starts at a newest version.
			if scanned > orphanScanPerSweep {
				next = item.KeyCopy(nil)
				break
			}
			key = item.KeyCopy(nil)
			// A live row whose object hash starts with releasedPrefix sorts in
			// here too; only a release row has this width.
			if item.IsDeletedOrExpired() || len(key) != releasedKeySize {
				continue
			}
			_, err := txn.Get(retainedKeyFor(key))
			if errors.Is(err, badger.ErrKeyNotFound) {
				orphans = append(orphans, key)
				continue
			}
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scan release rows: %w", err)
	}
	store.orphanCursor = next

	return orphans, nil
}

// dropOrphanedReleases deletes the given release rows in bounded transactions,
// skipping any whose retained row exists again by then.
func (store *Store) dropOrphanedReleases(orphans [][]byte) (int, error) {
	dropped := 0
	for start := 0; start < len(orphans); start += orphanReleaseBatch {
		if store.kickPending() {
			// Resume at the first undropped orphan once the kick is served.
			store.orphanMu.Lock()
			store.orphanCursor = orphans[start]
			store.orphanMu.Unlock()

			return dropped, nil
		}
		batch := orphans[start:min(start+orphanReleaseBatch, len(orphans))]
		var n int
		err := store.index.Update(func(txn *badger.Txn) error {
			var err error
			n, err = dropOrphanBatch(txn, batch)

			return err
		})
		if errors.Is(err, badger.ErrConflict) {
			// The next sweep finds whatever this batch left.
			continue
		}
		if err != nil {
			return dropped, fmt.Errorf("drop orphaned release rows: %w", err)
		}
		dropped += n
	}

	return dropped, nil
}

// dropOrphanBatch deletes the release rows in batch whose retained row txn
// cannot see. The read joins txn's conflict set, so a commit that writes the
// retained row back before txn commits fails txn rather than orphaning it.
func dropOrphanBatch(txn *badger.Txn, batch [][]byte) (int, error) {
	n := 0
	for _, key := range batch {
		_, err := txn.Get(retainedKeyFor(key))
		if err == nil {
			continue
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return n, err
		}
		if err := txn.Delete(key); err != nil {
			return n, err
		}
		n++
	}

	return n, nil
}

// sweepRetained reclaims generations that a commit superseded and nothing has
// asked for since. It is the only thing that decides a generation is dead —
// commit demotes, and never destroys.
//
// A release is what makes a generation reclaimable, and age only bounds how
// long a reader that already resolved it keeps answering. Age alone cannot
// decide: a writer that commits and dies before publishing starts no clock,
// leaves the record naming the generation its commit demoted, and the write
// path has no deadline that bounds when it could still publish. Reclaiming on
// age destroyed objects every writer had been acknowledged for.
func (store *Store) sweepRetained(maxAge time.Duration) (int, error) {
	cutoff := time.Now().Add(-maxAge).UnixNano()

	type row struct {
		key []byte
		at  int64
	}

	var stale [][]byte
	var group []row

	// Rows sort by position and then by big-endian epoch, so one position's
	// generations arrive contiguously and oldest first.
	flush := func() {
		for _, r := range group {
			if r.at < cutoff {
				stale = append(stale, r.key)
			}
		}
		group = nil
	}

	if err := store.indexScan([]byte{retainedPrefix}, func(k, v []byte) error {
		// Same width discrimination as the prepared sweep: an object hash
		// starting with retainedPrefix puts a 36-byte live row in this scan.
		if len(k) != retainedKeySize || len(v) != preparedValueSize {
			return nil
		}
		key := append([]byte(nil), k...)
		at, err := store.releaseStamp(key)
		if err != nil {
			return err
		}
		if at == 0 {
			// Not released: nothing has reported a record displacing it.
			return nil
		}
		if len(group) > 0 && !bytes.Equal(group[0].key[:retainedKeySize-8], key[:retainedKeySize-8]) {
			flush()
		}
		group = append(group, row{key: key, at: at})
		return nil
	}); err != nil {
		return 0, fmt.Errorf("scan retained: %w", err)
	}
	flush()

	dropped := 0
	for _, k := range stale {
		if err := store.dropGeneration(k, func(uint64) bool { return true }); err != nil {
			return dropped, err
		}
		dropped++
	}
	return dropped, nil
}

// tombstoneTotals summarises one segment's tombstones: the dead bytes they
// account for, how many there are, and one example slot. The example is what
// identifies the writer when the segment a tombstone names is already gone.
type tombstoneTotals struct {
	bytes   int64
	count   int
	sampOff int64
	sampLen int64
}

// candidateSegments scans the tombstone namespace, sums dead bytes per segment,
// and returns segments whose live fraction is below the threshold, plus a
// count of segments that could not be inspected. The active append segment is
// never a candidate — it is the relocation destination.
//
// A segment whose file is gone has its tombstones reclaimed here, since nothing
// else ever would: deleteTombstones is reachable only for a candidate, and the
// missing file is what stops the segment becoming one.
//
// A segment that exists but cannot be opened or stat'd is a real fault. That
// one is logged loudly and counted, and skipped rather than aborting the scan,
// so one bad segment does not strand every other reclaimable candidate.
func (store *Store) candidateSegments() ([]uint64, int, error) {
	dead := make(map[uint64]tombstoneTotals)
	err := store.indexScan([]byte{tombstonePrefix}, func(k, v []byte) error {
		// Keys carry no namespace prefix, so roughly one hash in
		// 256 starts with tombstonePrefix and lands in this scan. Reading a
		// segment number out of the middle of a hash invents one, so match on
		// the fixed tombstone width before trusting the key.
		if len(k) != tombstoneKeySize {
			return nil
		}
		psize := int64(binary.BigEndian.Uint64(v)) //nolint:gosec // tombstone value is a non-negative byte count.
		num := tombstoneSegNum(k)
		totals := dead[num]
		totals.bytes += psize
		totals.count++
		if totals.count == 1 {
			totals.sampOff, totals.sampLen = tombstoneOff(k), psize
		}
		dead[num] = totals
		return nil
	})
	if err != nil {
		return nil, 0, fmt.Errorf("scan tombstones: %w", err)
	}

	store.recordOccupancy(dead)

	var candidates, dropped []uint64
	var failed int
	// One short locked section per segment, so foreground requests never wait
	// behind a scan that grows with the store.
	for num, totals := range dead {
		store.mutex.Lock()
		active := num == store.segNum
		var seg *segment
		var err error
		if !active {
			seg, err = store.getSegment(num, false)
		}
		store.mutex.Unlock()
		if active {
			continue
		}
		if err != nil {
			// A segment file that is not there cannot hold live data, so every
			// tombstone naming it is dead weight. Reclaim it below, outside the
			// mutex, rather than reporting it on every cycle forever.
			if errors.Is(err, fs.ErrNotExist) && !store.segmentFileExists(num) {
				dropped = append(dropped, num)
				continue
			}
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
		if float64(size-totals.bytes)/float64(size) < compactionLiveThreshold {
			candidates = append(candidates, num)
		}
	}

	for _, num := range dropped {
		totals := dead[num]
		// The slot is the diagnostic: a tombstone in a segment that has already
		// been dropped was written by something holding a row that still named
		// it, and the offset and size are what identify which extent that was.
		slog.Warn("reclaiming tombstones naming a dropped segment",
			"segNum", num,
			"tombstones", totals.count,
			"deadBytes", totals.bytes,
			"sampleOff", totals.sampOff,
			"samplePSize", totals.sampLen)

		// A drop interrupted between its two unlinks leaves the sidecar; it
		// goes, durably, before the tombstones that would retry the drop.
		if err := store.removeLeftoverSidecar(num); err != nil {
			slog.Error("could not remove what a dropped segment left", "segNum", num, "error", err)
			failed++
			continue
		}
		if err := store.deleteTombstones(num); err != nil {
			slog.Error("could not reclaim tombstones naming a dropped segment", "segNum", num, "error", err)
			failed++
		}
	}

	return candidates, failed, nil
}

// recordOccupancy measures how much of the store's on-disk bytes are dead, for
// Snapshot to report. It runs once per compaction cycle, where a directory walk
// is affordable, precisely so the reporting path never has to do one.
//
// dead carries every tombstone, including any naming a segment that has
// already been dropped and is reclaimed later in this cycle, so the live figure
// is clamped at zero rather than trusted to agree with the files on disk.
func (store *Store) recordOccupancy(dead map[uint64]tombstoneTotals) {
	entries, err := os.ReadDir(store.dir)
	if err != nil {
		slog.Warn("occupancy measurement skipped", "dir", store.dir, "error", err)
		return
	}

	var total int64
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".seg" {
			continue
		}
		info, err := e.Info()
		if err != nil {
			slog.Warn("occupancy measurement skipped a segment", "name", e.Name(), "error", err)
			continue
		}
		total += info.Size()
	}

	var deadBytes int64
	for _, totals := range dead {
		deadBytes += totals.bytes
	}

	store.mutex.Lock()
	defer store.mutex.Unlock()
	store.stats.scanAt = time.Now()
	store.stats.deadBytes = min(deadBytes, total)
	store.stats.liveBytes = max(total-deadBytes, 0)
}

// segmentStats summarises the live data relocated out of one drained segment,
// and the on-disk bytes its drop gave back.
type segmentStats struct {
	extents   int
	bytes     int64
	reclaimed int64
}

// compactSegment relocates a segment's live extents into the active append
// segment, then drops it and clears its tombstones.
func (store *Store) compactSegment(num uint64, p *pacer) (segmentStats, error) {
	var stats segmentStats

	// A writer still open in this segment has not prepared its row yet, so the
	// enumeration below would skip its extent and the drop would lose it.
	store.mutex.Lock()
	src, err := store.getSegment(num, false)
	writing := err == nil && src.writers.Load() > 0
	store.mutex.Unlock()
	if writing {
		return stats, fmt.Errorf("%w: segment %d has an open writer", errSegmentBusy, num)
	}

	entries, err := scanIdx(store.dir, num)
	if err != nil {
		return stats, fmt.Errorf("scan idx %d: %w", num, err)
	}

	unresolved := 0
	for _, e := range entries {
		if p.stopped() {
			return stats, errCompactionStopped
		}
		moved, size, err := store.relocateSlot(e.Key, num, e.Off, p)
		if err != nil {
			return stats, err
		}
		switch {
		case moved:
			stats.extents++
			stats.bytes += size
		case size > 0:
			unresolved++
		}
	}
	// Closing: the extent in flight finished, but the segment is left for the
	// next pass rather than dropped on the way out.
	if p.stopped() {
		return stats, errCompactionStopped
	}
	// A row still names a slot here after every attempt, so the segment holds
	// data someone can read; keep it, and the next cycle tries again.
	if unresolved > 0 {
		return stats, fmt.Errorf("%w: segment %d, %d extents still named after relocation", errSegmentBusy, num, unresolved)
	}

	// The repoints above, and any made by an earlier pass that failed before its
	// drop, may be only in the page cache: badger runs with SyncWrites off. The
	// unlink is durable, so the index is synced first on every drop.
	if err := store.syncIndex(); err != nil {
		return stats, fmt.Errorf("sync index before drop %d: %w", num, err)
	}

	store.mutex.Lock()
	// Measured before the unlink, since afterwards there is nothing to stat.
	// A segment that cannot be measured is still dropped: the reclaim figure
	// is reporting, and must not decide whether the drop happens.
	var size int64
	if seg, segErr := store.getSegment(num, false); segErr == nil {
		if info, statErr := seg.Stat(); statErr == nil {
			size = info.Size()
		}
	}
	err = store.detachSegment(num)
	store.mutex.Unlock()
	if err != nil {
		return stats, fmt.Errorf("drop segment %d: %w", num, err)
	}

	// The unlink frees every extent before it returns, seconds for a sealed
	// segment, so it runs unlocked. No row names the segment any more; until
	// the tombstones go, a failed or interrupted unlink is retried next cycle.
	unlinkStart := time.Now()
	segUnlink, err := store.unlinkSegment(num)
	if err != nil {
		return stats, fmt.Errorf("drop segment %d: %w", num, err)
	}
	stats.reclaimed = size
	slog.Info("dropped segment", "segNum", num, "bytes", size,
		"segUnlinkStart", unlinkStart.UTC().Format(time.RFC3339Nano), "segUnlink", segUnlink, "unlink", time.Since(unlinkStart))

	return stats, store.deleteTombstones(num)
}

// relocateAttempts bounds how often one slot is re-read and re-relocated after
// losing its swap to a concurrent commit, which moves the row it raced.
const relocateAttempts = 3

// relocateSlot relocates whichever row occupies segment num at off. A lost swap
// means a commit moved that row, possibly into a namespace still naming this
// slot, so the slot is read afresh and tried again. moved reports a relocation;
// a zero size with moved false means no row names the slot any more.
func (store *Store) relocateSlot(idxKey [36]byte, num uint64, off int64, p *pacer) (moved bool, size int64, err error) {
	for range relocateAttempts {
		badgerKey, raw, cur, err := store.rowAt(idxKey, num, off)
		if err != nil {
			return false, 0, err
		}
		// A .idx row is only live if some index key still points at this exact
		// slot; anything deleted or superseded is already dead.
		if badgerKey == nil {
			return false, 0, nil
		}
		moved, err := store.relocateExtent(badgerKey, idxKey, raw, cur, p)
		if err != nil {
			return false, 0, fmt.Errorf("relocate extent: %w", err)
		}
		if moved {
			return true, cur.PSize, nil
		}
		size = cur.PSize
	}
	badgerKey, _, _, err := store.rowAt(idxKey, num, off)
	if err != nil {
		return false, 0, err
	}
	if badgerKey == nil {
		return false, 0, nil
	}
	return false, size, nil
}

// rowAt finds the index key, if any, whose extent occupies segment num at off.
// It checks the live row, then the prepared and retained generations: a
// prepared extent is data a commit is about to publish and a retained one is
// data a placement record may still name, so dropping either's segment would
// lose a write that has already been acknowledged as durable.
//
// Prepared and retained rows are keyed by epoch, so there may be several of
// each and none can be addressed directly — hence the scan.
func (store *Store) rowAt(idxKey [36]byte, num uint64, off int64) (badgerKey, raw []byte, ext extent, err error) {
	// One snapshot for all three namespaces, so a commit moving a row between
	// them mid-lookup cannot hide it from every one.
	err = store.index.View(func(txn *badger.Txn) error {
		value, err := readRaw(txn, idxKey[:])
		switch {
		case err == nil:
			cur, _, decodeErr := decodeIndexValue(value)
			if decodeErr != nil {
				return fmt.Errorf("decode extent: %w", decodeErr)
			}
			if cur.SegNum == num && cur.Off == off {
				badgerKey, raw, ext = idxKey[:], value, cur
				return nil
			}
		case !errors.Is(err, badger.ErrKeyNotFound):
			return fmt.Errorf("get extent: %w", err)
		}

		for _, prefix := range [][]byte{preparedScan(idxKey[:]), retainedScan(idxKey[:])} {
			found, err := scanRowAt(txn, prefix, num, off)
			if err != nil {
				return err
			}
			if found != nil {
				badgerKey, raw, ext = found.key, found.raw, found.ext
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, extent{}, err
	}
	return badgerKey, raw, ext, nil
}

type slotRow struct {
	key, raw []byte
	ext      extent
}

// scanRowAt returns the epoch-keyed row under prefix whose extent occupies
// segment num at off, or nil.
func scanRowAt(txn *badger.Txn, prefix []byte, num uint64, off int64) (*slotRow, error) {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	it := txn.NewIterator(opts)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		if len(item.Key()) != len(prefix)+8 {
			continue
		}
		v, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		cur, _, err := decodeIndexValue(v)
		if err != nil {
			return nil, fmt.Errorf("decode extent: %w", err)
		}
		if cur.SegNum == num && cur.Off == off {
			return &slotRow{key: item.KeyCopy(nil), raw: v, ext: cur}, nil
		}
	}
	return nil, nil
}

// relocateExtent moves an extent into the active append segment and repoints
// the index at it, unless a concurrent overwrite or delete of the same key gets
// there first, which moved reports as false. badgerKey is the live, prepared or
// retained row; idxKey is always the 36-byte form the sidecar records.
func (store *Store) relocateExtent(badgerKey []byte, idxKey [36]byte, oldRaw []byte, old extent, p *pacer) (moved bool, err error) {
	_, epoch, err := decodeIndexValue(oldRaw)
	if err != nil {
		return false, fmt.Errorf("decode source epoch: %w", err)
	}
	fragCount := uint64(old.PSize / totalFragSize) //nolint:gosec // PSize is a non-negative multiple of totalFragSize.

	store.mutex.Lock()
	// Read path: the extent's index entry claims this segment holds live data,
	// so a missing segment here must be reported rather than fabricated.
	srcSeg, err := store.getSegment(old.SegNum, false)
	if err != nil {
		store.mutex.Unlock()
		return false, fmt.Errorf("get source segment %d: %w", old.SegNum, err)
	}
	srcSeg.addRef()

	dstSeg, dstOff, err := store.reserveExtent(fragCount)
	if err != nil {
		srcSeg.releaseRef()
		store.mutex.Unlock()
		return false, fmt.Errorf("reserve destination: %w", err)
	}
	dstSeg.addRef()

	// dstSeg, not store.segNum: reserveExtent can roll onto a new segment, and
	// the destination the extent actually landed in is the one that must be
	// recorded, not whatever store.segNum happens to be afterward.
	newExt := extent{SegNum: dstSeg.num, Off: dstOff, PSize: old.PSize, LSize: old.LSize}
	if err := dstSeg.appendPrepareAllocation(idxEntry{Off: dstOff, Key: idxKey, PSize: old.PSize}, epoch, old.LSize); err != nil {
		srcSeg.releaseRef()
		dstSeg.releaseRef()
		store.mutex.Unlock()
		return false, fmt.Errorf("append destination idx: %w", err)
	}
	// Copy without the mutex so compaction never stalls the write path. This is
	// the window a racing overwrite or delete lands in.
	store.mutex.Unlock()

	defer srcSeg.releaseRef()
	defer dstSeg.releaseRef()

	// Verbatim is load-bearing: fragNum rides inside the copied bytes, so the
	// nonce moves with them and is never reissued.
	if err := copyExtent(srcSeg, old.Off, dstSeg, dstOff, old.PSize, p); err != nil {
		return false, fmt.Errorf("copy extent bytes: %w", err)
	}
	if err := dstSeg.Sync(); err != nil {
		return false, fmt.Errorf("sync destination segment: %w", err)
	}
	if err := dstSeg.syncIdx(); err != nil {
		return false, fmt.Errorf("sync destination idx: %w", err)
	}
	if store.beforeRelocateSwap != nil {
		store.beforeRelocateSwap()
	}

	// Swap onto the new slot only if the key still holds the one we copied from.
	for {
		var stale bool
		err := store.index.Update(func(txn *badger.Txn) error {
			stale = false
			item, err := txn.Get(badgerKey)
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
				stale = !bytes.Equal(cur, oldRaw)
			}

			// Losing the race strands the copy for good — only this swap could have
			// referenced it, and slots are never reissued — so tombstone it here
			// rather than let it pad its segment's live count.
			if stale {
				return txn.Set(tombstoneKey(newExt.SegNum, newExt.Off), tombstoneValue(newExt.PSize))
			}
			// Rewrite the extent and keep the tail, so the epoch — and, on a
			// prepared row, its stamp — survives the move untouched.
			return txn.Set(badgerKey, repointValue(oldRaw, newExt))
		})

		// The read above puts this txn under badger's conflict detection, which a
		// concurrent commit on the same key trips.
		if errors.Is(err, badger.ErrConflict) {
			continue
		}
		if err != nil {
			return false, fmt.Errorf("commit relocation: %w", err)
		}
		return !stale, nil
	}
}

// copyExtent streams size bytes from src@srcOff to dst@dstOff via disjoint
// ReadAt/WriteAt, so it runs lock-free against concurrent reads and writes,
// pacing after each buffer.
func copyExtent(src *segment, srcOff int64, dst *segment, dstOff int64, size int64, p *pacer) error {
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
		p.copied(n)
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
	if err := store.indexScan(prefix, func(k, _ []byte) error {
		// This scan feeds deletes, so a key colliding with the tombstone
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
		if err := store.indexDelete(k); err != nil {
			return fmt.Errorf("delete tombstone: %w", err)
		}
	}
	return nil
}
