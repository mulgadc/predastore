// Package engine implements segment-based large-value storage with authenticated
// at-rest encryption. Values are written as contiguous extents of fixed-size
// fragments within append-only segment files. Segments roll when they reach
// maxSegSize. Each fragment is sealed under AES-256-GCM (master key per
// cluster, storeID per data dir) — see docs/DESIGN.md §6.
package engine

import (
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

const indexFilename = "db"

// fragNumReservation is how many fragNums one durable state.json reservation
// covers; only an allocation that crosses the high-water costs an fsync. Sized
// so a typical value fsyncs about every 10k Appends. Tunable, and not
// on-disk-format-critical.
const fragNumReservation = 1 << 20 // 1 048 576

// Store manages segment files and an index mapping keys to on-disk
// extents. All public methods are safe for concurrent use.
type Store struct {
	dir string

	// The on-disk index: key -> extent, plus the tombstone namespace.
	index *badger.DB

	// Unbounded: entries accumulate as the store touches old segments. Fine at
	// single-data-dir scale; thousands of distinct segments would want an LRU.
	segCache map[uint64]*segment

	// Guards metadata only — counters, segCache, index commits. Segment bytes
	// are pre-allocated via Truncate and written lock-free through WriteAt.
	mutex sync.Mutex

	// Shared by every reader and writer; GCM permits concurrent Seal/Open.
	aead cipher.AEAD

	// Intrinsic to the data dir, generated on first Open and bound into every
	// nonce.
	storeID uint32

	// Monotonic counters persisted to state.json across restarts.
	segNum   uint64
	valueNum uint64
	fragNum  uint64

	// The durably-reserved ceiling on fragNum; Append fsyncs only to raise it.
	fragNumHighWater uint64

	maxSegSize uint64

	compactionEnabled  bool
	compactionInterval time.Duration
	compactor          *compactor

	// Free-space watermark fractions gating Append; see WithFreeSpaceWatermark.
	nearfullFreeFrac float64
	fullFreeFrac     float64

	// statfs cache backing freeSpaceFraction; see statfsThrottleInterval.
	statfsAt   time.Time
	freeFrac   float64
	freeBytes  uint64
	totalBytes uint64

	// bytesSinceStatfs accumulates reserved extent bytes since the last
	// statfs; see statfsBytesInterval.
	bytesSinceStatfs uint64

	// Reported through Snapshot. Maintained rather than derived on demand: a
	// reporter must not scan the tombstone namespace or the segment directory
	// to answer, since it runs on a collection interval and would then charge
	// the store for being observed.
	stats storeStats

	closed bool
}

// storeStats is everything Snapshot reports that is not already a field on
// Store. Guarded by store.mutex, like the counters beside it.
type storeStats struct {
	// segments currently on disk, seeded by a single directory count at Open
	// and maintained by create and drop from then on.
	liveSegments int64

	// Occupancy as measured by the last compaction scan, which already sums
	// dead bytes per segment and stats every candidate. Reporting its numbers
	// costs nothing; recomputing them per collection would cost a full scan.
	// Zero scanAt means no cycle has run yet and neither figure is reported.
	scanAt    time.Time
	liveBytes int64
	deadBytes int64

	// Compaction totals, monotonic for the life of the process.
	cycles          int64
	cyclesFailed    int64
	segmentsScanned int64
	segmentsDropped int64
	bytesRelocated  int64
	bytesReclaimed  int64
	lastCycleSecs   float64
}

type Reader interface {
	io.ReadCloser
	io.ReaderAt
	io.WriterTo

	Size() int64

	// Epoch is the write epoch the value was stored under.
	Epoch() uint64
}

type Writer interface {
	io.WriteCloser
	io.ReaderFrom
}

var (
	ErrClosedStore = errors.New("closed store")
	ErrKeyNotFound = errors.New("key not found")
	ErrValueFull   = errors.New("value full")

	// ErrStoreFull is returned by Append when free space has dropped below
	// the full watermark (see WithFreeSpaceWatermark).
	ErrStoreFull = errors.New("store full")

	// ErrNotPrepared is returned by Commit when the key holds no prepared
	// extent under that epoch: the prepare never landed, it was aborted, or it
	// was aged out. It is distinct from a store error because the caller's
	// answer is to rewrite the shard, not to retry the commit.
	ErrNotPrepared = errors.New("no prepared extent under that epoch")
)

// Option configures a Store at Open time. Options may fail (e.g. invalid key
// length, cipher construction error); Open propagates the first non-nil error.
type Option func(*Store) error

// WithMaxSegSize overrides the segment-roll threshold. Primarily intended for
// tests that need to exercise rollover without writing 4 GiB of data.
func WithMaxSegSize(n uint64) Option {
	return func(s *Store) error {
		s.maxSegSize = n
		return nil
	}
}

// WithCompaction starts a background compactor that reclaims dead space on the
// given interval. A non-positive interval falls back to
// defaultCompactionInterval. Without this option, no compaction goroutine runs.
func WithCompaction(interval time.Duration) Option {
	return func(s *Store) error {
		s.compactionEnabled = true
		if interval > 0 {
			s.compactionInterval = interval
		}
		return nil
	}
}

// WithFreeSpaceWatermark overrides the free-space fractions that gate Append:
// crossing below nearfullFreeFrac kicks an immediate compaction pass but
// still accepts the write; crossing below fullFreeFrac rejects it with
// ErrStoreFull. fullFreeFrac must not exceed nearfullFreeFrac. Defaults to
// nearfull 0.15 / full 0.05 if unset.
func WithFreeSpaceWatermark(nearfullFreeFrac, fullFreeFrac float64) Option {
	return func(s *Store) error {
		if nearfullFreeFrac < 0 || nearfullFreeFrac > 1 || fullFreeFrac < 0 || fullFreeFrac > 1 {
			return fmt.Errorf("free-space watermark fractions must be in [0,1], got nearfull=%v full=%v", nearfullFreeFrac, fullFreeFrac)
		}
		if fullFreeFrac > nearfullFreeFrac {
			return fmt.Errorf("full watermark (%v) must not exceed nearfull watermark (%v)", fullFreeFrac, nearfullFreeFrac)
		}
		s.nearfullFreeFrac = nearfullFreeFrac
		s.fullFreeFrac = fullFreeFrac
		return nil
	}
}

// WithAEAD sets the cipher sealing every fragment, and is mandatory. The nonce
// layout fixes NonceSize at 12. Callers build the cipher themselves, so the
// store never sees raw key bytes.
func WithAEAD(aead cipher.AEAD) Option {
	return func(s *Store) error {
		if aead == nil {
			return errors.New("aead must not be nil")
		}
		if aead.NonceSize() != 12 {
			return fmt.Errorf("aead nonce size must be 12, got %d", aead.NonceSize())
		}
		s.aead = aead
		return nil
	}
}

// Open recovers or creates a Store in dir, leaving it ready for the first
// Append. WithAEAD is mandatory.
func Open(dir string, opts ...Option) (store *Store, err error) {
	store = &Store{
		dir:                dir,
		segCache:           make(map[uint64]*segment),
		maxSegSize:         DefaultMaxSegSize,
		compactionInterval: defaultCompactionInterval,
		nearfullFreeFrac:   defaultNearfullFreeFrac,
		fullFreeFrac:       defaultFullFreeFrac,
	}

	for _, opt := range opts {
		if err := opt(store); err != nil {
			return nil, err
		}
	}

	if store.aead == nil {
		return nil, errors.New("aead is required (use WithAEAD)")
	}

	if err := store.loadState(); err != nil {
		return nil, fmt.Errorf("load state: %w", err)
	}

	// Reserve a fresh window durably before Open returns. On a new data dir this
	// is also what locks in the generated storeID, before any fragment can be
	// sealed under it.
	store.fragNumHighWater = store.fragNum + fragNumReservation
	if err := store.saveState(); err != nil {
		return nil, fmt.Errorf("save state: %w", err)
	}

	store.index, err = openIndex(filepath.Join(dir, indexFilename))
	if err != nil {
		return nil, fmt.Errorf("open disk index: %w", err)
	}

	// Counted once here, then maintained by create and drop. Doing it at Open
	// is what keeps the reporting path free of a directory read.
	store.stats.liveSegments, err = countSegments(dir)
	if err != nil {
		return nil, fmt.Errorf("count segments: %w", err)
	}

	if _, err := store.nextAvailableSegment(); err != nil {
		return nil, err
	}

	if store.compactionEnabled {
		store.startCompactor()
	}

	slog.Info("store opened",
		"dir", dir,
		"storeID", fmt.Sprintf("%08x", store.storeID),
		"fragNumHighWater", store.fragNumHighWater,
	)

	return store, nil
}

// Snapshot is the store's capacity, occupancy and compaction state at one
// instant, for a caller that reports it. Every field is read from a maintained
// counter, so taking one costs a mutex acquisition and no I/O.
type Snapshot struct {
	// Free space as of the last statfs, which the write path refreshes; see
	// statfsThrottleInterval. Measured reports whether one has happened at all:
	// an idle store that has never appended has nothing to report yet, and zero
	// free bytes would read as a full disk.
	Measured   bool
	FreeFrac   float64
	FreeBytes  uint64
	TotalBytes uint64

	// Pressure is the watermark band the free fraction falls in: "ok",
	// "nearfull" or "full", by the same comparison Append gates on.
	Pressure string

	// Monotonic counters persisted across restarts, so write volume comes out
	// of them without counting anything on the write path.
	SegNum   uint64
	ValueNum uint64
	FragNum  uint64

	LiveSegments int64

	// Occupancy as of the last compaction scan. Scanned is false until one has
	// run: a store with compaction disabled never reports occupancy at all,
	// which is honest, where a zero would read as a store holding no dead data.
	Scanned   bool
	LiveBytes int64
	DeadBytes int64

	CompactionCycles       int64
	CompactionCyclesFailed int64
	SegmentsScanned        int64
	SegmentsDropped        int64
	BytesRelocated         int64
	BytesReclaimed         int64
	LastCycleSeconds       float64

	// IntegrityFailures counts fragments that failed their GCM tag since the
	// process started. Non-zero means bytes on disk no longer authenticate,
	// which is corruption rather than a routine error.
	IntegrityFailures uint64
}

// Pressure bands, matching the watermarks Append gates on.
const (
	PressureOK       = "ok"
	PressureNearFull = "nearfull"
	PressureFull     = "full"
)

// Snapshot reports the store's current capacity and compaction state. It takes
// no statfs and scans nothing: an idle store answers from the last measurement
// the write path took, and reports Measured false until there has been one.
func (store *Store) Snapshot() Snapshot {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	s := Snapshot{
		Measured:               !store.statfsAt.IsZero(),
		FreeFrac:               store.freeFrac,
		FreeBytes:              store.freeBytes,
		TotalBytes:             store.totalBytes,
		SegNum:                 store.segNum,
		ValueNum:               store.valueNum,
		FragNum:                store.fragNum,
		LiveSegments:           store.stats.liveSegments,
		Scanned:                !store.stats.scanAt.IsZero(),
		LiveBytes:              store.stats.liveBytes,
		DeadBytes:              store.stats.deadBytes,
		CompactionCycles:       store.stats.cycles,
		CompactionCyclesFailed: store.stats.cyclesFailed,
		SegmentsScanned:        store.stats.segmentsScanned,
		SegmentsDropped:        store.stats.segmentsDropped,
		BytesRelocated:         store.stats.bytesRelocated,
		BytesReclaimed:         store.stats.bytesReclaimed,
		LastCycleSeconds:       store.stats.lastCycleSecs,
		IntegrityFailures:      integrityFailures.Load(),
	}

	s.Pressure = PressureOK
	switch {
	case !s.Measured:
	case s.FreeFrac < store.fullFreeFrac:
		s.Pressure = PressureFull
	case s.FreeFrac < store.nearfullFreeFrac:
		s.Pressure = PressureNearFull
	}

	return s
}

// Lookup returns a reader for the given key. The underlying segment is
// reference-counted: the caller must call reader.Close() to release it.
func (store *Store) Lookup(key [32]byte, index uint32) (Reader, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	if store.closed {
		return nil, ErrClosedStore
	}

	idxKey := MakeKey(key, index)
	data, err := store.indexGet(idxKey)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, ErrKeyNotFound
		}

		return nil, fmt.Errorf("get extent: %w", err)
	}

	ext, epoch, err := decodeIndexValue(data)
	if err != nil {
		return nil, fmt.Errorf("decode extent: %w", err)
	}

	// Read path: a segment the index still points at must exist; report it
	// missing rather than fabricating a stub.
	seg, err := store.getSegment(ext.SegNum, false)
	if err != nil {
		return nil, fmt.Errorf("get segment %d: %w", ext.SegNum, err)
	}

	seg.addRef()

	buf, held := takeFragWindow(ext.PSize / totalFragSize)

	return &reader{
		key:     key,
		index:   index,
		epoch:   epoch,
		storeID: store.storeID,
		aead:    store.aead,
		seg:     seg,
		ext:     ext,
		buf:     buf,
		held:    held,
	}, nil
}

// Append reserves space for a value of the given logical size and returns a
// writer. Closing that writer prepares the extent rather than publishing it,
// so an overwrite keeps serving its previous data until Commit is called.
//
// epoch is opaque here: the engine stores it, returns it on Lookup and matches
// it on Commit, and never orders or interprets it.
func (store *Store) Append(key [32]byte, index uint32, size int64, epoch uint64) (Writer, error) {
	if size < 0 {
		return nil, fmt.Errorf("negative size %d", size)
	}

	store.mutex.Lock()
	defer store.mutex.Unlock()

	if store.closed {
		return nil, ErrClosedStore
	}

	// Check the watermark before reserving any extent so a full store fails
	// fast. Treat a statfs error as permissive — a monitoring hiccup must not
	// itself take writes down.
	if frac, err := store.freeSpaceFraction(); err != nil {
		slog.Warn("free-space check failed, proceeding without a watermark decision", "dir", store.dir, "error", err)
	} else if frac < store.fullFreeFrac {
		return nil, ErrStoreFull
	} else if frac < store.nearfullFreeFrac {
		store.kickCompaction()
	}

	// Ceiling division; size == 0 yields an empty extent the writer never fills.
	fragCount := (uint64(size) + fragBodySize - 1) / fragBodySize

	// fragNums must be durable before they are handed out. A crash that rewound
	// fragNum would reissue a nonce under the same key, which breaks GCM
	// catastrophically, so this fsync is not optional.
	if store.fragNum+fragCount > store.fragNumHighWater {
		for store.fragNumHighWater < store.fragNum+fragCount {
			store.fragNumHighWater += fragNumReservation
		}
		if err := store.saveState(); err != nil {
			return nil, fmt.Errorf("advance fragNum high-water: %w", err)
		}
	}

	seg, off, err := store.reserveExtent(fragCount)
	if err != nil {
		return nil, err
	}

	seg.addRef()

	ext := extent{
		SegNum: store.segNum,
		Off:    off,
		PSize:  int64(fragCount) * totalFragSize, //nolint:gosec // fragCount derived from non-negative int64 size.
		LSize:  size,
	}

	idxKey := MakeKey(key, index)
	if err := seg.appendIdx(idxEntry{Off: off, Key: [36]byte(idxKey), PSize: ext.PSize}); err != nil {
		seg.releaseRef()
		return nil, fmt.Errorf("append idx: %w", err)
	}

	wbuf, wheld := takeFragWindow(int64(fragCount)) //nolint:gosec // G115: fragment count of a bounded extent.

	w := &writer{
		store:    store,
		key:      key,
		index:    index,
		epoch:    epoch,
		storeID:  store.storeID,
		seg:      seg,
		ext:      ext,
		valueNum: store.valueNum,
		fragNum:  store.fragNum,
		buf:      wbuf,
		held:     wheld,
	}

	store.valueNum += 1
	store.fragNum += fragCount

	// Count toward the statfs byte bound; see statfsBytesInterval.
	store.bytesSinceStatfs += uint64(ext.PSize) //nolint:gosec // PSize is a non-negative on-disk byte count.

	return w, nil
}

// reserveExtent locates a segment with capacity for fragCount fragments,
// pre-allocates the extent via Truncate, and returns the segment and the
// in-segment offset where the writer should start writing. Callers must hold
// store.mutex.
func (store *Store) reserveExtent(fragCount uint64) (*segment, int64, error) {
	seg, err := store.nextAvailableSegment()
	if err != nil {
		return nil, 0, err
	}

	info, err := seg.Stat()
	if err != nil {
		return nil, 0, fmt.Errorf("stat segment %d: %w", store.segNum, err)
	}
	segSize := info.Size()

	newSegSize := uint64(segSize) + fragCount*totalFragSize //nolint:gosec // segSize from os.File.Stat().Size() is always non-negative.

	if newSegSize >= store.maxSegSize {
		// Non-fatal: the next Append re-reads the on-disk flag and retries the mark.
		if err := seg.markFull(); err != nil {
			slog.Warn("failed to mark segment full",
				"segNum", store.segNum,
				"error", err,
			)
		}
	}

	// Roll if the value would overflow, unless the segment is fresh: one
	// oversized value is let through so a pathological size still makes progress.
	if newSegSize > store.maxSegSize && segSize != segHeaderSize {
		seg, err = store.rollSegment()
		if err != nil {
			return nil, 0, fmt.Errorf("roll to next segment: %w", err)
		}
		info, err = seg.Stat()
		if err != nil {
			return nil, 0, fmt.Errorf("stat segment %d: %w", store.segNum, err)
		}
		segSize = info.Size()
	}

	off := segSize
	if err := seg.Truncate(off + totalFragSize*int64(fragCount)); err != nil { //nolint:gosec // fragCount bounded by non-negative int64 size.
		return nil, 0, fmt.Errorf("truncate segment %d: %w", store.segNum, err)
	}

	return seg, off, nil
}

// prepareExtent records a durable extent that is not yet the value's data.
// Called from writer.Close; takes no store.mutex.
//
// The row goes into badger rather than being left implicit, and that is the
// whole point: compaction decides a .idx row is dead when no badger key points
// at it, so an extent held only in memory between prepare and commit would be
// dropped out from under the commit. A prepared row makes it live enough to be
// relocated instead.
func (store *Store) prepareExtent(key [32]byte, index uint32, ext extent, epoch uint64) error {
	value := encodePreparedValue(ext, epoch, time.Now().UnixNano())
	if err := store.index.Update(func(txn *badger.Txn) error {
		return txn.Set(preparedKey(MakeKey(key, index)), value)
	}); err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	return nil
}

// Commit publishes a prepared extent as the value's data, tombstoning whatever
// it supersedes. It is idempotent against the epoch: a retry after the row has
// already been published finds the live row already at epoch and reports
// success rather than failing a write that did land.
//
// A prepared row under a different epoch is left alone. Two writers racing the
// same key each prepare under their own epoch, and neither may commit the
// other's bytes.
func (store *Store) Commit(key [32]byte, index uint32, epoch uint64) error {
	idxKey := MakeKey(key, index)
	prepKey := preparedKey(idxKey)

	for {
		err := store.index.Update(func(txn *badger.Txn) error {
			raw, err := readRaw(txn, prepKey)
			switch {
			case errors.Is(err, badger.ErrKeyNotFound):
				// Nothing prepared. Either this is a duplicate commit of a row
				// already published, which is success, or the prepare never
				// happened, which is not.
				live, liveEpoch, liveErr := readIndexValue(txn, idxKey)
				if liveErr == nil && liveEpoch == epoch {
					_ = live
					return nil
				}
				return ErrNotPrepared
			case err != nil:
				return err
			}

			ext, prepared, err := decodeIndexValue(raw)
			if err != nil {
				return fmt.Errorf("decode prepared: %w", err)
			}
			if prepared != epoch {
				return ErrNotPrepared
			}

			// The superseded extent dies at this commit and nowhere else, so its
			// hint rides the same txn and can neither precede nor outlive it.
			old, _, err := readIndexValue(txn, idxKey)
			switch {
			case errors.Is(err, badger.ErrKeyNotFound): // a first write supersedes nothing
			case err != nil:
				return err
			default:
				if err := txn.Set(tombstoneKey(old.SegNum, old.Off), tombstoneValue(old.PSize)); err != nil {
					return fmt.Errorf("put tombstone: %w", err)
				}
			}

			if err := txn.Set(idxKey, encodeIndexValue(ext, epoch)); err != nil {
				return err
			}
			return txn.Delete(prepKey)
		})

		// The reads above put this txn under badger's conflict detection, which a
		// compactor relocating the same key trips.
		if errors.Is(err, badger.ErrConflict) {
			continue
		}
		if err != nil {
			if errors.Is(err, ErrNotPrepared) {
				return err
			}
			return fmt.Errorf("commit: %w", err)
		}
		return nil
	}
}

// Abort discards a prepared extent. The tombstone is what makes the dead space
// advertise itself: candidateSegments ranks by summed tombstone bytes, so an
// abort that wrote none would leave the space unreclaimed until something else
// happened to die in the same segment.
//
// Aborting something never prepared is not an error: the caller is telling the
// store to make sure nothing is pending, and it is not.
func (store *Store) Abort(key [32]byte, index uint32, epoch uint64) error {
	return store.dropPrepared(preparedKey(MakeKey(key, index)), func(prepared uint64) bool {
		return prepared == epoch
	})
}

// dropPrepared removes a prepared row when want accepts its epoch, tombstoning
// the extent it reserved.
func (store *Store) dropPrepared(prepKey []byte, want func(epoch uint64) bool) error {
	for {
		err := store.index.Update(func(txn *badger.Txn) error {
			raw, err := readRaw(txn, prepKey)
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			ext, prepared, err := decodeIndexValue(raw)
			if err != nil {
				return fmt.Errorf("decode prepared: %w", err)
			}
			if !want(prepared) {
				return nil
			}
			if err := txn.Set(tombstoneKey(ext.SegNum, ext.Off), tombstoneValue(ext.PSize)); err != nil {
				return fmt.Errorf("put tombstone: %w", err)
			}
			return txn.Delete(prepKey)
		})
		if errors.Is(err, badger.ErrConflict) {
			continue
		}
		if err != nil {
			return fmt.Errorf("abort: %w", err)
		}
		return nil
	}
}

// readRaw returns a key's value bytes, propagating badger.ErrKeyNotFound
// unwrapped so callers can branch on a missing key.
func readRaw(txn *badger.Txn, key []byte) ([]byte, error) {
	item, err := txn.Get(key)
	if err != nil {
		return nil, err
	}
	raw, err := item.ValueCopy(nil)
	if err != nil {
		return nil, fmt.Errorf("copy value: %w", err)
	}
	return raw, nil
}

// readIndexValue decodes the extent and epoch a key currently points at.
func readIndexValue(txn *badger.Txn, key []byte) (extent, uint64, error) {
	raw, err := readRaw(txn, key)
	if err != nil {
		return extent{}, 0, err
	}
	return decodeIndexValue(raw)
}

// Delete removes a key's index entry and tombstones its extent, in one
// transaction so the dead-space hint cannot outlive or precede the deletion.
// Reports whether an extent existed; a missing key is not an error, which
// keeps deletes idempotent.
func (store *Store) Delete(key [32]byte, index uint32) (bool, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	if store.closed {
		return false, ErrClosedStore
	}

	idxKey := MakeKey(key, index)
	deleted := false
	err := store.index.Update(func(txn *badger.Txn) error {
		// A prepared row for this key would otherwise survive the delete and
		// resurrect the value on a later commit.
		if raw, perr := readRaw(txn, preparedKey(idxKey)); perr == nil {
			pext, _, derr := decodeIndexValue(raw)
			if derr != nil {
				return fmt.Errorf("delete: decode prepared: %w", derr)
			}
			if err := txn.Set(tombstoneKey(pext.SegNum, pext.Off), tombstoneValue(pext.PSize)); err != nil {
				return fmt.Errorf("delete: tombstone prepared: %w", err)
			}
			if err := txn.Delete(preparedKey(idxKey)); err != nil {
				return err
			}
		} else if !errors.Is(perr, badger.ErrKeyNotFound) {
			return fmt.Errorf("delete: read prepared: %w", perr)
		}

		ext, _, err := readIndexValue(txn, idxKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("delete: read extent: %w", err)
		}

		if err := txn.Set(tombstoneKey(ext.SegNum, ext.Off), tombstoneValue(ext.PSize)); err != nil {
			return fmt.Errorf("delete: put tombstone: %w", err)
		}
		if err := txn.Delete(idxKey); err != nil {
			return err
		}
		deleted = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return deleted, nil
}

// Close blocks until all outstanding segment references drain, then closes
// segment file descriptors and the index. Must be called exactly once.
func (store *Store) Close() error {
	store.mutex.Lock()
	if store.closed {
		store.mutex.Unlock()
		return ErrClosedStore
	}
	store.closed = true
	c := store.compactor
	store.mutex.Unlock()

	// Join without the mutex: an in-flight cycle takes it, so joining under the
	// lock would deadlock. closed is already set, so nothing new slips in behind.
	if c != nil {
		c.stop()
	}

	store.mutex.Lock()
	defer store.mutex.Unlock()

	var errs []error

	if err := store.saveState(); err != nil {
		errs = append(errs, fmt.Errorf("save state: %w", err))
	}

	for num, seg := range store.segCache {
		seg.waitForRefs()

		if err := seg.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close segment %d: %w", num, err))
		}
	}

	if err := store.index.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close disk index: %w", err))
	}

	return errors.Join(errs...)
}

// Tombstones record a dead extent as d ‖ BE(segNum) ‖ BE(off) → BE(PSize). They
// only accelerate compaction's candidate selection and are never consulted for
// correctness. Keying by physical slot rather than object key is what lets one
// key die repeatedly: on delete, on overwrite, and on a relocation that lost
// its race.
const tombstonePrefix = 'd'

const tombstoneKeySize = 17 // prefix(1) ‖ segNum(8) ‖ off(8)

func tombstoneKey(segNum uint64, off int64) []byte {
	key := make([]byte, tombstoneKeySize)
	key[0] = tombstonePrefix
	binary.BigEndian.PutUint64(key[1:9], segNum)
	binary.BigEndian.PutUint64(key[9:17], uint64(off)) //nolint:gosec // round-trips bit-for-bit via int64 cast on decode.
	return key
}

func tombstoneSegNum(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[1:9])
}

func tombstoneValue(psize int64) []byte {
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, uint64(psize)) //nolint:gosec // PSize is a non-negative on-disk byte count.
	return v
}

// Prepared extents live at p ‖ MakeKey(...), 37 bytes against the live row's
// 36 and the tombstone namespace's 17, so all three are told apart by width
// alone — the same discrimination the rest of the index already makes.
const preparedPrefix = 'p'

const preparedKeySize = 37

func preparedKey(idxKey []byte) []byte {
	key := make([]byte, 0, preparedKeySize)
	return append(append(key, preparedPrefix), idxKey...)
}

// MakeKey builds a 36-byte index key: the 32-byte key || 4-byte big-endian index.
func MakeKey(key [32]byte, index uint32) []byte {
	idxKey := make([]byte, 36)
	copy(idxKey[:32], key[:])
	binary.BigEndian.PutUint32(idxKey[32:], index)

	return idxKey
}
