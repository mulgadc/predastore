// Package store implements segment-based shard storage with authenticated
// at-rest encryption. Shards are written as contiguous extents of fixed-size
// fragments within append-only segment files. Segments roll when they reach
// maxSegSize. Each fragment is sealed under AES-256-GCM (master key per
// cluster, storeID per data dir) — see docs/DESIGN.md §6.
package store

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
	"github.com/mulgadc/predastore/s3db"
)

const indexFilename = "db"

// fragNumReservation is the size of each batched fragNum allocation written
// durably to state.json. Append hands out fragNums freely below the persisted
// high-water; only an allocation that crosses the high-water triggers an
// fsync. Sized so a typical 100-fragment shard fsyncs roughly every 10k
// Append calls. Tunable; not on-disk-format-critical.
const fragNumReservation = 1 << 20 // 1 048 576

// Store manages segment files and an index mapping shard keys to on-disk extents.
// All public methods are safe for concurrent use. Segment files are pre-allocated
// via Truncate and written lock-free with WriteAt; the mutex protects only metadata
// (counters, segment cache, index commits).
//
// segCache is unbounded — entries accumulate as the store touches old
// segments. Fine for the current single-data-dir scale; a long-lived store
// reading from thousands of distinct segments will need an LRU.
type Store struct {
	dir      string
	index    *s3db.S3DB
	segCache map[uint64]*segment
	mutex    sync.Mutex

	// At-rest encryption: aead is supplied by the caller via WithAEAD and
	// shared (concurrency-safe per stdlib contract) across every shardWriter
	// and shardReader. The store never sees raw key bytes — cipher construction
	// lives at the layer that loads the key file. storeID is intrinsic per
	// data dir, generated on first Open and bound into every nonce.
	aead    cipher.AEAD
	storeID uint32

	// Monotonic counters persisted to state.json across restarts.
	// fragNumHighWater is the durably-reserved upper bound on fragNum; Append
	// extends it (and fsyncs) only when an allocation would cross it.
	segNum           uint64
	shardNum         uint64
	fragNum          uint64
	fragNumHighWater uint64

	maxSegSize uint64

	compactionEnabled  bool
	compactionInterval time.Duration
	compactor          *compactor

	closed bool
}

type Reader interface {
	io.ReadCloser
	io.ReaderAt
	io.WriterTo

	Size() int64
}

type Writer interface {
	io.WriteCloser
	io.ReaderFrom
}

var (
	ErrClosedStore = errors.New("closed store")
	ErrKeyNotFound = errors.New("key not found")
	ErrShardFull   = errors.New("shard full")
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

// WithAEAD configures the cipher.AEAD used to seal/open every fragment. The
// AEAD is shared across all writers and readers; the stdlib's GCM guarantees
// concurrent Seal/Open are safe. The store builds 12-byte nonces as
// BE(fragNum) || BE(storeID), so the AEAD must report NonceSize() == 12.
//
// Cipher construction (and the raw key it consumes) live at the operator
// layer — the store deliberately never sees key bytes.
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

// Open recovers or creates a Store in dir. On startup it restores monotonic
// counters from state.json (or generates a fresh storeID for a new data dir),
// advances and durably persists the fragNum high-water reservation, opens the
// index, then advances segNum past any full segments left over from a prior
// crash so the first Append sees a writable segment.
//
// WithAEAD is mandatory — Open errors if no cipher was supplied.
func Open(dir string, opts ...Option) (store *Store, err error) {
	store = &Store{
		dir:                dir,
		segCache:           make(map[uint64]*segment),
		maxSegSize:         DefaultMaxSegSize,
		compactionInterval: defaultCompactionInterval,
	}

	for _, opt := range opts {
		if err := opt(store); err != nil {
			return nil, err
		}
	}

	if store.aead == nil {
		return nil, errors.New("store: aead is required (use WithAEAD)")
	}

	if err := store.loadState(); err != nil {
		return nil, fmt.Errorf("load state: %w", err)
	}

	// Reserve a fresh high-water window. For fresh stores this also locks in
	// the just-generated storeID before any fragment can be sealed under it.
	// The save MUST be durable before Open returns — see plan §"first save of
	// state.json".
	store.fragNumHighWater = store.fragNum + fragNumReservation
	if err := store.saveState(); err != nil {
		return nil, fmt.Errorf("save state: %w", err)
	}

	store.index, err = s3db.New(filepath.Join(dir, indexFilename))
	if err != nil {
		return nil, fmt.Errorf("open disk index: %w", err)
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

// Lookup returns a reader for the given shard. The underlying segment is
// reference-counted: the caller must call reader.Close() to release it.
func (store *Store) Lookup(objectHash [32]byte, shardIndex uint32) (Reader, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	if store.closed {
		return nil, ErrClosedStore
	}

	key := MakeShardKey(objectHash, shardIndex)
	data, err := store.index.Get(key)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, ErrKeyNotFound
		}

		return nil, fmt.Errorf("get extent: %w", err)
	}

	ext, err := decodeExtent(data)
	if err != nil {
		return nil, fmt.Errorf("decode extent: %w", err)
	}

	seg, err := store.getSegment(ext.SegNum)
	if err != nil {
		return nil, fmt.Errorf("get segment %d: %w", ext.SegNum, err)
	}

	seg.addRef()

	return &reader{
		objectHash: objectHash,
		shardIndex: shardIndex,
		storeID:    store.storeID,
		aead:       store.aead,
		seg:        seg,
		ext:        ext,
		buf:        make([]byte, min(int64(bufLen), ext.PSize/totalFragSize)*totalFragSize),
	}, nil
}

// Append reserves space for a shard of the given logical size and returns a
// writer. The segment file is pre-allocated (Truncated) to fit all fragments,
// so subsequent WriteAt calls from the writer never extend the file. If the
// current segment can't fit the shard, it is marked full and the store rolls
// to the next segment. The index entry is committed only when the writer is
// closed.
func (store *Store) Append(objectHash [32]byte, shardIndex uint32, size int64) (Writer, error) {
	if size < 0 {
		return nil, fmt.Errorf("negative size %d", size)
	}

	store.mutex.Lock()
	defer store.mutex.Unlock()

	if store.closed {
		return nil, ErrClosedStore
	}

	// Ceiling division: number of fragments needed to hold size logical bytes.
	// size == 0 yields fragCount == 0; the writer's ReadFrom loop never runs
	// and Close commits an empty-extent index entry.
	fragCount := (uint64(size) + fragBodySize - 1) / fragBodySize

	// Reserve fragNums durably before handing them out. We must fsync before
	// any fragment can be sealed under (storeID, fragNum) — otherwise a crash
	// after the writer returns could rewind fragNum on restart and cause
	// catastrophic GCM nonce reuse.
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

	key := MakeShardKey(objectHash, shardIndex)
	if err := seg.appendIdx(idxEntry{Off: off, Key: [36]byte(key), PSize: ext.PSize}); err != nil {
		seg.releaseRef()
		return nil, fmt.Errorf("append idx: %w", err)
	}

	w := &writer{
		store:      store,
		objectHash: objectHash,
		shardIndex: shardIndex,
		storeID:    store.storeID,
		seg:        seg,
		ext:        ext,
		shardNum:   store.shardNum,
		fragNum:    store.fragNum,
		buf:        make([]byte, min(uint64(bufLen), fragCount)*totalFragSize),
	}

	store.shardNum += 1
	store.fragNum += fragCount

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
		// Mark full once we've decided this is the last shard for this segment.
		// markFull failure is non-fatal: next Append re-reads the on-disk flag
		// (cache stays as-is here too), so we'll retry the mark naturally.
		if err := seg.markFull(); err != nil {
			slog.Warn("failed to mark segment full",
				"segNum", store.segNum,
				"error", err,
			)
		}
	}

	// Roll if this shard would overflow the segment, unless the segment is
	// fresh (header-only) — then we let one oversized shard through so a
	// pathological size still makes forward progress.
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

// commitExtent persists a shard's extent to the index. Called by writers from
// Close once the data is durable. Concurrency-safe via badger; no Store-level
// lock needed.
//
// An overwrite supersedes whatever extent the key already held, so the old slot
// becomes dead space. Its tombstone is written in the same transaction that
// repoints the key — the commit is the instant the supersession happens, so the
// dead-space hint can neither precede nor outlive it. Without this the orphaned
// bytes stay invisible to compaction's selector and mask their segment from
// candidacy forever.
func (store *Store) commitExtent(objectHash [32]byte, shardIndex uint32, ext extent) error {
	key := MakeShardKey(objectHash, shardIndex)

	for {
		err := store.index.Badger.Update(func(txn *badger.Txn) error {
			old, err := readExtent(txn, key)
			switch {
			// A first write has nothing to supersede.
			case errors.Is(err, badger.ErrKeyNotFound):
			case err != nil:
				return err
			default:
				if err := txn.Set(tombstoneKey(old.SegNum, old.Off), tombstoneValue(old.PSize)); err != nil {
					return fmt.Errorf("put tombstone: %w", err)
				}
			}

			return txn.Set(key, ext.encode())
		})

		// Reading the key before writing it puts this txn under badger's conflict
		// detection: a compactor relocating the same key races us here.
		if errors.Is(err, badger.ErrConflict) {
			continue
		}
		if err != nil {
			return fmt.Errorf("commit: %w", err)
		}
		return nil
	}
}

// readExtent decodes the extent a key currently points at, propagating
// badger.ErrKeyNotFound unwrapped so callers can branch on a missing key.
func readExtent(txn *badger.Txn, key []byte) (extent, error) {
	item, err := txn.Get(key)
	if err != nil {
		return extent{}, err
	}
	raw, err := item.ValueCopy(nil)
	if err != nil {
		return extent{}, fmt.Errorf("copy extent: %w", err)
	}
	ext, err := decodeExtent(raw)
	if err != nil {
		return extent{}, fmt.Errorf("decode extent: %w", err)
	}
	return ext, nil
}

// Delete removes the index entry for a shard in a single index transaction:
// read the current extent, write a slot-keyed tombstone for compaction's
// selector, then delete the live key. Tombstone and deletion commit together,
// so the dead-space hint can never outlive or precede the deletion it records.
// The on-disk extent becomes dead space reclaimable by the compactor.
// Delete tombstones the extent for the given shard and removes its index entry.
// The bool reports whether an extent existed and was removed; a missing shard is
// not an error and returns (false, nil), keeping deletes idempotent.
func (store *Store) Delete(objectHash [32]byte, shardIndex uint32) (bool, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	if store.closed {
		return false, ErrClosedStore
	}

	key := MakeShardKey(objectHash, shardIndex)
	deleted := false
	err := store.index.Badger.Update(func(txn *badger.Txn) error {
		ext, err := readExtent(txn, key)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("delete: read extent: %w", err)
		}

		if err := txn.Set(tombstoneKey(ext.SegNum, ext.Off), tombstoneValue(ext.PSize)); err != nil {
			return fmt.Errorf("delete: put tombstone: %w", err)
		}
		if err := txn.Delete(key); err != nil {
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

	// Stop the compactor without holding the mutex: an in-flight cycle takes
	// store.mutex, so joining under the lock would deadlock. closed is already
	// set, so no new public mutation slips in behind the join.
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

// Slot-keyed tombstones live in the index under the tombstonePrefix namespace:
// Delete writes d ‖ BE(segNum) ‖ BE(off) → BE(PSize) for the extent it removes.
// They are a candidate-selection accelerator for compaction only — never
// consulted for correctness. Keyed by physical slot so the same object key
// dying more than once stays unique.
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

// MakeShardKey builds a 36-byte index key: 32-byte object hash || 4-byte big-endian shard index.
func MakeShardKey(objectHash [32]byte, shardIndex uint32) []byte {
	key := make([]byte, 36)
	copy(key[:32], objectHash[:])
	binary.BigEndian.PutUint32(key[32:], shardIndex)

	return key
}
