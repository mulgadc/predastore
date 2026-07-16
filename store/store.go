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

// fragNumReservation is how many fragNums one durable state.json reservation
// covers; only an allocation that crosses the high-water costs an fsync. Sized
// so a typical shard fsyncs about every 10k Appends. Tunable, and not
// on-disk-format-critical.
const fragNumReservation = 1 << 20 // 1 048 576

// Store manages segment files and an index mapping shard keys to on-disk
// extents. All public methods are safe for concurrent use.
type Store struct {
	dir   string
	index *s3db.S3DB

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
	shardNum uint64
	fragNum  uint64

	// The durably-reserved ceiling on fragNum; Append fsyncs only to raise it.
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

	// Reserve a fresh window durably before Open returns. On a new data dir this
	// is also what locks in the generated storeID, before any fragment can be
	// sealed under it.
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

	// Read path: a segment the index still points at must exist; report it
	// missing rather than fabricating a stub.
	seg, err := store.getSegment(ext.SegNum, false)
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
// writer. The shard reaches the index only when that writer is closed, so an
// overwrite keeps serving its previous data until then.
func (store *Store) Append(objectHash [32]byte, shardIndex uint32, size int64) (Writer, error) {
	if size < 0 {
		return nil, fmt.Errorf("negative size %d", size)
	}

	store.mutex.Lock()
	defer store.mutex.Unlock()

	if store.closed {
		return nil, ErrClosedStore
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
		// Non-fatal: the next Append re-reads the on-disk flag and retries the mark.
		if err := seg.markFull(); err != nil {
			slog.Warn("failed to mark segment full",
				"segNum", store.segNum,
				"error", err,
			)
		}
	}

	// Roll if the shard would overflow, unless the segment is fresh: one
	// oversized shard is let through so a pathological size still makes progress.
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

// commitExtent points a shard's key at ext, tombstoning any extent it
// supersedes. Called from writer.Close once the data is durable; takes no
// store.mutex.
func (store *Store) commitExtent(objectHash [32]byte, shardIndex uint32, ext extent) error {
	key := MakeShardKey(objectHash, shardIndex)

	for {
		err := store.index.Badger.Update(func(txn *badger.Txn) error {
			old, err := readExtent(txn, key)
			switch {
			// A first write supersedes nothing.
			case errors.Is(err, badger.ErrKeyNotFound):
			case err != nil:
				return err
			default:
				// The old extent dies at this commit and nowhere else, so its hint
				// rides the same txn and can neither precede nor outlive it.
				if err := txn.Set(tombstoneKey(old.SegNum, old.Off), tombstoneValue(old.PSize)); err != nil {
					return fmt.Errorf("put tombstone: %w", err)
				}
			}

			return txn.Set(key, ext.encode())
		})

		// The read above puts this txn under badger's conflict detection, which a
		// compactor relocating the same key trips.
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

// Delete removes a shard's index entry and tombstones its extent, in one
// transaction so the dead-space hint cannot outlive or precede the deletion.
// Reports whether an extent existed; a missing shard is not an error, which
// keeps deletes idempotent.
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

// MakeShardKey builds a 36-byte index key: 32-byte object hash || 4-byte big-endian shard index.
func MakeShardKey(objectHash [32]byte, shardIndex uint32) []byte {
	key := make([]byte, 36)
	copy(key[:32], objectHash[:])
	binary.BigEndian.PutUint32(key[32:], shardIndex)

	return key
}
