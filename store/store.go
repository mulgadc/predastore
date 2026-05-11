// Package store implements segment-based shard storage with authenticated
// at-rest encryption. Shards are written as contiguous extents of fixed-size
// fragments within append-only segment files. Segments roll when they reach
// maxSegSize. Each fragment is sealed under AES-256-GCM (master key per
// cluster, storeID per data dir) — see docs/development/feature/aes-256-encryption-at-rest.md.
package store

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/mulgadc/predastore/s3db"
	"github.com/mulgadc/predastore/store/crypto"
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
type Store struct {
	dir      string
	index    *s3db.S3DB
	segCache map[uint64]*segment
	mutex    sync.Mutex

	// At-rest encryption: masterKey/aead are built once at Open from
	// WithMasterKey and shared (concurrency-safe per stdlib contract) across
	// every shardWriter and shardReader. storeID is intrinsic per data dir,
	// generated on first Open and bound into every nonce.
	masterKey []byte
	aead      cipher.AEAD
	storeID   uint32

	// Monotonic counters persisted to state.json across restarts.
	// fragNumHighWater is the durably-reserved upper bound on fragNum; Append
	// extends it (and fsyncs) only when an allocation would cross it.
	segNum           uint64
	shardNum         uint64
	fragNum          uint64
	fragNumHighWater uint64

	maxSegSize uint64

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

// WithMasterKey configures the AES-256 master key used to seal/open every
// fragment. The key must be exactly 32 bytes. The cipher.AEAD built here is
// shared across all writers and readers; the stdlib guarantees concurrent
// Seal/Open are safe.
func WithMasterKey(key []byte) Option {
	return func(s *Store) error {
		if len(key) != crypto.MasterKeySize {
			return fmt.Errorf("master key must be %d bytes, got %d", crypto.MasterKeySize, len(key))
		}
		block, err := aes.NewCipher(key)
		if err != nil {
			return fmt.Errorf("create AES cipher: %w", err)
		}
		aead, err := cipher.NewGCM(block)
		if err != nil {
			return fmt.Errorf("create GCM: %w", err)
		}
		s.masterKey = key
		s.aead = aead
		return nil
	}
}

// Open recovers or creates a Store in dir. On startup it restores monotonic
// counters from state.json (or generates a fresh storeID for a new data dir),
// advances and durably persists the fragNum high-water reservation, opens the
// index, then finds a non-full segment to write into.
//
// WithMasterKey is mandatory — Open errors if no key was supplied.
func Open(dir string, opts ...Option) (store *Store, err error) {
	store = &Store{
		dir:        dir,
		segCache:   make(map[uint64]*segment),
		maxSegSize: DefaultMaxSegSize,
	}

	for _, opt := range opts {
		if err := opt(store); err != nil {
			return nil, err
		}
	}

	if store.aead == nil {
		return nil, errors.New("store: master key is required (use WithMasterKey)")
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

	_, err = store.getNextSegment(100, func(*segment) error { return nil })
	if err != nil {
		return nil, err
	}

	slog.Info("store opened",
		"dir", dir,
		"storeID", fmt.Sprintf("%08x", store.storeID),
		"keyFingerprint", crypto.Fingerprint(store.masterKey),
		"fragNumHighWater", store.fragNumHighWater,
	)

	return store, nil
}

// Lookup returns a reader for the given shard. The underlying segment is
// reference-counted: the caller must call reader.Close() to release it.
//
// 0-byte shards short-circuit: the recorded extent has LSize==0 and no
// segment fragment was reserved, so we return a reader that yields zero
// bytes without touching any segment file.
func (store *Store) Lookup(objectHash [32]byte, shardIndex uint32) (reader Reader, err error) {
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

	extent, err := decodeExtent(data)
	if err != nil {
		return nil, fmt.Errorf("decode extent: %w", err)
	}

	if extent.LSize == 0 {
		return &shardReader{
			objectHash: objectHash,
			shardIndex: shardIndex,
			storeID:    store.storeID,
			ext:        extent,
		}, nil
	}

	segment, err := store.getSegment(extent.SegNum)
	if err != nil {
		return nil, fmt.Errorf("get segment %d: %w", extent.SegNum, err)
	}

	segment.refs.Add(1)

	reader = &shardReader{
		objectHash: objectHash,
		shardIndex: shardIndex,
		storeID:    store.storeID,
		seg:        segment,
		ext:        extent,
		buf:        make([]byte, readBufLen*totalFragSize),
		bufPos:     0,
		closed:     false,
	}

	return reader, nil
}

// Append reserves space for a shard of the given logical size and returns a
// writer. The segment file is pre-allocated (Truncated) to fit all fragments,
// so subsequent WriteAt calls from the writer never extend the file. If the
// current segment can't fit the shard, it is marked full and the store rolls
// to the next segment number (up to 100 attempts). The index entry is committed
// only when the writer is closed.
//
// size == 0 short-circuits: no segment reservation, no fragNum allocation, no
// fragment written. The writer's Close just commits an empty-extent index
// entry. This avoids wasting an 8 KiB fragment per 0-byte shard.
func (store *Store) Append(objectHash [32]byte, shardIndex uint32, size int64) (writer Writer, err error) {
	if size < 0 {
		return nil, fmt.Errorf("negative size %d", size)
	}

	store.mutex.Lock()
	defer store.mutex.Unlock()

	if store.closed {
		return nil, ErrClosedStore
	}

	if size == 0 {
		ext := extent{}
		return &shardWriter{
			objectHash: objectHash,
			shardIndex: shardIndex,
			storeID:    store.storeID,
			aead:       store.aead,
			ext:        ext,
			onClose: func() error {
				key := MakeShardKey(objectHash, shardIndex)
				if err := store.index.Set(key, ext.encode()); err != nil {
					return fmt.Errorf("commit: %w", err)
				}
				return nil
			},
		}, nil
	}

	// Ceiling division: number of fragments needed to hold size logical bytes.
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

	var off int64
	seg, err := store.getNextSegment(100, func(seg *segment) error {
		info, err := seg.Stat()
		if err != nil {
			return err
		}

		segSize := info.Size()
		newSegSize := uint64(segSize) + fragCount*totalFragSize //nolint:gosec // segSize from os.File.Stat().Size() is always non-negative.
		if newSegSize >= store.maxSegSize {
			if err := seg.markFull(); err != nil {
				slog.Warn("failed to mark segment full",
					"num", store.segNum,
					"error", err,
				)
			}
		}

		if newSegSize > store.maxSegSize && segSize != segHeaderSize {
			return fmt.Errorf("segment full")
		}

		off = segSize
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Pre-allocate the extent so writer WriteAt calls never extend the file.
	if err := seg.Truncate(off + totalFragSize*int64(fragCount)); err != nil { //nolint:gosec // fragCount bounded by non-negative int64 size.
		return nil, fmt.Errorf("truncate segment %d: %w", store.segNum, err)
	}

	seg.refs.Add(1)

	ext := extent{
		SegNum: store.segNum,
		Off:    off,
		PSize:  int64(fragCount) * totalFragSize, //nolint:gosec // fragCount derived from non-negative int64 size.
		LSize:  size,
	}

	writer = &shardWriter{
		objectHash: objectHash,
		shardIndex: shardIndex,
		storeID:    store.storeID,
		aead:       store.aead,
		seg:        seg,
		ext:        ext,
		shardNum:   store.shardNum,
		fragNum:    store.fragNum,
		buf:        make([]byte, writeBufLen*totalFragSize),
		bufPos:     0,

		onClose: func() error {
			key := MakeShardKey(objectHash, shardIndex)
			if err := store.index.Set(key, ext.encode()); err != nil {
				return fmt.Errorf("commit: %w", err)
			}

			return nil
		},
		closed: false,
	}

	store.shardNum += 1
	store.fragNum += fragCount

	return writer, nil
}

// Delete removes the index entry for a shard. The on-disk extent becomes dead
// space reclaimable by a future compactor.
func (store *Store) Delete(objectHash [32]byte, shardIndex uint32) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	if store.closed {
		return ErrClosedStore
	}

	key := MakeShardKey(objectHash, shardIndex)
	if err := store.index.Delete(key); err != nil {
		return fmt.Errorf("delete: %w", err)
	}

	return nil
}

// Close drains all outstanding segment references (spinning with Gosched),
// closes segment file descriptors and the index. Blocks until all readers and
// writers have been closed.
func (store *Store) Close() error {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	if store.closed {
		return ErrClosedStore
	}

	store.closed = true

	var errs []error

	if err := store.saveState(); err != nil {
		slog.Warn("failed save store state:", "error", err)
	}

	for num, seg := range store.segCache {
		for seg.refs.Load() > 0 {
			runtime.Gosched()
		}

		if err := seg.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close segment %d: %w", num, err))
		}
	}

	if err := store.index.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close disk index: %w", err))
	}

	return errors.Join(errs...)
}

// MakeShardKey builds a 36-byte index key: 32-byte object hash || 4-byte big-endian shard index.
func MakeShardKey(objectHash [32]byte, shardIndex uint32) []byte {
	key := make([]byte, 36)
	copy(key[:32], objectHash[:])
	binary.BigEndian.PutUint32(key[32:], shardIndex)

	return key
}
