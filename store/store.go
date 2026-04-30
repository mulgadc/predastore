// Package store implements segment-based shard storage with CRC-protected
// fragments. Shards are written as contiguous extents of fixed-size fragments
// within append-only segment files. Segments roll when they reach maxSegSize.
package store

import (
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
)

const indexFilename = "db"

// Store manages segment files and an index mapping shard keys to on-disk extents.
// All public methods are safe for concurrent use. Segment files are pre-allocated
// via Truncate and written lock-free with WriteAt; the mutex protects only metadata
// (counters, segment cache, index commits).
type Store struct {
	dir      string
	index    *s3db.S3DB
	segCache map[uint64]*segment
	mutex    sync.Mutex

	// Monotonic counters persisted to state.json across restarts.
	segNum   uint64
	shardNum uint64
	fragNum  uint64

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

// Option configures a Store at Open time.
type Option func(*Store)

// WithMaxSegSize overrides the segment-roll threshold. Primarily intended for
// tests that need to exercise rollover without writing 4 GiB of data.
func WithMaxSegSize(n uint64) Option {
	return func(s *Store) { s.maxSegSize = n }
}

// Open recovers or creates a Store in dir. On startup it restores monotonic
// counters from state.json, opens the index, then finds a non-full segment to
// write into — rolling forward through segment numbers until one with capacity
// is found (up to 100 attempts).
func Open(dir string, opts ...Option) (store *Store, err error) {
	store = &Store{
		dir:        dir,
		segCache:   make(map[uint64]*segment),
		maxSegSize: DefaultMaxSegSize,
	}

	for _, opt := range opts {
		opt(store)
	}

	if err := store.loadState(); err != nil {
		slog.Warn("failed to load store state", "error", err)
	}

	store.index, err = s3db.New(filepath.Join(dir, indexFilename))
	if err != nil {
		return nil, fmt.Errorf("open disk index: %w", err)
	}

	_, err = store.getNextSegment(100, func(*segment) error { return nil })
	if err != nil {
		return nil, err
	}

	if err := store.saveState(); err != nil {
		slog.Warn("failed to save store state:", "error", err)
	}

	return store, err
}

// Lookup returns a reader for the given shard. The underlying segment is
// reference-counted: the caller must call reader.Close() to release it.
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

	segment, err := store.getSegment(extent.SegNum)
	if err != nil {
		return nil, fmt.Errorf("get segment %d: %w", extent.SegNum, err)
	}

	segment.refs.Add(1)

	reader = &shardReader{
		seg:    segment,
		ext:    extent,
		buf:    make([]byte, readBufLen*totalFragSize),
		bufPos: 0,
		closed: false,
	}

	return reader, nil
}

// Append reserves space for a shard of the given logical size and returns a
// writer. The segment file is pre-allocated (Truncated) to fit all fragments,
// so subsequent WriteAt calls from the writer never extend the file. If the
// current segment can't fit the shard, it is marked full and the store rolls
// to the next segment number (up to 100 attempts). The index entry is committed
// only when the writer is closed.
func (store *Store) Append(objectHash [32]byte, shardIndex uint32, size int64) (writer Writer, err error) {
	if size < 0 {
		return nil, fmt.Errorf("negative size %d", size)
	}

	store.mutex.Lock()
	defer store.mutex.Unlock()

	if store.closed {
		return nil, ErrClosedStore
	}

	// Ceiling division: number of fragments needed to hold size logical bytes.
	fragCount := max(1, (uint64(size)+fragBodySize-1)/fragBodySize)

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
		seg:      seg,
		ext:      ext,
		shardNum: store.shardNum,
		fragNum:  store.fragNum,
		buf:      make([]byte, writeBufLen*totalFragSize),
		bufPos:   0,

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

	if err := store.saveState(); err != nil {
		slog.Warn("failed to save store state:", "error", err)
	}

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
