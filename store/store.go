// Package store is a log-structured object store. It replaces predastore/s3/wal
// and implements the reserve → lock-free WriteAt → fsync+commit design from
// DESIGN.md §6. The on-disk format (14-byte segment header, 32-byte slot
// header + 8 KiB padded payload + CRC32) is identical to the legacy WAL;
// only the package, file extension (.seg), and Go API change.
package store

import (
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/mulgadc/predastore/s3db"
)

const indexFilename = "db"

// slotBufferPool reuses slot-sized buffers (slotHeaderSize + slotPayloadSize = 8224 bytes).
var slotBufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, slotSize)
		return &b
	},
}

// zeroPadBuffer is a pre-allocated zero slice for clearing the padding region of a slot payload.
var zeroPadBuffer = make([]byte, int(fragSize))

type segNum int64
type objNum int64
type slotNum int64

type ObjectKey string

type Store struct {
	dir   string
	index *s3db.S3DB
	segs  map[segNum]*segment

	segNum  segNum
	objNum  objNum
	slotNum slotNum

	mu     sync.Mutex
	closed bool
}

type objectReader struct {
	seg *segment
	ext extent
	buf []byte
	pos int64

	onClose func()
	closed  bool
}

type objectWriter struct {
	key ObjectKey
	seg *segment
	ext extent
	buf []byte
	pos int64

	onClose func()
	closed  bool
}

// Open opens or creates a Store rooted at dir.
func Open(dir string) (st *Store, err error) {
	st = &Store{
		dir: dir,
	}

	// Load persisted counters. Missing state is expected for fresh directories.
	if err := st.loadState(); err != nil {
		slog.Warn("store: Open: load state", "error", err)
	}

	// Open the Badger index for shard metadata.
	st.index, err = s3db.New(filepath.Join(dir, indexFilename))
	if err != nil {
		return nil, err
	}

	// Attach the active segment, rotating past full segments on restart.
	const maxAttempts = 100
	for attempt := range maxAttempts {
		seg, err := st.openSegment(st.segNum)
		if err != nil {
			return nil, err
		}

		segFull, err := seg.isFull()
		if err != nil {
			slog.Warn("failed to read segment flags",
				"segNum", st.segNum,
				"error", err,
			)
		} else if !segFull {
			break
		}

		if attempt == maxAttempts-1 {
			return nil, fmt.Errorf("failed to open segment after %d attempts: %w", maxAttempts, err)
		}

		slog.Debug("rotating",
			"segNum", st.segNum,
			"attempt", attempt,
			"error", err,
		)

		st.segNum += 1
	}

	if err = st.saveState(); err != nil {
		return nil, fmt.Errorf("saveState: %w", err)
	}

	return st, err
}

// Lookup fetches the object metadata for key from the store index.
func (st *Store) Lookup(key ObjectKey) (or *objectReader, err error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.closed {
		return nil, fmt.Errorf("store closed")
	}

	data, err := st.index.Get([]byte(key))
	if err != nil {
		return nil, fmt.Errorf("Lookup %s: %w", key, err)
	}

	ext, err := decodeExtent(data)
	if err != nil {
		return nil, err
	}

	seg, err := st.openSegment(ext.segNum)
	if err != nil {
		return nil, fmt.Errorf("openSegment: %w", err)
	}

	seg.refs.Add(1)

	or = &objectReader{
		seg: seg,
		ext: *ext,
		buf: make([]byte, readBufferSlots*slotSize),
		pos: 0,

		onClose: func() {
			seg.refs.Add(-1)
		},
		closed: false,
	}

	return or, nil
}

func (st *Store) Append(key ObjectKey, size int64) (ow *objectWriter, err error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.closed {
		return nil, fmt.Errorf("store closed")
	}

	slotCount := max(1, (size+fragSize-1)/fragSize)

	var seg *segment
	var offset int64

	for {
		seg, err = st.openSegment(st.segNum)
		if err != nil {
			return nil, fmt.Errorf("openSegment: %w", err)
		}

		segSize, err := seg.Size()
		if err != nil {
			return nil, err
		}

		if segSize == segHeaderSize || segSize+slotCount*slotSize <= maxSegSize {
			offset = segSize
			break
		}

		if err := seg.markFull(); err != nil {
			return nil, err
		}

		st.segNum += 1
	}

	if err := seg.fd.Truncate(offset + slotSize*slotCount); err != nil {
		return nil, fmt.Errorf("truncate seg %d: %w", st.segNum, err)
	}

	seg.refs.Add(1)

	ow = &objectWriter{
		key: key,
		seg: seg,
		ext: extent{
			segNum:  st.segNum,
			objNum:  st.objNum,
			slotNum: st.slotNum,
		},
		buf: make([]byte, writeBufferSlots*slotSize),
		pos: 0,

		onClose: func() {
			seg.refs.Add(-1)
		},
		closed: false,
	}

	st.objNum += 1
	st.slotNum += slotNum(slotCount)

	// Best-effort persist; counters are recoverable from segment files.
	if err := st.saveState(); err != nil {
		slog.Warn("Append failed:", "error", err)
	}

	return ow, nil
}

func (st *Store) Delete(key ObjectKey) error {
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.closed {
		return fmt.Errorf("store closed")
	}

	if err := st.index.Delete([]byte(key)); err != nil {
		return fmt.Errorf("Delete %s failed: %w", key, err)
	}
	return nil
}

// Close persists Store state, blocks until in-flight writers drain,
// then closes the active segment and the Badger index.
func (st *Store) Close() error {
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.closed {
		return fmt.Errorf("store closed")
	}

	st.closed = true

	var errs []error

	if err := st.saveState(); err != nil {
		errs = append(errs, fmt.Errorf("saveState: %w", err))
	}

	if st.seg != nil {
		// Release the Store's own ref. Spin until in-flight writers release theirs.
		st.seg.refs.Add(-1)
		for st.seg.refs.Load() > 0 {
			runtime.Gosched()
		}
		if err := st.seg.file.Close(); err != nil {
			errs = append(errs, fmt.Errorf("store: Close: close segment %d: %w", st.seg.num, err))
		}
	}

	if err := st.index.Close(); err != nil {
		errs = append(errs, fmt.Errorf("store: Close: close index: %w", err))
	}

	return errors.Join(errs...)
}
