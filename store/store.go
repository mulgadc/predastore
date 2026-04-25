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

func Open(dir string) (st *Store, err error) {
	st = &Store{
		dir: dir,
	}

	if err := st.loadState(); err != nil {
		slog.Warn("store: Open: load state", "error", err)
	}

	st.index, err = s3db.New(filepath.Join(dir, indexFilename))
	if err != nil {
		return nil, err
	}

	const maxAttempts = 100
	for attempt := range maxAttempts {
		seg, err := openSegment(st.dir, st.segNum)
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
			st.segs[st.segNum] = seg
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

	if err := st.saveState(); err != nil {
		slog.Warn("saveState:", "error", err)
	}

	return st, err
}

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

	seg, err := st.getSegment(ext.segNum)
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
		seg, err = st.getSegment(st.segNum)
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
		buf: make([writeBufFrags * fragSize]byte, writeBufFrags*fragSize),
		pos: 0,

		onClose: func() {
			seg.refs.Add(-1)
		},
		closed: false,
	}

	st.objNum += 1
	st.slotNum += slotNum(slotCount)

	if err := st.saveState(); err != nil {
		slog.Warn("saveState:", "error", err)
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

func (st *Store) Close() error {
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.closed {
		return fmt.Errorf("store closed")
	}

	st.closed = true

	var errs []error

	if err := st.saveState(); err != nil {
		slog.Warn("saveState:", "error", err)
	}

	for num, seg := range st.segs {
		for seg.refs.Load() > 0 {
			runtime.Gosched()
		}

		if err := seg.fd.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close segment %d: %w", num, err))
		}
	}

	if err := st.index.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close index: %w", err))
	}

	return errors.Join(errs...)
}
