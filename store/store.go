package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/mulgadc/predastore/s3db"
)

const indexFilename = "db"

type Store struct {
	dir      string
	index    *s3db.S3DB
	segCache map[uint64]*segment
	mutex    sync.Mutex

	segNum  uint64
	objNum  uint64
	slotNum uint64

	closed bool
}

func Open(dir string) (store *Store, err error) {
	store = &Store{
		dir: dir,
	}

	if err := store.loadState(); err != nil {
		slog.Warn("store: Open: load state", "error", err)
	}

	store.index, err = s3db.New(filepath.Join(dir, indexFilename))
	if err != nil {
		return nil, err
	}

	const maxAttempts = 100
	for attempt := range maxAttempts {
		segment, err := openSegment(store.dir, store.segNum)
		if err != nil {
			return nil, err
		}

		segFull, err := segment.isFull()
		if err != nil {
			slog.Warn("failed to read segment flags",
				"segNum", store.segNum,
				"error", err,
			)
		} else if !segFull {
			store.segCache[store.segNum] = segment
			break
		}

		if attempt == maxAttempts-1 {
			return nil, fmt.Errorf("failed to open segment after %d attempts: %w", maxAttempts, err)
		}

		slog.Debug("rotating",
			"segNum", store.segNum,
			"attempt", attempt,
			"error", err,
		)

		store.segNum += 1
	}

	if err := store.saveState(); err != nil {
		slog.Warn("saveState:", "error", err)
	}

	return store, err
}

func (store *Store) Lookup(objectHash [32]byte, shardIndex uint32) (reader *objectReader, err error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	if store.closed {
		return nil, fmt.Errorf("store closed")
	}

	key := makeShardKey(objectHash, shardIndex)
	data, err := store.index.Get(key)
	if err != nil {
		return nil, fmt.Errorf("Lookup %s: %w", key, err)
	}

	extent, err := decodeExtent(data)
	if err != nil {
		return nil, err
	}

	segment, err := store.getSegment(extent.segNum)
	if err != nil {
		return nil, fmt.Errorf("openSegment: %w", err)
	}

	segment.refs += 1

	reader = &objectReader{
		seg: segment,
		ext: extent,
		buf: make([]byte, readBufferSlots*slotSize),
		pos: 0,

		onClose: func() {
			store.mutex.Lock()
			defer store.mutex.Unlock()

			segment.refs -= 1
		},
		closed: false,
	}

	return reader, nil
}

func (store *Store) Append(objectHash [32]byte, shardIndex uint32, size int64) (writer *objectWriter, err error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	if store.closed {
		return nil, fmt.Errorf("store closed")
	}

	slotCount := max(1, (uint64(size)+slotBodySize-1)/slotBodySize)

	var segment *segment
	var off int64

	for {
		segment, err = store.getSegment(store.segNum)
		if err != nil {
			return nil, fmt.Errorf("openSegment: %w", err)
		}

		segFull, err := segment.isFull()
		if err != nil {
			return nil, err
		}

		segSize, err := segment.Size()
		if err != nil {
			return nil, err
		}

		if !segFull {
			// TODO: Check if this cast could be a problem
			if segSize+int64(slotCount)*slotSize >= maxSegSize {
				if err := segment.markFull(); err != nil {
					return nil, err
				}
			}

			// TODO: Check if this cast could be a problem
			if segSize == segHeaderSize || segSize+int64(slotCount)*slotSize <= maxSegSize {
				off = segSize
				break
			}
		}

		store.segNum += 1
	}

	// TODO: Check if this cast could be a problem
	if err := segment.file.Truncate(off + slotSize*int64(slotCount)); err != nil {
		return nil, fmt.Errorf("truncate seg %d: %w", store.segNum, err)
	}

	segment.refs += 1

	writer = &objectWriter{
		seg: segment,
		extent: extent{
			segNum: store.segNum,
			off:    off,
			size:   size,
		},
		objNum:  store.objNum,
		slotNum: store.slotNum,
		buf:     make([]byte, writeBufferFrags*slotBodySize),
		off:     0,

		onClose: func() error {
			store.mutex.Lock()
			defer store.mutex.Unlock()

			encoded, err := writer.extent.encode()
			if err != nil {
				return fmt.Errorf("encode extent: %w", err)
			}

			if err := store.index.Set(makeShardKey(objectHash, shardIndex), encoded); err != nil {
				return fmt.Errorf("commit to index: %w", err)
			}

			segment.refs -= 1

			return nil
		},
		closed: false,
	}

	store.objNum += 1
	store.slotNum += slotCount

	if err := store.saveState(); err != nil {
		slog.Warn("saveState:", "error", err)
	}

	return writer, nil
}

func (store *Store) Delete(hash []byte, index uint32) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	if store.closed {
		return fmt.Errorf("store closed")
	}

	if err := store.index.Delete(hash); err != nil {
		return fmt.Errorf("Delete %s failed: %w", hash, err)
	}
	return nil
}

func (store *Store) Close() error {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	if store.closed {
		return fmt.Errorf("store closed")
	}

	store.closed = true

	var errs []error

	if err := store.saveState(); err != nil {
		slog.Warn("saveState:", "error", err)
	}

	for num, seg := range store.segCache {
		for seg.refs > 0 {
			runtime.Gosched()
		}

		if err := seg.file.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close segment %d: %w", num, err))
		}
	}

	if err := store.index.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close index: %w", err))
	}

	return errors.Join(errs...)
}

func makeShardKey(objectHash [32]byte, shardIndex uint32) []byte {
	key := make([]byte, 36)
	copy(key[:32], objectHash[:])
	binary.BigEndian.PutUint32(key[32:], shardIndex)
	return key
}
