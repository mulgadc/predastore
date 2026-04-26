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

	segNum   uint64
	shardNum uint64
	fragNum  uint64

	closed bool
}

func Open(dir string) (store *Store, err error) {
	store = &Store{
		dir:      dir,
		segCache: make(map[uint64]*segment),
	}

	if err := store.loadState(); err != nil {
		slog.Warn("failed to load store state", "error", err)
	}

	store.index, err = s3db.New(filepath.Join(dir, indexFilename))
	if err != nil {
		return nil, fmt.Errorf("open disk index: %w", err)
	}

	const maxAttempts = 100
	for attempt := range maxAttempts {
		seg, err := func() (*segment, error) {
			seg, err := openSegment(store.dir, store.segNum)
			if err != nil {
				return nil, fmt.Errorf("open segment: %w", err)
			}

			defer func() {
				if err != nil {
					if closeErr := seg.file.Close(); closeErr != nil {
						slog.Warn("failed to close segment",
							"num", store.segNum,
							"attempt", attempt,
							"error", closeErr,
						)
					}
				}
			}()

			full, err := seg.isFull()
			if err != nil {
				return nil, fmt.Errorf("check segment capacity: %w", err)
			}

			if full {
				return nil, fmt.Errorf("segment full")
			}

			return seg, nil
		}()

		if err == nil {
			store.segCache[store.segNum] = seg
			break
		}

		if attempt == maxAttempts-1 {
			return nil, fmt.Errorf("get segment after %d attempts: %w", maxAttempts, err)
		}

		slog.Debug("rotating segment",
			"segNum", store.segNum,
			"attempt", attempt,
			"error", err,
		)

		store.segNum += 1
	}

	if err := store.saveState(); err != nil {
		slog.Warn("failed to save store state:", "error", err)
	}

	return store, err
}

func (store *Store) Lookup(objectHash [32]byte, shardIndex uint32) (reader *shardReader, err error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	if store.closed {
		return nil, fmt.Errorf("store closed")
	}

	key := makeShardKey(objectHash, shardIndex)
	data, err := store.index.Get(key)
	if err != nil {
		return nil, fmt.Errorf("get: %w", err)
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

		onClose: func() error {
			segment.refs.Add(-1)

			return nil
		},
		closed: false,
	}

	return reader, nil
}

func (store *Store) Append(objectHash [32]byte, shardIndex uint32, size int64) (writer *shardWriter, err error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	if store.closed {
		return nil, fmt.Errorf("store closed")
	}

	fragCount := max(1, (uint64(size)+fragBodySize-1)/fragBodySize)

	var seg *segment
	var off int64

	const maxAttempts = 100
	for attempt := range maxAttempts {
		if seg, err = func() (*segment, error) {
			seg, err := store.getSegment(store.segNum)
			if err != nil {
				return nil, fmt.Errorf("get segment: %w", err)
			}

			if full, err := seg.isFull(); err != nil {
				return nil, fmt.Errorf("check segment capacity: %w", err)
			} else if full {
				return nil, fmt.Errorf("segment full")
			}

			segSize, err := seg.Size()
			if err != nil {
				return nil, fmt.Errorf("get segment size: %w", err)
			}

			newSegSize := uint64(segSize) + fragCount*totalFragSize
			if newSegSize >= maxSegSize {
				if err := seg.markFull(); err != nil {
					slog.Warn("failed to mark segment full",
						"num", store.segNum,
						"attempt", attempt,
						"error", err,
					)
				}

			}

			if newSegSize > maxSegSize && segSize != segHeaderSize {
				return nil, fmt.Errorf("segment full")
			}

			off = segSize
			return seg, nil
		}(); err == nil {
			break
		}

		if attempt == maxAttempts-1 {
			return nil, fmt.Errorf("get segment after %d attempts: %w", maxAttempts, err)
		}

		slog.Debug("rotating segment",
			"segNum", store.segNum,
			"attempt", attempt,
			"error", err,
		)

		store.segNum += 1
	}

	// TODO: Check if this cast could be a problem
	if err := seg.file.Truncate(off + totalFragSize*int64(fragCount)); err != nil {
		return nil, fmt.Errorf("truncate segment %d: %w", store.segNum, err)
	}

	seg.refs.Add(1)

	writer = &shardWriter{
		seg: seg,
		ext: extent{
			SegNum: store.segNum,
			Off:    off,
			PSize:  int64(fragCount) * totalFragSize,
			LSize:  size,
		},
		shardNum: store.shardNum,
		fragNum:  store.fragNum,
		buf:      make([]byte, writeBufLen*totalFragSize),
		bufPos:   0,

		onClose: func() error {
			encoded, err := writer.ext.encode()
			if err != nil {
				return fmt.Errorf("encode extent: %w", err)
			}

			key := makeShardKey(objectHash, shardIndex)
			if err := store.index.Set(key, encoded); err != nil {
				return fmt.Errorf("commit: %w", err)
			}

			seg.refs.Add(-1)

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

func (store *Store) Delete(objectHash [32]byte, shardIndex uint32) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	if store.closed {
		return fmt.Errorf("store closed")
	}

	key := makeShardKey(objectHash, shardIndex)
	if err := store.index.Delete(key); err != nil {
		return fmt.Errorf("delete: %w", err)
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
		slog.Warn("failed save store state:", "error", err)
	}

	for num, seg := range store.segCache {
		for seg.refs.Load() > 0 {
			runtime.Gosched()
		}

		if err := seg.file.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close segment %d: %w", num, err))
		}
	}

	if err := store.index.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close disk index: %w", err))
	}

	return errors.Join(errs...)
}

func makeShardKey(objectHash [32]byte, shardIndex uint32) []byte {
	key := make([]byte, 36)
	copy(key[:32], objectHash[:])
	binary.BigEndian.PutUint32(key[32:], shardIndex)

	return key
}
