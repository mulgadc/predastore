// Package store is a log-structured object store. It replaces predastore/s3/wal
// and implements the reserve → lock-free WriteAt → fsync+commit design from
// DESIGN.md §6. The on-disk format (14-byte segment header, 32-byte slot
// header + 8 KiB padded payload + CRC32) is identical to the legacy WAL;
// only the package, file extension (.seg), and Go API change.
package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/mulgadc/predastore/s3db"
)

const stateFileName = "state.json"
const indexFileName = "db"

const extension = ".seg"

// magic identifies a file as a store segment.
var magic = [4]byte{'S', '3', 'S', 'F'}

const (
	_ uint16 = iota
	v1
)

const (
	KiB = 1024
	MiB = 1024 * KiB
	GiB = 1024 * MiB
)

const (
	segHeaderSize  = 14
	slotHeaderSize = 32
	fragSize       = 8 * KiB
	slotSize       = slotHeaderSize + fragSize
	maxSegSize     = 4 * GiB
)

// slotFlags is the per-slot feature flag bitmask.
type slotFlags uint32

const (
	flagNone         slotFlags = 0
	flagEndOfShard   slotFlags = 1 << 0
	flagShardHeader  slotFlags = 1 << 1
	flagCompressed   slotFlags = 1 << 2
	flagDeleted      slotFlags = 1 << 3
	flagPartialWrite slotFlags = 1 << 4
	flagReserved1    slotFlags = 1 << 5
	flagReserved2    slotFlags = 1 << 6
	flagReserved3    slotFlags = 1 << 7
)

// Store is a log-structured object store. Writes reserve disjoint slot
// ranges under a short lock, then perform lock-free WriteAt followed by
// fsync and Badger commit.
type Store struct {
	dir string

	index *s3db.S3DB
	segs  map[uint64]*os.File

	segNum  uint64
	objNum  uint64
	fragNum uint64

	mu sync.Mutex
}

// storeState is the JSON-serialised state persisted to state.json between restarts.
type storeState struct {
	SegNum   uint64    `json:"SegNum"`
	SeqNum   uint64    `json:"SeqNum"`
	ShardNum uint64    `json:"ShardNum"`
	Epoch    time.Time `json:"Epoch"`
}

type extent struct {
	segNum  uint64
	fragNum uint64
	offset  int64
	size    int64

	buf []byte
}

// Open opens or creates a Store rooted at dir.
func Open(dir string) (store *Store, err error) {
	store = &Store{
		dir: dir,
	}

	// Load persisted counters. Missing state is expected for fresh directories.
	if err := store.loadState(); err != nil {
		slog.Warn("store: Open: load state", "error", err)
	}

	// Open the Badger index for shard metadata.
	store.index, err = s3db.New(filepath.Join(dir, indexFileName))
	if err != nil {
		return nil, err
	}

	// Attach the active segment, rotating past full segments on restart.
	const maxAttempts = 100
	for attempt := range maxAttempts {
		store.seg, err = openSegment(dir, store.segNum.Load())
		if err == nil {
			break
		}
		slog.Debug("store: Open: segment full, rotating",
			"segNum", store.segNum.Load(),
			"attempt", attempt,
			"error", err,
		)
		store.segNum.Add(1)
		if attempt == maxAttempts-1 {
			return nil, fmt.Errorf("store: Open: open segment after %d attempts: %w", maxAttempts, err)
		}
	}

	if err = store.saveState(); err != nil {
		return nil, fmt.Errorf("store: Open: save state: %w", err)
	}

	return store, err
}

// Lookup fetches the object metadata for key from the store index.
func (st *Store) Lookup(key string) (ObjectReader, error) {
	data, err := st.index.Get([]byte(key))
	if err != nil {
		return nil, err
	}
	obj, err := decodeObject(data)
	if err != nil {
		return nil, err
	}
	obj.store = st
	return obj, nil
}

// Append reserves slots for an object of the given size and returns an
// ObjectWriter. The caller writes data via the ObjectWriter and calls
// Close to fsync and commit. The lock is held only during reservation.
func (st *Store) Append(key string, size int64) (ObjectWriter, error) {

	seqNum := st.seqNum.Load()
	st.seqNum.Add(uint64(fragCount))
	shardNum := st.shardNum.Add(1)
	slotExts, err := st.reserve(fragCount)

	if err != nil {
		return nil, fmt.Errorf("store: Append: reserve: %w", err)
	}

	return &objectWriter{
		store:     st,
		key:       key,
		size:      size,
		exts:      slotExts,
		seqNum:    seqNum,
		shardNum:  shardNum,
		fragCount: fragCount,
		buf:       make([]byte, writeBufferFragments*fragSize),
	}, nil
}

// Delete removes the index entry for the given key. It does not
// reclaim segment disk space; that is compaction's job.
func (st *Store) Delete(key string) error {
	if err := st.index.Delete([]byte(key)); err != nil {
		return fmt.Errorf("store: Delete: %w", err)
	}
	return nil
}

// Close persists Store state, blocks until in-flight writers drain,
// then closes the active segment and the Badger index.
func (st *Store) Close() error {
	st.mu.Lock()
	defer st.mu.Unlock()

	var errs []error

	if err := st.saveState(); err != nil {
		errs = append(errs, fmt.Errorf("store: Close: save state: %w", err))
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

// loadState reads state.json from the Store directory and restores monotonic counters.
func (st *Store) loadState() error {
	var state storeState
	if data, err := os.ReadFile(filepath.Join(st.dir, stateFileName)); err != nil {
		return err
	} else if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	// +1 ensures counters never reuse a value from a previous run.
	st.segNum.Store(state.SegNum + 1)
	st.seqNum.Store(state.SeqNum + 1)
	st.shardNum.Store(state.ShardNum + 1)
	st.epoch = state.Epoch

	return nil
}

// saveState writes the current monotonic counters to state.json in the Store directory.
func (st *Store) saveState() error {
	state := storeState{
		SegNum:   st.segNum.Load(),
		SeqNum:   st.seqNum.Load(),
		ShardNum: st.shardNum.Load(),
		Epoch:    st.epoch,
	}

	stateData, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("store: saveState: marshal: %w", err)
	}

	return os.WriteFile(filepath.Join(st.dir, stateFileName), stateData, 0600)
}

func (st *Store) openFile(num uint64) (file *os.File, err error) {
	if file, ok := st.segs[num]; ok {
		return file, nil
	}

	path := filepath.Join(st.dir, fmt.Sprintf("%016d%s", num, extension))

	file, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open %v: %w", path, err)
	}

	st.segs[num] = file

	return file, nil
}

func (st *Store) alloc(objSize int64) (ext *extent, err error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	fragCount := max(1, (objSize+fragSize-1)/fragSize)

	var seg *os.File
	var offset int64
outer:
	for {
		seg, err = st.openFile(st.segNum)
		if err != nil {
			return nil, fmt.Errorf("alloc: %w", err)
		}

		stat, err := seg.Stat()
		if err != nil {
			return nil, fmt.Errorf("alloc: stat: %w", err)
		}

		switch {
		// New file: write the segment header, then break loop.
		case stat.Size() == 0:
			header := make([]byte, segHeaderSize)
			copy(header[0:4], magic[:])                 // [0:4]   magic
			binary.BigEndian.PutUint16(header[4:6], v1) // [4:6]   version
			binary.BigEndian.PutUint32(header[6:14], 0) // [6:14]  empty bytes

			if _, err = seg.Write(header); err != nil {
				return nil, fmt.Errorf("alloc: write seg header: %w", err)
			}

			offset = 14

			break outer

		// Existing file: validate segment header, then break loop.
		case stat.Size()+slotSize*fragCount <= maxSegSize:
			header := make([]byte, segHeaderSize)
			if _, err = seg.ReadAt(header, 0); err != nil {
				return nil, fmt.Errorf("alloc: read seg %d header: %w", st.segNum, err)
			}

			var fileMagic [4]byte
			copy(fileMagic[:], header[0:4])
			if fileMagic != magic {
				return nil, fmt.Errorf("alloc: read seg %d header: invalid magic %x", st.segNum, fileMagic)
			}

			offset = stat.Size()

			break outer

		// Segment full: open next.
		default:
			st.segNum += 1
			continue
		}
	}

	ext = &extent{
		segNum: st.segNum,
		offset: offset,
		size:   slotSize * fragCount,
	}

	if err := seg.Truncate(offset + slotSize*fragCount); err != nil {
		return nil, fmt.Errorf("alloc: truncate seg %d: %w", st.segNum, err)
	}

	// Best-effort persist; counters are recoverable from segment files.
	if err := st.saveState(); err != nil {
		slog.Warn("store: alloc: save state", "error", err)
	}

	return ext, nil
}
