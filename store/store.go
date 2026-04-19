// Package store is a log-structured object store. It replaces predastore/s3/wal
// and implements the reserve → lock-free WriteAt → fsync+commit design from
// DESIGN.md §6. The on-disk format (14-byte segment header, 32-byte slot
// header + 8 KiB padded payload + CRC32) is identical to the legacy WAL;
// only the package, file extension (.seg), and Go API change.
package store

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mulgadc/predastore/s3db"
	"github.com/mulgadc/predastore/utils"
)

const stateFileName = "state.json"
const indexFileName = "db"

// makeShardKey builds the 36-byte composite Badger key for a (objectHash,
// shardIndex) pair.
func makeShardKey(objectHash [32]byte, shardIndex int) []byte {
	key := make([]byte, 36)
	copy(key[:32], objectHash[:])
	binary.BigEndian.PutUint32(key[32:], utils.IntToUint32(shardIndex))
	return key
}

// Store is a log-structured object store. Writes reserve disjoint slot
// ranges under a short lock, then perform lock-free WriteAt followed by
// fsync and Badger commit.
type Store struct {
	dir   string
	index *s3db.S3DB

	seg *segment
	mu  sync.Mutex

	segNum atomic.Uint64
	seqNum atomic.Uint64
	epoch  time.Time
}

type storeState struct {
	SegNum uint64    `json:"segNum"`
	SeqNum uint64    `json:"seqNum"`
	Epoch  time.Time `json:"epoch"`
}

// Open or create a Store rooted at dir.
func Open(dir string) (store *Store, err error) {
	store = &Store{
		dir: dir,
	}

	// Attempt to load store state from disk.
	if err := store.loadState(); err != nil {
		slog.Warn("failed to load store state", "error", err)
	}

	// Create DB to map (objectHash, shardIndex) keys to segment offsets.
	store.index, err = s3db.New(filepath.Join(dir, indexFileName))
	if err != nil {
		return nil, err
	}

	// Open (or create) the active segment at segNum and attach it to
	// the Store. On a fresh directory this creates the first segment; on
	// restart it reopens the existing one in append mode. If the existing
	// file is already full, segNum rotates until an available slot is
	// found, matching the rotation behaviour used during normal writes.
	const maxAttempts = 100
	for attempt := range maxAttempts {
		store.seg, err = openSegment(dir, store.segNum.Load())
		if err == nil {
			break
		}
		slog.Debug("segment full or unavailable, rotating",
			"segNum", store.segNum.Load(),
			"attempt", attempt,
			"error", err,
		)
		store.segNum.Add(1)
		if attempt == maxAttempts-1 {
			return nil, fmt.Errorf("could not open segment after %d attempts: %w", maxAttempts, err)
		}
	}

	if err = store.saveState(); err != nil {
		return nil, fmt.Errorf("failed to save store state: %v", err)
	}

	return store, err
}

// Write an object shard to the store and commits its index entry.
// It reserves slot ranges under a short lock, then drains r into those
// slots lock-free and fsyncs before returning.
func (store *Store) Append(objectHash [32]byte, shardIndex int, totalSize int, r io.Reader) (*Shard, error) {
	panic("store: Append not implemented")
}

// Fetch the Shard metadata for (objectHash, shardIndex).
func (store *Store) Lookup(objectHash [32]byte, shardIndex int) (*Shard, error) {
	panic("store: Lookup not implemented")
}

// Delete the index entry for (objectHash, shardIndex).
// Does not reclaim disk space; that is compaction's job.
func (store *Store) Delete(objectHash [32]byte, shardIndex int) error {
	panic("store: Delete not implemented")
}

// Persist Store state and close all underlying files and indexes.
func (store *Store) Close() error {
	panic("store: Close not implemented")
}

// reserve claims a contiguous range of n slots in the active segment under
// mu, rotating to a new segment if necessary.
func (store *Store) reserve(n uint64) (allocation, error) {
	panic("store: reserve not implemented")
}

func (store *Store) loadState() error {
	var state storeState
	if data, err := os.ReadFile(filepath.Join(store.dir, stateFileName)); err != nil {
		return err
	} else if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	store.segNum.Store(state.SegNum + 1)
	store.seqNum.Store(state.SeqNum + 1)
	store.epoch = state.Epoch

	return nil
}

func (store *Store) saveState() error {
	state := storeState{
		SegNum: store.segNum.Load(),
		SeqNum: store.seqNum.Load(),
		Epoch:  store.epoch,
	}

	stateData, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal state data: %v", err)
	}

	return os.WriteFile(filepath.Join(store.dir, stateFileName), stateData, 0600)
}
