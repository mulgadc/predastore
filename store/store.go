// Package store is a log-structured object store. It replaces predastore/s3/wal
// and implements the reserve → lock-free WriteAt → fsync+commit design from
// DESIGN.md §6. The on-disk format (14-byte segment header, 32-byte slot
// header + 8 KiB padded payload + CRC32) is identical to the legacy WAL;
// only the package, file extension (.seg), and Go API change.
package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mulgadc/predastore/s3db"
)

const stateFileName = "state.json"
const indexFileName = "db"

// Store is a log-structured object store. Writes reserve disjoint slot
// ranges under a short lock, then perform lock-free WriteAt followed by
// fsync and Badger commit.
type Store struct {
	dir   string
	index *s3db.S3DB

	seg *segment
	mu  sync.Mutex

	segNum   atomic.Uint64
	shardNum atomic.Uint64
	seqNum   atomic.Uint64
	epoch    time.Time
}

// storeState is the JSON-serialised state persisted to state.json between restarts.
type storeState struct {
	SegNum   uint64    `json:"SegNum"`
	SeqNum   uint64    `json:"SeqNum"`
	ShardNum uint64    `json:"ShardNum"`
	Epoch    time.Time `json:"Epoch"`
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

// Close persists Store state, blocks until in-flight writers drain,
// then closes the active segment and the Badger index.
func (store *Store) Close() error {
	store.mu.Lock()
	defer store.mu.Unlock()

	var errs []error

	if err := store.saveState(); err != nil {
		errs = append(errs, fmt.Errorf("store: Close: save state: %w", err))
	}

	if store.seg != nil {
		// Release the Store's own ref. Spin until in-flight writers release theirs.
		store.seg.refs.Add(-1)
		for store.seg.refs.Load() > 0 {
			runtime.Gosched()
		}
		if err := store.seg.file.Close(); err != nil {
			errs = append(errs, fmt.Errorf("store: Close: close segment %d: %w", store.seg.num, err))
		}
	}

	if err := store.index.Close(); err != nil {
		errs = append(errs, fmt.Errorf("store: Close: close index: %w", err))
	}

	return errors.Join(errs...)
}

// loadState reads state.json from the Store directory and restores monotonic counters.
func (store *Store) loadState() error {
	var state storeState
	if data, err := os.ReadFile(filepath.Join(store.dir, stateFileName)); err != nil {
		return err
	} else if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	// +1 ensures counters never reuse a value from a previous run.
	store.segNum.Store(state.SegNum + 1)
	store.seqNum.Store(state.SeqNum + 1)
	store.shardNum.Store(state.ShardNum + 1)
	store.epoch = state.Epoch

	return nil
}

// saveState writes the current monotonic counters to state.json in the Store directory.
func (store *Store) saveState() error {
	state := storeState{
		SegNum:   store.segNum.Load(),
		SeqNum:   store.seqNum.Load(),
		ShardNum: store.shardNum.Load(),
		Epoch:    store.epoch,
	}

	stateData, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("store: saveState: marshal: %w", err)
	}

	return os.WriteFile(filepath.Join(store.dir, stateFileName), stateData, 0600)
}
