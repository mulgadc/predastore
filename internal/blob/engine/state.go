package engine

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dgraph-io/badger/v4"
)

const stateFilename = "state.json"
const legacyStateTemp = stateFilename + ".tmp"

// The header makes engines that only accept the old JSON record refuse Open.
const deferredStateHeader = "predastore-store-state-v2\n"

type state struct {
	SegNum           uint64 `json:"segNum"`
	ValueNum         uint64 `json:"valueNum"`
	FragNum          uint64 `json:"fragNum"`
	FragNumHighWater uint64 `json:"fragNumHighWater"`
	StoreID          uint32 `json:"storeID"`
}

// loadState restores the monotonic counters, generating a fresh storeID when
// the data dir has no state.json. Callers must follow with a durable saveState
// before any fragment is sealed: a crash first would regenerate a different
// storeID and orphan everything written under the old one.
func (store *Store) loadState() error {
	data, err := os.ReadFile(filepath.Join(store.dir, stateFilename))
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}

		var idBytes [4]byte
		if _, err := rand.Read(idBytes[:]); err != nil {
			return fmt.Errorf("generate storeID: %w", err)
		}
		store.storeID = binary.BigEndian.Uint32(idBytes[:])
		return nil
	}

	data, store.deferredState = bytes.CutPrefix(data, []byte(deferredStateHeader))
	var sta state
	if err := json.Unmarshal(data, &sta); err != nil {
		return err
	}

	store.segNum = sta.SegNum
	store.valueNum = sta.ValueNum
	store.storeID = sta.StoreID
	store.fragNumHighWater = sta.FragNumHighWater
	// Resume from the high-water, not the last fragNum: the unflushed window from
	// before a crash is sacrificed to keep nonces unique.
	store.fragNum = sta.FragNumHighWater
	return nil
}

// saveState persists the counters atomically, returning only once they are
// durable.
func (store *Store) saveState() (retErr error) {
	sta := state{
		SegNum:           store.segNum,
		ValueNum:         store.valueNum,
		FragNum:          store.fragNum,
		FragNumHighWater: store.fragNumHighWater,
		StoreID:          store.storeID,
	}
	data, err := json.Marshal(sta)
	if err != nil {
		return err
	}

	if store.deferredState {
		data = append([]byte(deferredStateHeader), data...)
	}

	tmpPath := filepath.Join(store.dir, stateFilename+".next")
	finalPath := filepath.Join(store.dir, stateFilename)

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			_ = f.Close()
		}
		if retErr != nil {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	closed = true

	if err := os.Rename(tmpPath, finalPath); err != nil {
		return err
	}

	// The rename itself is only durable once the parent directory is fsynced.
	dir, err := os.Open(store.dir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// fenceDeferredState is called before a durable deferred row can be protected
// only by its new index namespace.
func (store *Store) fenceDeferredState() error {
	if store.deferredState {
		return nil
	}
	if err := store.reserveLegacyStateTemp(); err != nil {
		return err
	}
	store.deferredState = true
	if err := store.saveState(); err != nil {
		store.deferredState = false
		return fmt.Errorf("fence deferred shard state: %w", err)
	}
	return nil
}

// reserveLegacyStateTemp stops an older open that already loaded the counters.
func (store *Store) reserveLegacyStateTemp() error {
	path := filepath.Join(store.dir, legacyStateTemp)
	if err := os.Mkdir(path, 0700); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("reserve legacy state path: %w", err)
		}
		info, statErr := os.Lstat(path)
		if statErr != nil {
			return fmt.Errorf("inspect legacy state path: %w", statErr)
		}
		if !info.IsDir() {
			return fmt.Errorf("legacy state writer still owns %s: %w", path, err)
		}
	}
	return nil
}

func (store *Store) fenceExistingDeferred() error {
	if store.deferredState {
		return nil
	}
	found := false
	if err := store.index.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{deferredPrefix}
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if len(it.Item().Key()) == preparedKeySize {
				found = true
				break
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if found {
		return store.fenceDeferredState()
	}
	return nil
}
