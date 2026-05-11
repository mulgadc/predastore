package store

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const stateFilename = "state.json"

type state struct {
	SegNum           uint64 `json:"segNum"`
	ShardNum         uint64 `json:"shardNum"`
	FragNum          uint64 `json:"fragNum"`
	FragNumHighWater uint64 `json:"fragNumHighWater"`
	StoreID          uint32 `json:"storeID"`
}

// loadState reads state.json from the Store directory and restores monotonic
// counters. If state.json does not exist, a fresh storeID is generated and
// the in-memory counters are zeroed; callers MUST follow up with a durable
// saveState before any fragment is written under this storeID — otherwise a
// crash + restart could regenerate a different storeID and orphan any data
// written under the old one.
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

	var sta state
	if err := json.Unmarshal(data, &sta); err != nil {
		return err
	}

	store.segNum = sta.SegNum
	store.shardNum = sta.ShardNum
	store.storeID = sta.StoreID
	store.fragNumHighWater = sta.FragNumHighWater
	// fragNum resumes from the persisted high-water mark: the unflushed
	// reservation window from before the crash is sacrificed to guarantee
	// nonce uniqueness (see Stage 1 plan, "fragNum high-water reservation").
	store.fragNum = sta.FragNumHighWater
	return nil
}

// saveState atomically and durably persists the Store's counters to disk:
// write to state.json.tmp, fsync the file, rename to state.json, fsync the
// parent directory. Returns when all writes are guaranteed durable.
func (store *Store) saveState() (retErr error) {
	sta := state{
		SegNum:           store.segNum,
		ShardNum:         store.shardNum,
		FragNum:          store.fragNum,
		FragNumHighWater: store.fragNumHighWater,
		StoreID:          store.storeID,
	}
	data, err := json.Marshal(sta)
	if err != nil {
		return err
	}

	tmpPath := filepath.Join(store.dir, stateFilename+".tmp")
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

	dir, err := os.Open(store.dir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
