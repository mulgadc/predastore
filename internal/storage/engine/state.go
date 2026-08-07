package engine

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

	var sta state
	if err := json.Unmarshal(data, &sta); err != nil {
		return err
	}

	store.segNum = sta.SegNum
	store.shardNum = sta.ShardNum
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

	// The rename itself is only durable once the parent directory is fsynced.
	dir, err := os.Open(store.dir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
