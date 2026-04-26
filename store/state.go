package store

import (
	"encoding/json"
	"os"
	"path/filepath"
)

const stateFilename = "state.json"

type state struct {
	SegNum   uint64 `json:"segNum"`
	ShardNum uint64 `json:"shardNum"`
	FragNum  uint64 `json:"fragNum"`
}

// loadState reads state.json from the Store directory and restores monotonic counters.
func (store *Store) loadState() error {
	var sta state
	if data, err := os.ReadFile(filepath.Join(store.dir, stateFilename)); err != nil {
		return err
	} else if err := json.Unmarshal(data, &sta); err != nil {
		return err
	}

	store.segNum = sta.SegNum
	store.shardNum = sta.ShardNum
	store.fragNum = sta.FragNum

	return nil
}

// saveState writes the current monotonic counters to state.json in the Store directory.
func (store *Store) saveState() error {
	sta := state{
		SegNum:   store.segNum,
		ShardNum: store.shardNum,
		FragNum:  store.fragNum,
	}

	data, err := json.Marshal(sta)
	if err != nil {
		return err
	}

	return os.WriteFile(filepath.Join(store.dir, stateFilename), data, 0600)
}
