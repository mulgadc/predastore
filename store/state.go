package store

import (
	"encoding/json"
	"os"
	"path/filepath"
)

const stateFilename = "state.json"

type state struct {
	SegNum  segNum  `json:"SegNum"`
	ObjNum  objNum  `json:"ObjNum"`
	SlotNum slotNum `json:"SlotNum"`
}

// loadState reads state.json from the Store directory and restores monotonic counters.
func (st *Store) loadState() error {
	var sta state
	if data, err := os.ReadFile(filepath.Join(st.dir, stateFilename)); err != nil {
		return err
	} else if err := json.Unmarshal(data, &sta); err != nil {
		return err
	}

	st.segNum = sta.SegNum
	st.objNum = sta.ObjNum
	st.slotNum = sta.SlotNum

	return nil
}

// saveState writes the current monotonic counters to state.json in the Store directory.
func (st *Store) saveState() error {
	sta := state{
		SegNum:  st.segNum,
		ObjNum:  st.objNum,
		SlotNum: st.slotNum,
	}

	data, err := json.Marshal(sta)
	if err != nil {
		return err
	}

	return os.WriteFile(filepath.Join(st.dir, stateFilename), data, 0600)
}
