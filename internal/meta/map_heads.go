package meta

import (
	"bytes"
	"container/heap"
	"errors"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

const (
	mapHeadObjectTable = "objects/arn:aws:s3:::"
	mapHeadKeyMarker   = "/map/head."
)

var errMapHeadIndexUnavailable = errors.New("map-head index unavailable")

// mapHeadIndex holds only the object keys used by the authority extension.
type mapHeadIndex map[string]*mapHeadSet

type mapHeadSet struct {
	keys      []string
	positions map[string]int
}

func (s *mapHeadSet) Len() int           { return len(s.keys) }
func (s *mapHeadSet) Less(i, j int) bool { return s.keys[i] > s.keys[j] }
func (s *mapHeadSet) Swap(i, j int) {
	s.keys[i], s.keys[j] = s.keys[j], s.keys[i]
	s.positions[s.keys[i]] = i
	s.positions[s.keys[j]] = j
}
func (s *mapHeadSet) Push(value any) {
	key := value.(string) //nolint:errcheck // Only the heap insertion below supplies a value.
	s.positions[key] = len(s.keys)
	s.keys = append(s.keys, key)
}
func (s *mapHeadSet) Pop() any {
	last := len(s.keys) - 1
	key := s.keys[last]
	s.keys[last] = ""
	s.keys = s.keys[:last]
	delete(s.positions, key)
	return key
}

func isMapHeadObject(key []byte) bool {
	return bytes.HasPrefix(key, []byte(mapHeadObjectTable)) && bytes.Contains(key, []byte(mapHeadKeyMarker))
}

func (heads mapHeadIndex) update(key string, remove bool) {
	// Include every matching prefix so a malformed nested object cannot
	// disappear from an enclosing prefix's authority result.
	for start := len(mapHeadObjectTable); start < len(key); {
		i := strings.Index(key[start:], mapHeadKeyMarker)
		if i < 0 {
			break
		}
		start += i + len(mapHeadKeyMarker)
		prefix := key[:start]
		set := heads[prefix]
		if remove {
			if set == nil {
				continue
			}
			if pos, ok := set.positions[key]; ok {
				heap.Remove(set, pos)
			}
			if set.Len() == 0 {
				delete(heads, prefix)
			}
			continue
		}

		if set == nil {
			set = &mapHeadSet{positions: make(map[string]int)}
			heads[prefix] = set
		}
		if _, exists := set.positions[key]; !exists {
			heap.Push(set, key)
		}
	}
}

// rebuildMapHeads is called before the replica serves and after snapshot restore.
func (f *FSM) rebuildMapHeads() error {
	heads := make(mapHeadIndex)
	err := f.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(mapHeadObjectTable)
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().Key()
			if isMapHeadObject(key) {
				heads.update(string(key), false)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	f.setMapHeads(heads)
	return nil
}

func (f *FSM) setMapHeads(heads mapHeadIndex) {
	f.mapHeadsMu.Lock()
	f.mapHeads = heads
	f.mapHeadsMu.Unlock()
}

func (f *FSM) applyMapHeadChanges(changes []Command) {
	if len(changes) == 0 {
		return
	}
	f.mapHeadsMu.Lock()
	defer f.mapHeadsMu.Unlock()
	if f.mapHeads == nil {
		return
	}
	for _, cmd := range changes {
		f.mapHeads.update(string(cmd.Key), cmd.Type == CommandDelete)
	}
}

// LastKey returns the greatest object-table map-head key under an exact prefix.
func (f *FSM) LastKey(prefix string) (string, error) {
	if !strings.HasPrefix(prefix, mapHeadObjectTable) || !strings.HasSuffix(prefix, mapHeadKeyMarker) {
		return "", errMapHeadIndexUnavailable
	}
	f.mapHeadsMu.RLock()
	defer f.mapHeadsMu.RUnlock()
	if f.mapHeads == nil {
		return "", errMapHeadIndexUnavailable
	}
	if set := f.mapHeads[prefix]; set != nil {
		return set.keys[0], nil
	}
	return "", nil
}
