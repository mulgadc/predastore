package meta

import (
	"bytes"
	"errors"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/tidwall/btree"
)

const (
	mapHeadObjectTable = "objects/arn:aws:s3:::"
	mapHeadKeyMarker   = "/map/head."
)

var errMapHeadIndexUnavailable = errors.New("map-head index unavailable")

// mapHeadIndex holds only the object keys used by the authority extension.
type mapHeadIndex map[string]*mapHeadSet

type mapHeadSet struct {
	keys *btree.BTreeG[string]
	last string
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
			set.keys.Delete(key)
			if set.keys.Len() == 0 {
				delete(heads, prefix)
				continue
			}
			if set.last == key {
				set.last, _ = set.keys.Max()
			}
			continue
		}

		if set == nil {
			set = &mapHeadSet{keys: btree.NewBTreeGOptions(
				func(a, b string) bool { return a < b },
				btree.Options{NoLocks: true},
			)}
			heads[prefix] = set
		}
		set.keys.Set(key)
		set.last = max(set.last, key)
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

func (f *FSM) mapHeadIndexReady() bool {
	if !f.mapHeadsMu.TryRLock() {
		return false
	}
	defer f.mapHeadsMu.RUnlock()
	return f.mapHeads != nil
}

// LastKey returns the greatest object-table map-head key under an exact prefix.
func (f *FSM) LastKey(prefix string) (string, error) {
	if !strings.HasPrefix(prefix, mapHeadObjectTable) || !strings.HasSuffix(prefix, mapHeadKeyMarker) {
		return "", errMapHeadIndexUnavailable
	}
	if !f.mapHeadsMu.TryRLock() {
		return "", errMapHeadIndexUnavailable
	}
	defer f.mapHeadsMu.RUnlock()
	if f.mapHeads == nil {
		return "", errMapHeadIndexUnavailable
	}
	if set := f.mapHeads[prefix]; set != nil {
		return set.last, nil
	}
	return "", nil
}
