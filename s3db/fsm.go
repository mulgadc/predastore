package s3db

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
)

// CommandType represents the type of operation
type CommandType uint8

const (
	CommandPut CommandType = iota
	CommandDelete
)

// Command represents a database operation that goes through Raft
type Command struct {
	Type  CommandType `json:"type"`
	Table string      `json:"table"`
	Key   []byte      `json:"key"` // []byte for safe JSON base64 encoding of binary keys
	Value []byte      `json:"value,omitempty"`
}

// FSM implements raft.FSM interface backed by Badger
type FSM struct {
	mu sync.RWMutex
	db *badger.DB
}

// NewFSM creates a new FSM with the given Badger database
func NewFSM(db *badger.DB) *FSM {
	return &FSM{db: db}
}

// Apply is called once a log entry is committed by Raft
// It applies the command to the Badger database
func (f *FSM) Apply(log *raft.Log) any {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("failed to unmarshal command: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd.Type {
	case CommandPut:
		return f.applyPut(cmd.Table, string(cmd.Key), cmd.Value)
	case CommandDelete:
		return f.applyDelete(cmd.Table, string(cmd.Key))
	default:
		return fmt.Errorf("unknown command type: %d", cmd.Type)
	}
}

// applyPut stores a key-value pair
func (f *FSM) applyPut(table, key string, value []byte) error {
	fullKey := makeKey(table, key)
	return f.db.Update(func(txn *badger.Txn) error {
		return txn.Set(fullKey, value)
	})
}

// applyDelete removes a key
func (f *FSM) applyDelete(table, key string) error {
	fullKey := makeKey(table, key)
	return f.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(fullKey)
	})
}

// Snapshot returns an FSMSnapshot for creating a point-in-time snapshot
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Collect all key-value pairs for the snapshot
	data := make(map[string][]byte)
	err := f.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.KeyCopy(nil))
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			data[key] = val
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &FSMSnapshot{data: data}, nil
}

// Restore restores the FSM from a snapshot
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	f.mu.Lock()
	defer f.mu.Unlock()

	// Decode the snapshot data
	var data map[string][]byte
	if err := json.NewDecoder(rc).Decode(&data); err != nil {
		return fmt.Errorf("failed to decode snapshot: %w", err)
	}

	// Clear existing data and restore from snapshot
	return f.db.Update(func(txn *badger.Txn) error {
		// Drop all existing data
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		var keysToDelete [][]byte
		for it.Rewind(); it.Valid(); it.Next() {
			keysToDelete = append(keysToDelete, it.Item().KeyCopy(nil))
		}
		it.Close()

		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}

		// Restore snapshot data
		for key, value := range data {
			if err := txn.Set([]byte(key), value); err != nil {
				return err
			}
		}
		return nil
	})
}

// Get reads a value from the local store (can be stale on non-leader)
func (f *FSM) Get(table, key string) ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	fullKey := makeKey(table, key)
	var value []byte
	err := f.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(fullKey)
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	})
	return value, err
}

// Scan iterates over keys with the given table and prefix
func (f *FSM) Scan(table, prefix string, fn func(key string, value []byte) error) error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	fullPrefix := makeKey(table, prefix)
	return f.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = fullPrefix
		it := txn.NewIterator(opts)
		defer it.Close()

		tablePrefix := table + "/"
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			fullKey := string(item.Key())
			// Strip table prefix to get the actual key
			key := fullKey[len(tablePrefix):]

			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			if err := fn(key, value); err != nil {
				return err
			}
		}
		return nil
	})
}

// FSMSnapshot implements raft.FSMSnapshot
type FSMSnapshot struct {
	data map[string][]byte
}

// Persist writes the snapshot to the given sink
func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data to JSON
		b, err := json.Marshal(s.data)
		if err != nil {
			return err
		}

		// Write to sink
		if _, err := sink.Write(b); err != nil {
			return err
		}

		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}
	return err
}

// Release is called when the snapshot is no longer needed
func (s *FSMSnapshot) Release() {}

// makeKey creates a composite key from table and key
func makeKey(table, key string) []byte {
	return []byte(table + "/" + key)
}
