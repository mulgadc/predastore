package s3db

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
)

// CommandType represents the type of operation.
type CommandType uint8

const (
	CommandPut CommandType = iota
	CommandDelete
)

// Command represents a database operation that goes through Raft.
type Command struct {
	Type  CommandType `json:"type"`
	Table string      `json:"table"`
	Key   []byte      `json:"key"` // []byte for safe JSON base64 encoding of binary keys
	Value []byte      `json:"value,omitempty"`
}

// FSM implements raft.FSM interface backed by Badger.
type FSM struct {
	mu sync.RWMutex
	db *badger.DB
}

// NewFSM creates a new FSM with the given Badger database.
func NewFSM(db *badger.DB) *FSM {
	return &FSM{db: db}
}

// Apply is called once a log entry is committed by Raft
// It applies the command to the Badger database.
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

// applyPut stores a key-value pair.
func (f *FSM) applyPut(table, key string, value []byte) error {
	fullKey := makeKey(table, key)
	return f.db.Update(func(txn *badger.Txn) error {
		return txn.Set(fullKey, value)
	})
}

// applyDelete removes a key.
func (f *FSM) applyDelete(table, key string) error {
	fullKey := makeKey(table, key)
	return f.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(fullKey)
	})
}

// Snapshot returns an FSMSnapshot for creating a point-in-time snapshot.
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

// snapshotEntry is one key/value pair read from a snapshot stream.
type snapshotEntry struct{ key, value []byte }

// Restore restores the FSM from a snapshot written by FSMSnapshot.Persist,
// reading the length-prefixed key/value frames back byte-exact.
//
// It also reads the legacy JSON snapshot format (a single map object) so a node
// upgraded on top of a store with pre-existing snapshots still starts. Legacy
// snapshots lost binary keys to U+FFFD substitution before the upgrade; they are
// decoded as-is (no recovery) and future snapshots are written in the frame
// format.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	f.mu.Lock()
	defer f.mu.Unlock()

	r := bufio.NewReader(rc)
	entries, err := readSnapshot(r)
	if err != nil {
		return err
	}

	// Clear, then rewrite in batches.
	//
	// Both halves used to run inside a single db.Update. Badger caps how much
	// one transaction may hold, so once the metadata set outgrew that cap every
	// snapshot became permanently unrestorable:
	//
	//   raft: snapshot restore progress: ... percent-complete="100.00%"
	//   raft: failed to restore snapshot: error="Txn is too big to fit into one request"
	//   failed to create raft node: failed to load any existing snapshots
	//
	// and the node could never start again — on every node at once, since they
	// all restore the same oversized snapshot, taking the metadata plane (and
	// with it the AMI catalogue, so DescribeImages) down with it. The write path
	// has no matching limit, so snapshots that cannot be read back are written
	// happily. See mulga-tjoz9.
	//
	// DropAll is badger's own bulk clear and is not bounded by a transaction;
	// WriteBatch commits in chunks as it fills, so restore cost no longer scales
	// into a hard wall.
	if err := f.db.DropAll(); err != nil {
		return fmt.Errorf("restore: drop existing data: %w", err)
	}

	wb := f.db.NewWriteBatch()
	defer wb.Cancel()

	for _, e := range entries {
		if err := wb.Set(e.key, e.value); err != nil {
			return fmt.Errorf("restore: set key: %w", err)
		}
	}

	if err := wb.Flush(); err != nil {
		return fmt.Errorf("restore: flush batch: %w", err)
	}
	return nil
}

// readSnapshot reads snapshot entries, accepting both the current binary frame
// format and the legacy JSON map format.
func readSnapshot(r *bufio.Reader) ([]snapshotEntry, error) {
	// Legacy snapshots are a JSON object, so they begin with '{'. A frame stream
	// begins with a big-endian key length whose high byte is 0x00 for any
	// realistic key (< 16 MiB), never '{', so the first byte disambiguates.
	first, err := r.Peek(1)
	if err != nil {
		if err == io.EOF {
			return nil, nil // empty snapshot
		}
		return nil, fmt.Errorf("peek snapshot: %w", err)
	}
	if first[0] == '{' {
		var data map[string][]byte
		if err := json.NewDecoder(r).Decode(&data); err != nil {
			return nil, fmt.Errorf("decode legacy json snapshot: %w", err)
		}
		entries := make([]snapshotEntry, 0, len(data))
		for k, v := range data {
			entries = append(entries, snapshotEntry{key: []byte(k), value: v})
		}
		return entries, nil
	}

	var entries []snapshotEntry
	var lenBuf [4]byte
	for {
		// A clean EOF on the frame boundary ends the stream; a short read mid-frame
		// is a truncated snapshot and must surface as an error, not a silent stop.
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("read key length: %w", err)
		}
		key := make([]byte, binary.BigEndian.Uint32(lenBuf[:]))
		if _, err := io.ReadFull(r, key); err != nil {
			return nil, fmt.Errorf("read key: %w", err)
		}
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return nil, fmt.Errorf("read value length: %w", err)
		}
		value := make([]byte, binary.BigEndian.Uint32(lenBuf[:]))
		if _, err := io.ReadFull(r, value); err != nil {
			return nil, fmt.Errorf("read value: %w", err)
		}
		entries = append(entries, snapshotEntry{key: key, value: value})
	}
	return entries, nil
}

// Get reads a value from the local store (can be stale on non-leader).
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

// Scan iterates over keys with the given table and prefix.
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

// FSMSnapshot implements raft.FSMSnapshot.
type FSMSnapshot struct {
	data map[string][]byte
}

// Persist writes the snapshot to the given sink as a stream of length-prefixed
// key/value frames: BE uint32 keyLen, key, BE uint32 valLen, value.
//
// The keys are raw badger keys, and object metadata hash rows are keyed
// "objects/"+sha256, which is not valid UTF-8. A JSON or other text encoding
// silently rewrites those bytes to U+FFFD and loses the row on restore, so the
// wire format must preserve keys byte-for-byte.
func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		w := bufio.NewWriter(sink)
		var lenBuf [4]byte
		for k, v := range s.data {
			binary.BigEndian.PutUint32(lenBuf[:], uint32(len(k))) //nolint:gosec // key length is bounded by badger's key-size limit.
			if _, err := w.Write(lenBuf[:]); err != nil {
				return err
			}
			if _, err := w.WriteString(k); err != nil {
				return err
			}
			binary.BigEndian.PutUint32(lenBuf[:], uint32(len(v))) //nolint:gosec // value length is bounded by badger's value-size limit.
			if _, err := w.Write(lenBuf[:]); err != nil {
				return err
			}
			if _, err := w.Write(v); err != nil {
				return err
			}
		}
		if err := w.Flush(); err != nil {
			return err
		}
		return sink.Close()
	}()

	if err != nil {
		if cerr := sink.Cancel(); cerr != nil {
			slog.Warn("Failed to cancel snapshot sink", "error", cerr)
		}
	}
	return err
}

// Release is called when the snapshot is no longer needed.
func (s *FSMSnapshot) Release() {}

// makeKey creates a composite key from table and key.
func makeKey(table, key string) []byte {
	return []byte(table + "/" + key)
}
