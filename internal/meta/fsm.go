package meta

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"github.com/mulgadc/predastore/internal/config"
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
	Key   []byte      `json:"key"` // []byte for safe JSON base64 encoding of binary keys
	Value []byte      `json:"value,omitempty"`
}

// FSM implements raft.FSM interface backed by Badger.
type FSM struct {
	mu sync.RWMutex
	db *badger.DB

	// node labels the snapshot lifecycle logs, which are otherwise
	// indistinguishable between the replicas colocated in one process.
	node config.NodeID

	// applied is the last index Apply saw. raft does not tell Snapshot which
	// index it is capturing, so a snapshot log line can only name one if the
	// FSM tracks it.
	applied atomic.Uint64

	// serving separates the restore raft performs while it is being
	// constructed from one a leader sends afterwards. Only the second is a
	// node catching up, and it is the one worth an alarm.
	serving atomic.Bool
}

// NewFSM creates a new FSM with the given Badger database.
func NewFSM(db *badger.DB) *FSM {
	return &FSM{db: db}
}

// Apply is called once a log entry is committed by Raft
// It applies the command to the Badger database.
func (f *FSM) Apply(log *raft.Log) any {
	f.applied.Store(log.Index)

	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("failed to unmarshal command: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd.Type {
	case CommandPut:
		return f.applyPut(string(cmd.Key), cmd.Value)
	case CommandDelete:
		return f.applyDelete(string(cmd.Key))
	default:
		return fmt.Errorf("unknown command type: %d", cmd.Type)
	}
}

// applyPut stores a key-value pair.
func (f *FSM) applyPut(key string, value []byte) error {
	return f.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), value)
	})
}

// applyDelete removes a key.
func (f *FSM) applyDelete(key string) error {
	return f.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

// Snapshot returns an FSMSnapshot for creating a point-in-time snapshot.
//
// The capture is timed in microseconds rather than milliseconds on purpose.
// raft's contract is that this returns immediately and the cost falls in
// Persist, so a healthy capture is tens of microseconds and Milliseconds would
// truncate every one of them to zero.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	started := time.Now()

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

	index := f.applied.Load()
	slog.Info("meta: snapshot captured",
		"node", f.node, "index", index, "entries", len(data),
		"duration_us", time.Since(started).Microseconds())

	return &FSMSnapshot{node: f.node, index: index, data: data}, nil
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

	started := time.Now()

	// A restore before the replica serves is raft rebuilding local state on
	// start. One after it is a leader sending a snapshot because this node
	// fell outside the log it retains, which is the event an operator wants
	// to know about and the one a test asserts the path on.
	if f.serving.Load() {
		slog.Warn("meta: catching up by snapshot install", "node", f.node)
	}
	slog.Info("meta: restoring from snapshot", "node", f.node, "install", f.serving.Load())

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

	// The store was dropped and rewritten, so every entry counts as written and
	// there is nothing to report as unchanged. Restoring by merge is what makes
	// those counts meaningful, and it adds them here when it lands.
	slog.Info("meta: snapshot restored",
		"node", f.node, "entries", len(entries),
		"duration_ms", time.Since(started).Milliseconds())

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
func (f *FSM) Get(key string) ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	var value []byte
	err := f.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	})
	return value, err
}

// Scan iterates over every key with the given prefix, passing each stored key
// through verbatim. Namespacing keys is the caller's business.
func (f *FSM) Scan(prefix string, fn func(key string, value []byte) error) error {
	return f.ScanFrom(prefix, "", fn)
}

// ScanFrom iterates over keys with the prefix that sort strictly after the
// cursor. Badger iterates in key order, so seeking past the last key a page
// returned is the whole of the continuation: an empty cursor starts at the
// beginning of the prefix.
func (f *FSM) ScanFrom(prefix, after string, fn func(key string, value []byte) error) error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)
		it := txn.NewIterator(opts)
		defer it.Close()

		// Seek lands on the cursor itself when it still exists, so the first
		// key is skipped explicitly rather than by seeking to a successor this
		// would have to synthesise.
		if after != "" {
			it.Seek([]byte(after))
			if it.Valid() && string(it.Item().Key()) == after {
				it.Next()
			}
		} else {
			it.Rewind()
		}

		for ; it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())

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
	node  config.NodeID
	index uint64
	data  map[string][]byte
}

// Persist writes the snapshot to the given sink as a stream of length-prefixed
// key/value frames: BE uint32 keyLen, key, BE uint32 valLen, value.
//
// The keys are raw badger keys, and object metadata hash rows are keyed
// "objects/"+sha256, which is not valid UTF-8. A JSON or other text encoding
// silently rewrites those bytes to U+FFFD and loses the row on restore, so the
// wire format must preserve keys byte-for-byte.
func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	started := time.Now()
	var written int64

	err := func() error {
		w := bufio.NewWriter(sink)
		var lenBuf [4]byte
		for k, v := range s.data {
			written += int64(8 + len(k) + len(v))
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
		return err
	}

	slog.Info("meta: snapshot persisted",
		"node", s.node, "index", s.index, "entries", len(s.data), "bytes", written,
		"duration_ms", time.Since(started).Milliseconds())

	return nil
}

// Release is called when the snapshot is no longer needed.
func (s *FSMSnapshot) Release() {}
