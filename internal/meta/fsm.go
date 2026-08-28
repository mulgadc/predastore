package meta

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
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

// Snapshot captures a point-in-time view of the store and returns.
//
// raft's own contract asks for exactly this: Apply cannot run while Snapshot
// is running, so the work belongs in Persist, and a snapshot may be discarded
// without Persist ever being called. A badger read transaction is that view --
// MVCC, so it costs nothing to take and nothing to hold beyond the versions it
// pins.
//
// It walked the whole keyspace into a map before, under a read lock, which
// stalled every metadata write for as long as the walk took and allocated the
// entire store to produce a result raft was free to throw away.
//
// The capture is timed in microseconds because a healthy one is tens of them,
// and Milliseconds would truncate every one to zero.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	started := time.Now()
	index := f.applied.Load()

	snap := &FSMSnapshot{node: f.node, index: index, txn: f.db.NewTransaction(false)}
	slog.Info("meta: snapshot captured",
		"node", f.node, "index", index, "duration_us", time.Since(started).Microseconds())

	return snap, nil
}

// Restore rebuilds the FSM from a snapshot written by FSMSnapshot.Persist.
//
// A stream that declares itself sorted is merged into the existing store: only
// what differs is written, and nothing is dropped first. An unsorted one --
// the legacy JSON map, or a frame stream from a version that iterated a Go map
// -- cannot be merged in one pass, so it takes the older path of clearing the
// store and rewriting it.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	started := time.Now()

	// A restore before the replica serves is raft rebuilding local state on
	// start. One after it is a leader sending a snapshot because this node
	// fell outside the log it retains, which is the event an operator wants
	// to know about and the one a test asserts the path on.
	install := f.serving.Load()
	if install {
		slog.Warn("meta: catching up by snapshot install", "node", f.node)
	}
	slog.Info("meta: restoring from snapshot", "node", f.node, "install", install)

	f.mu.Lock()
	defer f.mu.Unlock()

	r := bufio.NewReader(rc)
	sorted, err := snapshotIsSorted(r)
	if err != nil {
		return err
	}
	if sorted {
		return f.restoreByMerge(r, started)
	}

	return f.restoreByReplace(r, started)
}

// snapshotIsSorted reports whether the stream carries the marker promising key
// order, consuming the marker when it does.
func snapshotIsSorted(r *bufio.Reader) (bool, error) {
	head, err := r.Peek(len(snapshotMagic))
	if err != nil && !errors.Is(err, io.EOF) {
		return false, fmt.Errorf("peek snapshot: %w", err)
	}
	if len(head) < len(snapshotMagic) || !bytes.Equal(head, snapshotMagic[:]) {
		return false, nil
	}
	if _, err := r.Discard(len(snapshotMagic)); err != nil {
		return false, fmt.Errorf("read snapshot marker: %w", err)
	}

	return true, nil
}

// restoreByMerge walks the snapshot and the local store together, both in key
// order, and writes only the difference.
//
// Snapshot wins every comparison, and that needs no policy behind it: a
// snapshot is a prefix of the committed log, and raft has already truncated
// any divergent suffix before Restore is called. There is no case where the
// local value should survive.
//
// It is O(1) in memory on both sides, and it never passes through an empty
// store -- which the drop-and-rewrite path does, so a crash in the middle of
// one left a node with no metadata at all.
func (f *FSM) restoreByMerge(r *bufio.Reader, started time.Time) error {
	txn := f.db.NewTransaction(false)
	defer txn.Discard()

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	it.Rewind()

	wb := f.db.NewWriteBatch()
	defer wb.Cancel()

	var added, changed, deleted, unchanged int

	// Local keys the snapshot has passed are orphans: present here, absent
	// there, and therefore deleted in the state the snapshot describes.
	dropBefore := func(key []byte) error {
		for it.Valid() && bytes.Compare(it.Item().Key(), key) < 0 {
			if err := wb.Delete(it.Item().KeyCopy(nil)); err != nil {
				return err
			}
			deleted++
			it.Next()
		}
		return nil
	}

	err := streamSnapshot(r, func(key, value []byte) error {
		if err := dropBefore(key); err != nil {
			return err
		}
		if it.Valid() && bytes.Equal(it.Item().Key(), key) {
			local, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			it.Next()
			if bytes.Equal(local, value) {
				unchanged++
				return nil
			}
			changed++
		} else {
			added++
		}

		return wb.Set(key, value)
	})
	if err != nil {
		return err
	}

	for ; it.Valid(); it.Next() {
		if err := wb.Delete(it.Item().KeyCopy(nil)); err != nil {
			return err
		}
		deleted++
	}

	if err := wb.Flush(); err != nil {
		return fmt.Errorf("restore: flush merge: %w", err)
	}

	slog.Info("meta: snapshot restored",
		"node", f.node, "entries", added+changed+unchanged,
		"added", added, "changed", changed, "deleted", deleted, "unchanged", unchanged,
		"duration_ms", time.Since(started).Milliseconds())

	return nil
}

// restoreByReplace clears the store and rewrites it, which is the only thing
// that can be done with a stream whose order is unknown.
//
// Clear and rewrite used to run inside a single db.Update. Badger caps how much
// one transaction may hold, so once the metadata set outgrew that cap every
// snapshot became permanently unrestorable:
//
//	raft: snapshot restore progress: ... percent-complete="100.00%"
//	raft: failed to restore snapshot: error="Txn is too big to fit into one request"
//	failed to create raft node: failed to load any existing snapshots
//
// and the node could never start again -- on every node at once, since they all
// restore the same oversized snapshot, taking the metadata plane (and with it
// the AMI catalogue, so DescribeImages) down with it. The write path has no
// matching limit, so snapshots that cannot be read back are written happily.
// See mulga-tjoz9.
//
// DropAll is badger's own bulk clear and is not bounded by a transaction;
// WriteBatch commits in chunks as it fills.
func (f *FSM) restoreByReplace(r *bufio.Reader, started time.Time) error {
	if err := f.db.DropAll(); err != nil {
		return fmt.Errorf("restore: drop existing data: %w", err)
	}

	wb := f.db.NewWriteBatch()
	defer wb.Cancel()

	var entries int
	err := streamSnapshot(r, func(key, value []byte) error {
		entries++
		return wb.Set(key, value)
	})
	if err != nil {
		return err
	}

	if err := wb.Flush(); err != nil {
		return fmt.Errorf("restore: flush batch: %w", err)
	}

	slog.Info("meta: snapshot restored",
		"node", f.node, "entries", entries, "replaced", true,
		"duration_ms", time.Since(started).Milliseconds())

	return nil
}

// streamSnapshot yields every key/value pair in the stream, without holding
// the stream in memory. The marker, if there was one, has already been read.
//
// It also accepts the legacy JSON snapshot format (a single map object) so a
// node upgraded on top of a store with pre-existing snapshots still starts.
// Legacy snapshots lost binary keys to U+FFFD substitution before the upgrade;
// they are decoded as-is, with no recovery, and future snapshots are written in
// the frame format. That one path does hold the snapshot in memory, because a
// JSON object cannot be read any other way -- it is a compatibility path for
// snapshots written by a version that could not produce a large one.
func streamSnapshot(r *bufio.Reader, fn func(key, value []byte) error) error {
	first, err := r.Peek(1)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil // empty snapshot
		}
		return fmt.Errorf("peek snapshot: %w", err)
	}

	if first[0] == '{' {
		var data map[string][]byte
		if err := json.NewDecoder(r).Decode(&data); err != nil {
			return fmt.Errorf("decode legacy json snapshot: %w", err)
		}
		for k, v := range data {
			if err := fn([]byte(k), v); err != nil {
				return err
			}
		}
		return nil
	}

	var lenBuf [4]byte
	read := func() ([]byte, error) {
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return nil, err
		}
		buf := make([]byte, binary.BigEndian.Uint32(lenBuf[:]))
		_, err := io.ReadFull(r, buf)
		return buf, err
	}

	for {
		// A clean EOF on the frame boundary ends the stream; a short read
		// mid-frame is a truncated snapshot and must surface as an error, not
		// a silent stop.
		key, err := read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("read key: %w", err)
		}
		value, err := read()
		if err != nil {
			return fmt.Errorf("read value: %w", err)
		}
		if err := fn(key, value); err != nil {
			return err
		}
	}

	return nil
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

// snapshotMagic marks a stream whose frames are in key order. Restoring by
// merge is only correct against a sorted stream, and two unsorted formats are
// already in the wild: the legacy JSON map, and the frame stream this file
// produced while it iterated a Go map. The first byte tells all three apart --
// a JSON snapshot starts with '{', a key length starts with 0x00 for any key
// badger accepts, and this starts with neither.
var snapshotMagic = [4]byte{0xFF, 'P', 'D', 'S'}

// FSMSnapshot implements raft.FSMSnapshot over a badger read transaction.
type FSMSnapshot struct {
	node  config.NodeID
	index uint64
	txn   *badger.Txn
}

// Persist writes the snapshot to the given sink: the magic, then a stream of
// length-prefixed key/value frames, BE uint32 keyLen, key, BE uint32 valLen,
// value.
//
// The keys are raw badger keys, and object metadata hash rows are keyed
// "objects/"+sha256, which is not valid UTF-8. A JSON or other text encoding
// silently rewrites those bytes to U+FFFD and loses the row on restore, so the
// wire format must preserve keys byte-for-byte.
//
// This is where a snapshot's cost belongs, and it runs concurrently with Apply
// rather than blocking it. Iterating badger also emits the frames in key
// order, which is what the merging restore below needs.
func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	started := time.Now()
	var entries int
	var written int64

	err := func() error {
		w := bufio.NewWriter(sink)
		if _, err := w.Write(snapshotMagic[:]); err != nil {
			return err
		}

		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := s.txn.NewIterator(opts)
		defer it.Close()

		var lenBuf [4]byte
		frame := func(b []byte) error {
			binary.BigEndian.PutUint32(lenBuf[:], uint32(len(b))) //nolint:gosec // bounded by badger's key and value size limits.
			if _, err := w.Write(lenBuf[:]); err != nil {
				return err
			}
			_, err := w.Write(b)
			return err
		}

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if err := frame(item.Key()); err != nil {
				return err
			}
			if err := frame(value); err != nil {
				return err
			}
			entries++
			written += int64(8+len(value)) + item.KeySize()
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
		"node", s.node, "index", s.index, "entries", entries, "bytes", written,
		"duration_ms", time.Since(started).Milliseconds())

	return nil
}

// Release discards the read transaction. Holding one keeps every version it
// can see alive, so a snapshot that is never released pins the store against
// compaction.
func (s *FSMSnapshot) Release() {
	s.txn.Discard()
}
