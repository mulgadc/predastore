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
	CommandPutMax
	// CommandSwap stores a value and reports the one it replaced, so a writer
	// learns what it superseded. Appended rather than inserted: these numbers
	// are on disk in every existing log.
	CommandSwap
)

// Command represents a database operation that goes through Raft.
type Command struct {
	Type  CommandType `json:"type"`
	Key   []byte      `json:"key"` // []byte for safe JSON base64 encoding of binary keys
	Value []byte      `json:"value,omitempty"`
	Epoch uint64      `json:"epoch,omitempty"`
}

// FSM implements raft.FSM interface backed by Badger.
type FSM struct {
	mapHeadsMu sync.RWMutex
	mapHeads   mapHeadIndex

	mu            sync.RWMutex
	db            *badger.DB
	restoreUnsafe bool

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

	onKeyChanged func(key, value []byte)

	// commitBatch commits a batch's transaction. A test replaces it to fail a
	// commit on demand; production always commits the transaction.
	commitBatch func(*badger.Txn) error
}

// FSMOption configures an FSM at construction. The FSM stays a pure state
// machine either way: it reports what changed and never decides what to do
// about it.
type FSMOption func(*FSM)

// WithNode labels this replica in the snapshot lifecycle logs, which are
// otherwise indistinguishable between the replicas colocated in one process.
func WithNode(id config.NodeID) FSMOption {
	return func(f *FSM) { f.node = id }
}

// OnKeyChanged registers a callback for every key a merging restore adds or
// changes. That set is exactly the objects whose shards may have moved while
// this node was outside the log, which the repair sweep otherwise has to
// rediscover by scanning the whole placement table.
//
// It is called as the merge encounters each key, before the batch is flushed,
// because buffering the set until the write commits would cost the memory the
// streaming restore exists to avoid. So a failed restore may have reported keys
// it did not commit. That is the safe direction: raft retries the restore, and
// repairing an object that did not need it costs a read.
//
// key and value are valid for the duration of the call only.
func OnKeyChanged(fn func(key, value []byte)) FSMOption {
	return func(f *FSM) { f.onKeyChanged = fn }
}

// NewFSM creates a new FSM with the given Badger database.
func NewFSM(db *badger.DB, opts ...FSMOption) *FSM {
	f := &FSM{db: db, commitBatch: (*badger.Txn).Commit}
	for _, opt := range opts {
		opt(f)
	}

	return f
}

// Apply applies one committed entry. It is the batch path with a batch of one,
// so a replica applies an entry the same way whichever path raft takes.
func (f *FSM) Apply(log *raft.Log) any {
	return f.ApplyBatch([]*raft.Log{log})[0]
}

// ApplyBatch applies committed entries in order inside as few badger
// transactions as fit, so a batch costs one synced commit instead of one per
// entry. Each entry still gets its own response, as Apply would have given it.
//
// A failed commit stops the process. Badger may have applied part of it, so no
// retry or error response can say what the store holds; a restart restores the
// snapshot and replays the log, which rebuilds the store exactly.
func (f *FSM) ApplyBatch(logs []*raft.Log) []any {
	responses := make([]any, len(logs))
	if len(logs) == 0 {
		return responses
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	var mapHeadChanges []Command
	txn := f.db.NewTransaction(true)
	commit := func() {
		if err := f.commitBatch(txn); err != nil {
			//nolint:forbidigo // A part-applied commit cannot be answered; restart rebuilds the store from the log.
			panic(fmt.Sprintf("meta: committing applied entries %d-%d failed; stopping so the log rebuilds the store: %v",
				logs[0].Index, logs[len(logs)-1].Index, err))
		}
	}

	for i, log := range logs {
		if log.Type != raft.LogCommand {
			continue
		}
		var cmd Command
		if err := json.Unmarshal(log.Data, &cmd); err != nil {
			responses[i] = fmt.Errorf("failed to unmarshal command: %w", err)
			continue
		}
		resp, err := applyCommand(txn, cmd)
		if errors.Is(err, badger.ErrTxnTooBig) {
			commit()
			txn = f.db.NewTransaction(true)
			resp, err = applyCommand(txn, cmd)
		}
		if err != nil {
			responses[i] = err
			continue
		}
		responses[i] = resp
		if isMapHeadObject(cmd.Key) {
			mapHeadChanges = append(mapHeadChanges, cmd)
		}
	}
	commit()
	f.applyMapHeadChanges(mapHeadChanges)
	f.applied.Store(logs[len(logs)-1].Index)

	return responses
}

// applyCommand stages one command in txn and returns its response: the
// replaced value for CommandSwap, nil otherwise.
//
// CommandPutMax stores a placement record, and the epoch it carries orders
// nothing. It exists so log entries written when this command did compare
// epochs still replay, and new writers should use CommandPut. A client's PUT
// is not acknowledged until its record reaches the log, so arrival order is
// acknowledgement order, and last-to-arrive is the write S3 calls the winner.
//
// CommandSwap reads before writing, through the same transaction, so it sees
// the writes earlier in its batch. What the read buys is a writer knowing
// which generation it displaced: the log settles who wins, and a caller that
// reads the key itself beforehand races every other writer of that key.
func applyCommand(txn *badger.Txn, cmd Command) (any, error) {
	switch cmd.Type {
	case CommandPut, CommandPutMax:
		return nil, txn.Set(cmd.Key, cmd.Value)
	case CommandDelete:
		return nil, txn.Delete(cmd.Key)
	case CommandSwap:
		var previous []byte
		switch item, err := txn.Get(cmd.Key); {
		case err == nil:
			if previous, err = item.ValueCopy(nil); err != nil {
				return nil, err
			}
		case !errors.Is(err, badger.ErrKeyNotFound):
			return nil, err
		}
		if err := txn.Set(cmd.Key, cmd.Value); err != nil {
			return nil, err
		}
		return previous, nil
	default:
		return nil, fmt.Errorf("unknown command type: %d", cmd.Type)
	}
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

	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.restoreUnsafe {
		return nil, errors.New("meta: snapshot capture refused after failed restore")
	}
	index := f.applied.Load()

	snap := &FSMSnapshot{node: f.node, index: index, txn: f.db.NewTransaction(false)}
	slog.Info("meta: snapshot captured",
		"node", f.node, "index", index, "duration_us", time.Since(started).Microseconds())

	return snap, nil
}

// Restore merges a sorted snapshot into the local metadata store.
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

	r := bufio.NewReader(rc)
	var marker [len(snapshotMagic)]byte
	if _, err := io.ReadFull(r, marker[:]); err != nil {
		return fmt.Errorf("unsupported metadata snapshot format: %w", err)
	}
	if marker != snapshotMagic {
		return errors.New("unsupported metadata snapshot format")
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.restoreUnsafe = true
	f.setMapHeads(nil)
	if err := f.restoreByMerge(r, started); err != nil {
		return err
	}
	if err := f.rebuildMapHeads(); err != nil {
		return err
	}
	f.restoreUnsafe = false
	return nil
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
		if f.onKeyChanged != nil {
			f.onKeyChanged(key, value)
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

// streamSnapshot reads sorted key-value frames after the format marker.
func streamSnapshot(r *bufio.Reader, fn func(key, value []byte) error) error {
	var lenBuf [4]byte
	readBody := func() ([]byte, error) {
		buf := make([]byte, binary.BigEndian.Uint32(lenBuf[:]))
		_, err := io.ReadFull(r, buf)
		return buf, err
	}

	for {
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("read key length: %w", err)
		}
		key, err := readBody()
		if err != nil {
			return fmt.Errorf("read key: %w", err)
		}
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return fmt.Errorf("read value length: %w", err)
		}
		value, err := readBody()
		if err != nil {
			return fmt.Errorf("read value: %w", err)
		}
		if err := fn(key, value); err != nil {
			return err
		}
	}
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

// snapshotMagic identifies snapshots with frames in key order.
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
