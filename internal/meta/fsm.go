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
	// CommandTxn applies its writes only if every one of its checks holds,
	// and publishes all of them or none.
	CommandTxn
)

// Command represents a database operation that goes through Raft.
type Command struct {
	Type  CommandType `json:"type"`
	Key   []byte      `json:"key"` // []byte for safe JSON base64 encoding of binary keys
	Value []byte      `json:"value,omitempty"`
	Epoch uint64      `json:"epoch,omitempty"`
	// Checks and Writes are the body of a CommandTxn; other commands leave
	// them empty, so their encoding is unchanged.
	Checks []TxnCheck `json:"checks,omitempty"`
	Writes []TxnWrite `json:"writes,omitempty"`
}

// TxnCheck is one precondition of a transaction: Key holds exactly Value, or,
// with Absent set, Key holds nothing. An empty Value without Absent requires
// the key to be present with an empty value.
type TxnCheck struct {
	Key    []byte `json:"key"`
	Value  []byte `json:"value,omitempty"`
	Absent bool   `json:"absent,omitempty"`
}

// TxnWrite is one write of a transaction: Key is set to Value, or, with
// Delete set, removed.
type TxnWrite struct {
	Key    []byte `json:"key"`
	Value  []byte `json:"value,omitempty"`
	Delete bool   `json:"delete,omitempty"`
}

// TxnValue is what a checked key held when a transaction was evaluated.
// Exists is false for an absent key, which then has no Value. Oversized marks
// a value larger than MaxTxnBodyBytes, which no check can name and no reply
// carries, so it has no Value either.
type TxnValue struct {
	Key       []byte `json:"key"`
	Value     []byte `json:"value,omitempty"`
	Exists    bool   `json:"exists,omitempty"`
	Oversized bool   `json:"oversized,omitempty"`
}

// TxnResult is the FSM's answer to a CommandTxn it evaluated. On a mismatch,
// Current holds one entry per check, in check order.
type TxnResult struct {
	Applied bool
	Current []TxnValue
}

// ErrInvalidTxn reports a transaction that no store could apply as written.
var ErrInvalidTxn = errors.New("invalid meta transaction")

// Limits on one transaction. The gate sends at most five rows of one record
// each, so these leave wide headroom and still bound what a request can cost.
const (
	MaxTxnChecks    = 16
	MaxTxnWrites    = 16
	MaxTxnBodyBytes = 1 << 20
)

// ValidateTxn rejects a transaction whose checks or writes are malformed. The
// FSM, the server and the client run the same rules, so a replica never
// learns of a malformed transaction that the client could have refused.
func ValidateTxn(checks []TxnCheck, writes []TxnWrite) error {
	if len(writes) == 0 {
		return fmt.Errorf("%w: no writes", ErrInvalidTxn)
	}
	if len(checks) > MaxTxnChecks {
		return fmt.Errorf("%w: %d checks exceed the limit of %d", ErrInvalidTxn, len(checks), MaxTxnChecks)
	}
	if len(writes) > MaxTxnWrites {
		return fmt.Errorf("%w: %d writes exceed the limit of %d", ErrInvalidTxn, len(writes), MaxTxnWrites)
	}
	for i, c := range checks {
		if len(c.Key) == 0 {
			return fmt.Errorf("%w: check %d has no key", ErrInvalidTxn, i)
		}
		if c.Absent && len(c.Value) > 0 {
			return fmt.Errorf("%w: check %d requires both absence and a value", ErrInvalidTxn, i)
		}
		if IsReservedKey(c.Key) {
			return fmt.Errorf("%w: check %d: %w", ErrInvalidTxn, i, ErrReservedKey)
		}
		for j := range i {
			if bytes.Equal(checks[j].Key, c.Key) {
				return fmt.Errorf("%w: checks %d and %d name the same key", ErrInvalidTxn, j, i)
			}
		}
	}
	for i, w := range writes {
		if len(w.Key) == 0 {
			return fmt.Errorf("%w: write %d has no key", ErrInvalidTxn, i)
		}
		if w.Delete && len(w.Value) > 0 {
			return fmt.Errorf("%w: write %d is both a delete and a put", ErrInvalidTxn, i)
		}
		if IsReservedKey(w.Key) {
			return fmt.Errorf("%w: write %d: %w", ErrInvalidTxn, i, ErrReservedKey)
		}
	}
	return nil
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

	// persisted is the index the applied row names: the store already holds
	// every entry at or below it, so ApplyBatch skips them. Guarded by mu.
	persisted uint64

	// serving separates the restore raft performs while it is being
	// constructed from one a leader sends afterwards. Only the second is a
	// node catching up, and it is the one worth an alarm.
	serving atomic.Bool

	onKeyChanged func(key, value []byte)

	// commitBatch commits a batch's transaction. A test replaces it to fail a
	// commit on demand; production always commits the transaction.
	commitBatch func(*badger.Txn) error

	// copyValue reads the value of an item an entry reads. A test replaces it
	// to fail a read on demand.
	copyValue func(*badger.Item) ([]byte, error)

	limits txnLimits

	// row and cmd are scratch space that ApplyBatch reuses, guarded by mu.
	row appliedRow
	cmd Command
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

// OpenFSM creates an FSM over db and reads the applied index the store holds.
// A row that does not decode is an error: skipping entries against it could
// skip one the store does not hold.
func OpenFSM(db *badger.DB, opts ...FSMOption) (*FSM, error) {
	persisted, err := readApplied(db)
	if err != nil {
		return nil, err
	}
	f := &FSM{db: db, commitBatch: (*badger.Txn).Commit, copyValue: valueCopy, persisted: persisted, limits: newTxnLimits(db)}
	for _, opt := range opts {
		opt(f)
	}

	return f, nil
}

// NewFSM is OpenFSM for a store whose applied row is known to decode, and
// panics when it does not.
func NewFSM(db *badger.DB, opts ...FSMOption) *FSM {
	f, err := OpenFSM(db, opts...)
	if err != nil {
		panic(err) //nolint:forbidigo // A caller that cannot handle the error has nothing safe to do.
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
// retry or error response can say what the store holds.
func (f *FSM) ApplyBatch(logs []*raft.Log) []any {
	responses := make([]any, len(logs))
	if len(logs) == 0 {
		return responses
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	b := &batch{f: f, first: logs[0].Index, last: logs[len(logs)-1].Index, budget: newTxnBudget(f.limits)}
	b.begin()

	for i, log := range logs {
		// Every commit writes the applied row. An entry at or below it is
		// already in the store, as after a restart raft replays it, so it is
		// skipped and answers nil: no caller waits for a replayed entry.
		if (f.persisted != 0 && log.Index <= f.persisted) || log.Type != raft.LogCommand {
			continue
		}
		// Decoding into f.cmd keeps the command off the heap. It is cleared
		// after the copy, so it holds no value beyond this entry.
		f.cmd = Command{}
		err := json.Unmarshal(log.Data, &f.cmd)
		cmd := f.cmd
		f.cmd = Command{}
		if err != nil {
			responses[i] = fmt.Errorf("failed to unmarshal command: %w", err)
			continue
		}
		if cmd.Type == CommandTxn {
			// The batch transaction ends before the command and the next one
			// starts after it, so the command reads the entries before it and
			// the entries after it read the command's writes.
			b.end()
			responses[i] = f.applyTxn(cmd, log.Index, b)
			b.begin()
			continue
		}
		responses[i] = b.stage(cmd, log.Index)
	}
	b.end()
	f.applied.Store(logs[len(logs)-1].Index)

	return responses
}

// batch is the shared transaction of one ApplyBatch call.
type batch struct {
	f           *FSM
	first, last uint64
	txn         *badger.Txn
	budget      txnBudget
	// dirty is set once txn holds a write, and staged is the index of the
	// last entry that wrote.
	dirty  bool
	staged uint64
}

func (b *batch) begin() {
	b.txn, b.dirty, b.staged = b.f.db.NewTransaction(true), false, 0
	b.budget.reset()
}

// end commits the shared transaction with its applied row, or discards it
// when it holds no write, which leaves the store and the row as they were.
func (b *batch) end() {
	if !b.dirty {
		b.txn.Discard()
		return
	}
	b.commitOrStop(b.txn, b.staged)
}

// stage adds one command to the shared transaction, committing first when the
// command and the applied row would not both fit.
func (b *batch) stage(cmd Command, index uint64) any {
	if IsReservedKey(cmd.Key) {
		return fmt.Errorf("%w: %q", ErrReservedKey, cmd.Key)
	}
	cost, writes := b.budget.commandCost(cmd)
	if writes && !b.budget.fits(cost) && b.dirty {
		b.end()
		b.begin()
	}
	if writes && !b.budget.fits(cost) {
		return fmt.Errorf("meta: entry %d with the applied row: %w", index, badger.ErrTxnTooBig)
	}
	resp, err := b.f.applyCommand(b.txn, cmd)
	if errors.Is(err, badger.ErrTxnTooBig) && b.dirty {
		b.end()
		b.begin()
		resp, err = b.f.applyCommand(b.txn, cmd)
	}
	if err != nil {
		b.stopOnStoreError(b.txn, index, err)
		return err
	}
	b.budget.add(cost)
	b.dirty, b.staged = true, index
	return resp
}

// commitOrStop writes the applied row for index into txn and commits it.
func (b *batch) commitOrStop(txn *badger.Txn, index uint64) {
	if err := b.f.row.set(txn, index); err != nil {
		txn.Discard()
		b.stop(err)
	}
	b.commitStaged(txn, index)
}

// commitStaged commits txn, whose applied row names index, and stops the
// process when the commit fails.
func (b *batch) commitStaged(txn *badger.Txn, index uint64) {
	if err := b.f.commitBatch(txn); err != nil {
		b.stop(err)
	}
	b.f.persisted = index
}

func (b *batch) stop(err error) {
	//nolint:forbidigo // A part-applied commit cannot be answered; restart rebuilds the store from the log.
	panic(fmt.Sprintf("meta: committing applied entries %d-%d failed; stopping so the log rebuilds the store: %v",
		b.first, b.last, err))
}

// storeError is a failure of the local store while an entry is evaluated.
// Another replica can evaluate the same entry, so it is never the entry's
// answer.
type storeError struct{ err error }

func (e *storeError) Error() string { return e.err.Error() }

func (e *storeError) Unwrap() error { return e.err }

// stopOnStoreError stops the process when err is a storeError, discarding
// txn first so nothing staged commits. Answering instead would let a later
// commit move the applied row past an entry this store never applied.
func (b *batch) stopOnStoreError(txn *badger.Txn, index uint64, err error) {
	var se *storeError
	if !errors.As(err, &se) {
		return
	}
	txn.Discard()
	//nolint:forbidigo // An entry this store could not evaluate must not be skipped; restart applies it again.
	panic(fmt.Sprintf("meta: reading the store to apply entry %d failed; stopping so the log applies it again: %v",
		index, se.err))
}

func valueCopy(item *badger.Item) ([]byte, error) { return item.ValueCopy(nil) }

// readValue reads key in txn, reporting an absent key with found false. Any
// other failure is a storeError.
func (f *FSM) readValue(txn *badger.Txn, key []byte) (value []byte, found bool, err error) {
	item, err := txn.Get(key)
	switch {
	case errors.Is(err, badger.ErrKeyNotFound):
		return nil, false, nil
	case err != nil:
		return nil, false, &storeError{err}
	}
	if value, err = f.copyValue(item); err != nil {
		return nil, false, &storeError{err}
	}
	return value, true, nil
}

// applyTxn evaluates a CommandTxn in a badger transaction of its own. It
// commits, with the applied row, only when every check holds and every write
// and the row stage; otherwise it discards the transaction and writes nothing.
func (f *FSM) applyTxn(cmd Command, index uint64, b *batch) any {
	if err := ValidateTxn(cmd.Checks, cmd.Writes); err != nil {
		return err
	}
	txn := f.db.NewTransaction(true)
	result, err := f.stageTxn(txn, cmd)
	b.stopOnStoreError(txn, index, err)
	if err == nil && result.Applied {
		err = f.row.set(txn, index)
	}
	if err != nil || !result.Applied {
		txn.Discard()
		if err != nil {
			return fmt.Errorf("meta transaction not applied: %w", err)
		}
		return result
	}
	b.commitStaged(txn, index)
	return result
}

// stageTxn reads every checked key, then stages the writes only if all the
// checks hold. The result depends on the store and the command alone, and a
// failure to read the store is a storeError.
//
// A check value arrives inside a body of at most MaxTxnBodyBytes, so a row
// larger than that holds no checked value. It is a mismatch and is not read.
func (f *FSM) stageTxn(txn *badger.Txn, cmd Command) (TxnResult, error) {
	current := make([]TxnValue, len(cmd.Checks))
	holds := true
	for i, check := range cmd.Checks {
		current[i] = TxnValue{Key: check.Key}
		item, err := txn.Get(check.Key)
		switch {
		case errors.Is(err, badger.ErrKeyNotFound):
			holds = holds && check.Absent
			continue
		case err != nil:
			return TxnResult{}, &storeError{err}
		}
		current[i].Exists = true
		if item.ValueSize() > MaxTxnBodyBytes {
			current[i].Oversized, holds = true, false
			continue
		}
		value, err := f.copyValue(item)
		if err != nil {
			return TxnResult{}, &storeError{err}
		}
		current[i].Value = value
		holds = holds && !check.Absent && bytes.Equal(value, check.Value)
	}
	if !holds {
		return TxnResult{Current: current}, nil
	}
	for _, w := range cmd.Writes {
		var err error
		if w.Delete {
			err = txn.Delete(w.Key)
		} else {
			err = txn.Set(w.Key, w.Value)
		}
		if err != nil {
			return TxnResult{}, err
		}
	}
	return TxnResult{Applied: true}, nil
}

// applyCommand stages one command in txn and returns its response: the
// replaced value for CommandSwap, nil otherwise. A failure to read the store
// is a storeError.
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
func (f *FSM) applyCommand(txn *badger.Txn, cmd Command) (any, error) {
	switch cmd.Type {
	case CommandPut, CommandPutMax:
		return nil, txn.Set(cmd.Key, cmd.Value)
	case CommandDelete:
		return nil, txn.Delete(cmd.Key)
	case CommandSwap:
		previous, _, err := f.readValue(txn, cmd.Key)
		if err != nil {
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
	rs := &restoreState{budget: newTxnBudget(f.limits)}
	if sorted {
		err = f.restoreByMerge(r, started, rs)
	} else {
		err = f.restoreByReplace(r, started, rs)
	}
	if err == nil {
		err = f.restoreApplied(rs.applied)
	}
	if err != nil && rs.wrote {
		// The store is part old, part snapshot, and raft would go on without
		// a retry. The next start restores the snapshot before it skips any
		// entry.
		//nolint:forbidigo // A part-restored store must not apply or answer another entry.
		panic(fmt.Sprintf("meta: restoring a snapshot failed after it wrote to the store; stopping: %v", err))
	}

	return err
}

// restoreState is what a restore learns on its way: whether it may have
// changed the store, and the applied row the snapshot holds, nil for none.
type restoreState struct {
	wrote   bool
	applied []byte
	budget  txnBudget
}

// write counts one write sent to a WriteBatch. The batch commits a chunk only
// when its transaction is full, so until the count says it could be, the
// store is unchanged.
func (rs *restoreState) write(key, value []byte) {
	cost := rs.budget.cost(key, len(value))
	if !rs.budget.holds(cost) {
		rs.wrote = true
	}
	rs.budget.add(cost)
}

// takeApplied keeps the snapshot's applied row aside and reports whether key
// was that row, which the data pass then leaves out.
func (rs *restoreState) takeApplied(key, value []byte) bool {
	if !bytes.Equal(key, appliedKey) {
		return false
	}
	rs.applied = bytes.Clone(value)
	return true
}

// restoreApplied runs after every data row is flushed. It writes the
// snapshot's applied row, or removes the local one when the snapshot has
// none, then moves the in-memory index to match, backwards included.
func (f *FSM) restoreApplied(row []byte) error {
	var index uint64
	if row != nil {
		var err error
		if index, err = decodeApplied(row); err != nil {
			return fmt.Errorf("restore: %w", err)
		}
	}
	err := f.db.Update(func(txn *badger.Txn) error {
		if row == nil {
			return txn.Delete(appliedKey)
		}
		return txn.Set(appliedKey, row)
	})
	if err != nil {
		return fmt.Errorf("restore: write the applied index: %w", err)
	}
	f.persisted = index

	return nil
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
func (f *FSM) restoreByMerge(r *bufio.Reader, started time.Time, rs *restoreState) error {
	txn := f.db.NewTransaction(false)
	defer txn.Discard()

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	it.Rewind()

	wb := f.db.NewWriteBatch()
	defer wb.Cancel()

	var added, changed, deleted, unchanged int

	// valid reports whether the iterator is on a local data row, stepping
	// past the applied row, which restoreApplied writes after the data.
	valid := func() bool {
		if it.Valid() && bytes.Equal(it.Item().Key(), appliedKey) {
			it.Next()
		}
		return it.Valid()
	}
	drop := func() error {
		key := it.Item().KeyCopy(nil)
		rs.write(key, nil)
		deleted++
		return wb.Delete(key)
	}

	// Local keys the snapshot has passed are orphans: present here, absent
	// there, and therefore deleted in the state the snapshot describes.
	dropBefore := func(key []byte) error {
		for valid() && bytes.Compare(it.Item().Key(), key) < 0 {
			if err := drop(); err != nil {
				return err
			}
			it.Next()
		}
		return nil
	}

	err := streamSnapshot(r, func(key, value []byte) error {
		if rs.takeApplied(key, value) {
			return nil
		}
		if err := dropBefore(key); err != nil {
			return err
		}
		if valid() && bytes.Equal(it.Item().Key(), key) {
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

		rs.write(key, value)
		return wb.Set(key, value)
	})
	if err != nil {
		return err
	}

	for ; valid(); it.Next() {
		if err := drop(); err != nil {
			return err
		}
	}

	rs.wrote = true
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
func (f *FSM) restoreByReplace(r *bufio.Reader, started time.Time, rs *restoreState) error {
	// Every key is rewritten here, so a change set from this path would name
	// the whole store. Saying so beats reporting nothing, which a consumer
	// cannot tell from a snapshot that changed nothing.
	if f.onKeyChanged != nil {
		slog.Warn("meta: restore reports no change set",
			"node", f.node, "reason", "unsorted snapshot, restored by replacement")
	}

	rs.wrote = true
	if err := f.db.DropAll(); err != nil {
		return fmt.Errorf("restore: drop existing data: %w", err)
	}

	wb := f.db.NewWriteBatch()
	defer wb.Cancel()

	var entries int
	err := streamSnapshot(r, func(key, value []byte) error {
		if rs.takeApplied(key, value) {
			return nil
		}
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

// Get reads a value from the local store (can be stale on non-leader). A
// reserved key is never found.
func (f *FSM) Get(key string) ([]byte, error) {
	if IsReservedKey([]byte(key)) {
		return nil, badger.ErrKeyNotFound
	}
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
// beginning of the prefix. Reserved keys are never returned.
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
			if IsReservedKey(item.Key()) {
				continue
			}
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
