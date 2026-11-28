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

// commandTypeUnset fills the type before a decode, so an entry with no type,
// or a null one, keeps a value that no release knows and is never a put.
const commandTypeUnset = ^CommandType(0)

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

// TxnValue is what a checked key held when a transaction was evaluated. An
// absent key has Exists false, and a value over MaxTxnBodyBytes is Oversized,
// which no check can match. Neither carries a Value.
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
	applied         atomic.Uint64
	authorityWaiter atomic.Pointer[chan struct{}]

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

	var mapHeadChanges []Command
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
		f.cmd = Command{Type: commandTypeUnset}
		err := json.Unmarshal(log.Data, &f.cmd)
		cmd := f.cmd
		f.cmd = Command{}
		if err != nil || cmd.Type > CommandTxn {
			b.stopOnUnreadableEntry(log.Index, log.Data, cmd.Type, err)
		}
		if cmd.Type == CommandTxn {
			// The batch transaction ends before the command and the next one
			// starts after it, so the command reads the entries before it and
			// the entries after it read the command's writes.
			b.end()
			responses[i] = f.applyTxn(cmd, log.Index, b)
			if result, ok := responses[i].(TxnResult); ok && result.Applied {
				for _, write := range cmd.Writes {
					if isMapHeadObject(write.Key) {
						cmdType := CommandPut
						if write.Delete {
							cmdType = CommandDelete
						}
						mapHeadChanges = append(mapHeadChanges, Command{Type: cmdType, Key: write.Key})
					}
				}
			}
			b.begin()
			continue
		}
		responses[i] = b.stage(cmd, log.Index)
		if _, failed := responses[i].(error); !failed && isMapHeadObject(cmd.Key) {
			mapHeadChanges = append(mapHeadChanges, Command{Type: cmd.Type, Key: cmd.Key})
		}
	}
	b.end()
	f.applyMapHeadChanges(mapHeadChanges)
	f.applied.Store(logs[len(logs)-1].Index)
	f.signalAuthorityApply()

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

// stopOnUnreadableEntry stops the process on a committed entry this release
// cannot read. A replica that can read it applies it, so answering and moving
// on would leave the stores different. The message names no key.
func (b *batch) stopOnUnreadableEntry(index uint64, data []byte, typ CommandType, err error) {
	b.txn.Discard()
	reason := fmt.Sprintf("type %d", typ)
	switch {
	case err != nil:
		reason = fmt.Sprintf("an encoding that does not decode (%s)", decodeErrorKind(err))
	case typ == commandTypeUnset && !hasType(data):
		reason = "no type"
	}
	//nolint:forbidigo // Skipping an entry another replica applies diverges the stores; restart applies it again.
	panic(fmt.Sprintf("meta: committed entry %d has %s, and this release reads types up to %d; "+
		"stopping so the entry is not skipped", index, reason, CommandTxn))
}

// hasType reports whether an entry that decoded sets its type to a value, as
// opposed to leaving it out or null. Only a rejected entry pays for it.
func hasType(data []byte) bool {
	var probe struct {
		Type json.RawMessage `json:"type"`
	}
	return json.Unmarshal(data, &probe) == nil && len(probe.Type) > 0 && string(probe.Type) != "null"
}

// decodeErrorKind names a decode failure without the bytes it failed on.
func decodeErrorKind(err error) string {
	var syntax *json.SyntaxError
	var typ *json.UnmarshalTypeError
	switch {
	case errors.As(err, &syntax):
		return fmt.Sprintf("syntax error at offset %d", syntax.Offset)
	case errors.As(err, &typ):
		return fmt.Sprintf("field %q is not a %s", typ.Field, typ.Type)
	default:
		return "invalid JSON"
	}
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
// checks hold. A row over MaxTxnBodyBytes is a mismatch and is not read, as no
// check value can be that large. A failed read is a storeError.
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
	rs := &restoreState{budget: newTxnBudget(f.limits)}
	err := f.restoreByMerge(r, started, rs)
	if err == nil {
		err = f.restoreApplied(rs)
	}
	if err == nil {
		err = f.rebuildMapHeads()
	}
	if err == nil {
		err = publishInstalledSnapshot(rc)
	}
	if err != nil {
		if rs.wrote {
			//nolint:forbidigo // A part-restored store must not apply or answer another entry.
			panic(fmt.Sprintf("meta: restoring a snapshot failed after it wrote to the store; stopping: %v", err))
		}
		return err
	}
	f.restoreUnsafe = false
	return nil
}

// restoreState is what a restore learns on its way: whether it may have
// changed the store, whether it sent any write, and the applied row the
// snapshot holds, nil for none, with the index it names.
type restoreState struct {
	wrote, sent bool
	applied     []byte
	index       uint64
	budget      txnBudget
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
	rs.sent = true
}

// takeApplied keeps the snapshot's applied row aside and reports whether key
// was that row, which the data pass then leaves out. The row is decoded here,
// so a snapshot with a bad row fails before the flush.
func (rs *restoreState) takeApplied(key, value []byte) (bool, error) {
	if !bytes.Equal(key, appliedKey) {
		return false, nil
	}
	index, err := decodeApplied(value)
	if err != nil {
		return true, fmt.Errorf("restore: %w", err)
	}
	rs.applied, rs.index = bytes.Clone(value), index
	return true, nil
}

// restoreApplied runs after every data row is flushed. It writes the
// snapshot's applied row, or removes the local one when the snapshot has
// none, then moves the in-memory index to match, backwards included.
func (f *FSM) restoreApplied(rs *restoreState) error {
	// A failed commit can still have landed and named the newer index.
	rs.wrote = true
	err := f.db.Update(func(txn *badger.Txn) error {
		if rs.applied == nil {
			return txn.Delete(appliedKey)
		}
		return txn.Set(appliedKey, rs.applied)
	})
	if err != nil {
		return fmt.Errorf("restore: write the applied index: %w", err)
	}
	f.persisted = rs.index

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
		if taken, err := rs.takeApplied(key, value); taken || err != nil {
			return err
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

	// A flush with no write sent commits nothing, so it cannot change the store.
	rs.wrote = rs.wrote || rs.sent
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
	return f.ScanFromLimit(prefix, after, 0, fn)
}

// ScanFromLimit is ScanFrom that stops once fn has seen limit keys, before it
// reads the next one, and reads values only for the keys it returns. Zero is
// no limit.
func (f *FSM) ScanFromLimit(prefix, after string, limit int, fn func(key string, value []byte) error) error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)
		// Value prefetch would read values past the limit row.
		if limit > 0 {
			opts.PrefetchValues = false
		}
		seen := 0
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
			if seen++; limit > 0 && seen >= limit {
				return nil
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
	if staged, ok := sink.(*stagedSnapshot); ok {
		staged.local = true
	}
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
