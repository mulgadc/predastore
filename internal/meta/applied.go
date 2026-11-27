package meta

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// reservedPrefix names the rows the replica keeps for itself in the store.
// No command may write them and no read returns them.
var reservedPrefix = []byte("\x00raft/")

// appliedKey holds the raft index of the last entry whose writes the store
// holds. Every commit the FSM makes writes it, so a replay can skip what the
// store already includes.
var appliedKey = []byte("\x00raft/applied")

// appliedMagic and appliedVersion lead the row's value, so a value written by
// anything else fails to decode rather than being trusted as an index.
var appliedMagic = [4]byte{'P', 'D', 'A', 'I'}

const appliedVersion byte = 1

const appliedValueLen = len(appliedMagic) + 1 + 8

// ErrReservedKey refuses a write to, or a check of, a key under the reserved
// prefix.
var ErrReservedKey = errors.New("meta key is reserved for the replica")

// IsReservedKey reports whether key is under the prefix the replica keeps for
// itself.
func IsReservedKey(key []byte) bool {
	return bytes.HasPrefix(key, reservedPrefix)
}

// putApplied writes the row value for index into v, which holds
// appliedValueLen bytes.
func putApplied(v []byte, index uint64) {
	copy(v, appliedMagic[:])
	v[len(appliedMagic)] = appliedVersion
	binary.BigEndian.PutUint64(v[len(appliedMagic)+1:], index)
}

// appliedRow is the entry and value the FSM reuses for the applied row. With
// no badger subscriber, Commit is done with both when it returns, and the FSM
// commits under its lock, so only one transaction at a time uses them.
type appliedRow struct {
	entry badger.Entry
	value [appliedValueLen]byte
}

// set stages the applied row for index in txn.
func (r *appliedRow) set(txn *badger.Txn, index uint64) error {
	putApplied(r.value[:], index)
	r.entry = badger.Entry{Key: appliedKey, Value: r.value[:]}
	return txn.SetEntry(&r.entry)
}

func decodeApplied(v []byte) (uint64, error) {
	if len(v) != appliedValueLen || !bytes.Equal(v[:len(appliedMagic)], appliedMagic[:]) {
		return 0, fmt.Errorf("meta: the applied index row %q holds %d bytes that are not an applied index", appliedKey, len(v))
	}
	if version := v[len(appliedMagic)]; version != appliedVersion {
		return 0, fmt.Errorf("meta: the applied index row has version %d, and this release reads only version %d", version, appliedVersion)
	}
	return binary.BigEndian.Uint64(v[len(appliedMagic)+1:]), nil
}

// readApplied returns the index the store holds, or zero for a store written
// before the row existed.
func readApplied(db *badger.DB) (uint64, error) {
	var index uint64
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(appliedKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("meta: read the applied index: %w", err)
		}
		return item.Value(func(v []byte) error {
			index, err = decodeApplied(v)
			return err
		})
	})
	return index, err
}

// Badger charges a transaction, for each write, the key, the value or a
// value pointer, and entryOverhead bytes. A value at or above the value
// threshold goes to the value log, so only its pointer counts.
const (
	valuePointerLen int64 = 12
	entryOverhead   int64 = 2 + 10
)

// A new badger transaction already counts one entry and 21 bytes for the
// marker that ends it.
const (
	txnBaseCount int64 = 1
	txnBaseSize  int64 = int64(len("!badger!txn") + 10)
)

// txnLimits are the badger limits a txnBudget counts against.
type txnLimits struct {
	maxCount, maxSize int64
	// threshold is the value size from which badger charges a pointer. With
	// a dynamic threshold it is the highest the threshold can reach, so a
	// cost is never below badger's charge.
	threshold int64
	rowCost   int64
}

func newTxnLimits(db *badger.DB) txnLimits {
	opts := db.Opts()
	l := txnLimits{maxCount: db.MaxBatchCount(), maxSize: db.MaxBatchSize(), threshold: opts.ValueThreshold}
	if opts.VLogPercentile > 0 {
		l.threshold = max(l.threshold, min(1<<20, l.maxSize))
	}
	l.rowCost = l.cost(appliedKey, appliedValueLen)
	return l
}

// cost is what badger charges a transaction for one write of key with a value
// of valueLen bytes.
func (l *txnLimits) cost(key []byte, valueLen int) int64 {
	v := int64(valueLen)
	if v >= l.threshold {
		v = valuePointerLen
	}
	return int64(len(key)) + v + entryOverhead
}

// txnBudget counts what the shared transaction has staged against badger's
// limits, keeping room for the applied row.
type txnBudget struct {
	txnLimits

	count, size int64
}

func newTxnBudget(l txnLimits) txnBudget {
	b := txnBudget{txnLimits: l}
	b.reset()
	return b
}

// fits reports whether one more write of cost and then the applied row stay
// within both of badger's limits, which refuse a write that reaches either.
func (b *txnBudget) fits(cost int64) bool {
	return b.count+2 < b.maxCount && b.size+cost+b.rowCost < b.maxSize
}

// holds reports whether one more write of cost stays within badger's limits.
func (b *txnBudget) holds(cost int64) bool {
	return b.count+1 < b.maxCount && b.size+cost < b.maxSize
}

func (b *txnBudget) add(cost int64) { b.count, b.size = b.count+1, b.size+cost }

func (b *txnBudget) reset() { b.count, b.size = txnBaseCount, txnBaseSize }

// commandCost is what staging cmd charges, with ok false for a command that
// stages no write.
func (l *txnLimits) commandCost(cmd Command) (cost int64, ok bool) {
	switch cmd.Type {
	case CommandPut, CommandPutMax, CommandSwap:
		return l.cost(cmd.Key, len(cmd.Value)), true
	case CommandDelete:
		return l.cost(cmd.Key, 0), true
	default:
		return 0, false
	}
}
