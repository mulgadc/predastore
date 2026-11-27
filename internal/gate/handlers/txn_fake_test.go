package handlers

import (
	"bytes"
	"context"

	"github.com/mulgadc/predastore/internal/meta"
)

// Txn applies the transaction atomically under the map's lock, as the FSM
// does in a badger transaction of its own.
func (m *fakeMeta) Txn(_ context.Context, checks []meta.TxnCheck, writes []meta.TxnWrite) (meta.TxnOutcome, []meta.TxnValue, error) {
	if err := meta.ValidateTxn(checks, writes); err != nil {
		return meta.TxnRefused, nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.count("txn")
	applied, current := evalTxnChecks(m.rows, checks)
	if !applied {
		return meta.TxnMismatch, current, nil
	}
	applyTxnWrites(m.rows, writes)
	return meta.TxnApplied, nil, nil
}

// evalTxnChecks reports whether every check holds against rows, and the value
// of each checked key when one does not.
func evalTxnChecks(rows map[string][]byte, checks []meta.TxnCheck) (bool, []meta.TxnValue) {
	holds := true
	current := make([]meta.TxnValue, len(checks))
	for i, c := range checks {
		v, ok := rows[string(c.Key)]
		current[i] = meta.TxnValue{Key: c.Key, Exists: ok}
		if ok {
			current[i].Value = append([]byte(nil), v...)
		}
		if c.Absent == ok || (!c.Absent && !bytes.Equal(v, c.Value)) {
			holds = false
		}
	}
	return holds, current
}

func applyTxnWrites(rows map[string][]byte, writes []meta.TxnWrite) {
	for _, w := range writes {
		if w.Delete {
			delete(rows, string(w.Key))
			continue
		}
		rows[string(w.Key)] = append([]byte(nil), w.Value...)
	}
}
