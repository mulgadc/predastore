package reaper

import (
	"bytes"
	"context"

	"github.com/mulgadc/predastore/internal/meta"
)

// Txn applies the transaction under the store's lock, all or nothing.
func (m *gateMeta) Txn(_ context.Context, checks []meta.TxnCheck, writes []meta.TxnWrite) (meta.TxnOutcome, []meta.TxnValue, error) {
	if err := meta.ValidateTxn(checks, writes); err != nil {
		return meta.TxnRefused, nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	holds := true
	current := make([]meta.TxnValue, len(checks))
	for i, c := range checks {
		v, ok := m.rows[string(c.Key)]
		current[i] = meta.TxnValue{Key: c.Key, Value: append([]byte(nil), v...), Exists: ok}
		if c.Absent == ok || (!c.Absent && !bytes.Equal(v, c.Value)) {
			holds = false
		}
	}
	if !holds {
		return meta.TxnMismatch, current, nil
	}
	for _, w := range writes {
		if w.Delete {
			delete(m.rows, string(w.Key))
			continue
		}
		m.rows[string(w.Key)] = append([]byte(nil), w.Value...)
	}
	return meta.TxnApplied, nil, nil
}
