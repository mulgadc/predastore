package gate

import (
	"bytes"
	"context"

	"github.com/mulgadc/predastore/internal/meta"
)

// Txn applies the transaction in one step, as the FSM does: every check
// against the rows as they stand, then every write or none.
func (f *fakeMeta) Txn(_ context.Context, checks []meta.TxnCheck, writes []meta.TxnWrite) (meta.TxnOutcome, []meta.TxnValue, error) {
	if err := meta.ValidateTxn(checks, writes); err != nil {
		return meta.TxnRefused, nil, err
	}
	if f.err != nil {
		return meta.TxnRefused, nil, f.err
	}
	holds := true
	current := make([]meta.TxnValue, len(checks))
	for i, c := range checks {
		v, ok := f.rows[string(c.Key)]
		current[i] = meta.TxnValue{Key: c.Key, Value: v, Exists: ok}
		if c.Absent == ok || (!c.Absent && !bytes.Equal(v, c.Value)) {
			holds = false
		}
	}
	if !holds {
		return meta.TxnMismatch, current, nil
	}
	f.writes++
	for _, w := range writes {
		if w.Delete {
			delete(f.rows, string(w.Key))
			continue
		}
		f.rows[string(w.Key)] = append([]byte(nil), w.Value...)
	}
	return meta.TxnApplied, nil, nil
}
