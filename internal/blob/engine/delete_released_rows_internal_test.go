package engine

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
)

// A release row is reachable only through the retained row it hangs off, so a
// delete that takes the retained row and leaves the release behind leaves a row
// nothing can ever find again.

// releasedScan is the prefix every release row for a position shares. Only the
// tests scan it; production code reaches a release through its retained row.
func releasedScan(idxKey []byte) []byte {
	key := make([]byte, 0, releasedKeySize-8)

	return append(append(key, releasedPrefix), idxKey...)
}

// countRowsUnder returns how many rows carry prefix.
func countRowsUnder(t *testing.T, st *Store, prefix []byte) int {
	t.Helper()

	n := 0
	err := st.index.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			n++
		}

		return nil
	})
	if err != nil {
		t.Fatalf("count rows under %q: %v", prefix, err)
	}

	return n
}

// The orphan, in one position. A generation is demoted, released, and the
// object is then deleted inside the grace window, which is the whole window
// the sweep exists to honour.
func TestDeleteTakesTheReleaseRowsWithTheRetainedRows(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x61}

	epochs, _, _ := raceWriters(t, st, oh, 2)
	if err := st.ReleaseGeneration(oh, 0, epochs[0]); err != nil {
		t.Fatalf("release: %v", err)
	}

	idxKey := MakeKey(oh, 0)
	if got := countRowsUnder(t, st, releasedScan(idxKey)); got != 1 {
		t.Fatalf("fixture wrote %d release rows, want exactly 1", got)
	}

	deleted, err := st.Delete(oh, 0)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !deleted {
		t.Fatalf("delete reported nothing removed for a live object")
	}

	if got := countRowsUnder(t, st, retainedScan(idxKey)); got != 0 {
		t.Fatalf("delete left %d retained rows", got)
	}
	if got := countRowsUnder(t, st, releasedScan(idxKey)); got != 0 {
		t.Fatalf("delete left %d release rows behind, each reachable only through a retained row it just removed", got)
	}
}
