package engine

import (
	"bytes"
	"errors"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

// prepareAt prepares body at a chosen epoch, as a repair that rebuilds a shard
// at its record's generation does.
func prepareAt(t *testing.T, st *Store, oh [32]byte, idx uint32, epoch uint64, body []byte) {
	t.Helper()
	w, err := st.Append(oh, idx, int64(len(body)), epoch)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := w.Write(body); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
}

func commitAt(t *testing.T, st *Store, oh [32]byte, idx uint32, epoch uint64) {
	t.Helper()
	if published, err := st.Commit(oh, idx, epoch); err != nil || !published {
		t.Fatalf("commit epoch %d: published %v, err %v", epoch, published, err)
	}
}

// A commit that replaces the live row at the same epoch leaves no record that
// can name the bytes it displaced, so it tombstones them in the same
// transaction. Nothing is kept under the generation's retained row.
func TestASameEpochCommitTombstonesTheExtentItDisplaces(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x61}
	epoch := nextTestEpoch()

	prepareAt(t, st, oh, 0, epoch, bytes.Repeat([]byte("a"), 4*KiB))
	commitAt(t, st, oh, 0, epoch)
	displaced := extentOf(t, st, oh, 0)

	replacement := bytes.Repeat([]byte("b"), 4*KiB)
	prepareAt(t, st, oh, 0, epoch, replacement)
	commitAt(t, st, oh, 0, epoch)

	if got := readValue(t, st, oh, 0); !bytes.Equal(got, replacement) {
		t.Fatalf("the live row does not read the replacement")
	}
	if _, err := st.indexGet(tombstoneKey(displaced.SegNum, displaced.Off)); err != nil {
		t.Errorf("the displaced extent has no tombstone, so its space is never reclaimed: %v", err)
	}
	if _, err := st.indexGet(retainedKey(MakeKey(oh, 0), epoch)); !errors.Is(err, badger.ErrKeyNotFound) {
		t.Errorf("the displaced extent is still retained under the live epoch: %v", err)
	}
}

// A same-epoch replace, then a newer writer that commits and dies before it
// publishes, so the record still names the replaced epoch. No release names
// that epoch, so a sweep before and after the newer commit keeps it readable.
func TestASameEpochReplaceDoesNotLetTheSweepTakeTheNamedGeneration(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x62}
	named := nextTestEpoch()

	prepareAt(t, st, oh, 0, named, bytes.Repeat([]byte("a"), 4*KiB))
	commitAt(t, st, oh, 0, named)
	replacement := bytes.Repeat([]byte("b"), 4*KiB)
	prepareAt(t, st, oh, 0, named, replacement)
	commitAt(t, st, oh, 0, named)

	if _, err := st.sweepRetained(-1); err != nil {
		t.Fatalf("sweep before the newer commit: %v", err)
	}
	newer := nextTestEpoch()
	prepareAt(t, st, oh, 0, newer, bytes.Repeat([]byte("c"), 4*KiB))
	commitAt(t, st, oh, 0, newer)
	if _, err := st.sweepRetained(-1); err != nil {
		t.Fatalf("sweep after the newer commit: %v", err)
	}

	r, err := st.LookupAt(oh, 0, named)
	if err != nil {
		t.Fatalf("the generation the record names is gone: %v", err)
	}
	defer r.Close()
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(r); err != nil {
		t.Fatalf("read the named generation: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), replacement) {
		t.Fatalf("the named generation does not read the replacement")
	}
}
