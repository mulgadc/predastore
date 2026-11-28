package engine

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

// A rebuild of a generation that a newer commit already demoted replaces the
// retained row. The bytes it displaces are dead, so they are tombstoned.
func TestARebuildOfARetainedGenerationTombstonesTheBytesItDisplaces(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x63}
	named, newer := nextTestEpoch(), nextTestEpoch()

	prepareAt(t, st, oh, 0, named, bytes.Repeat([]byte("a"), 4*KiB))
	commitAt(t, st, oh, 0, named)
	prepareAt(t, st, oh, 0, newer, bytes.Repeat([]byte("n"), 4*KiB))
	commitAt(t, st, oh, 0, newer)
	raw, err := st.indexGet(retainedKey(MakeKey(oh, 0), named))
	if err != nil {
		t.Fatalf("read the retained row: %v", err)
	}
	displaced, _, err := decodeIndexValue(raw)
	if err != nil {
		t.Fatalf("decode the retained row: %v", err)
	}

	rebuilt := bytes.Repeat([]byte("r"), 4*KiB)
	prepareAt(t, st, oh, 0, named, rebuilt)
	if published, err := st.Commit(oh, 0, named); err != nil || published {
		t.Fatalf("a commit under a newer live row publishes nothing: published %v, err %v", published, err)
	}

	if _, err := st.indexGet(tombstoneKey(displaced.SegNum, displaced.Off)); err != nil {
		t.Errorf("the displaced retained extent has no tombstone, so its space is never reclaimed: %v", err)
	}
	r, err := st.LookupAt(oh, 0, named)
	if err != nil {
		t.Fatalf("lookup the named generation: %v", err)
	}
	defer r.Close()
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(r); err != nil || !bytes.Equal(buf.Bytes(), rebuilt) {
		t.Fatalf("the named generation must read the rebuilt bytes: %v", err)
	}
}

// A same-epoch replace frees the old bytes physically through compaction, but
// only after a reader that already holds them closes. The replacement reads
// back after a restart.
func TestASameEpochReplaceIsReclaimedByCompactionAfterItsReaderCloses(t *testing.T) {
	st, dir := openTestStore(t, WithMaxSegSize(40*KiB))
	oh := [32]byte{0x64}
	epoch := nextTestEpoch()
	old := bytes.Repeat([]byte("o"), 12*KiB)
	prepareAt(t, st, oh, 0, epoch, old)
	commitAt(t, st, oh, 0, epoch)
	putValue(t, st, oh, 1, bytes.Repeat([]byte("f"), 12*KiB))
	putValue(t, st, oh, 2, bytes.Repeat([]byte("f"), 12*KiB))
	if ext := extentOf(t, st, oh, 0); ext.SegNum != 0 {
		t.Fatalf("the value must start in segment 0, got %d", ext.SegNum)
	}
	for _, idx := range []uint32{1, 2} {
		if _, err := st.Delete(oh, idx); err != nil {
			t.Fatalf("delete: %v", err)
		}
	}

	held, err := st.Lookup(oh, 0)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	replacement := bytes.Repeat([]byte("r"), 12*KiB)
	prepareAt(t, st, oh, 0, epoch, replacement)
	commitAt(t, st, oh, 0, epoch)
	if ext := extentOf(t, st, oh, 0); ext.SegNum == 0 {
		t.Fatal("the replacement must land outside segment 0, or this proves nothing")
	}

	seg0 := filepath.Join(dir, fmt.Sprintf(segFilename, 0))
	if err := st.compactOnce(); err != nil {
		t.Fatalf("compact with the reader open: %v", err)
	}
	if _, err := os.Stat(seg0); err != nil {
		t.Fatalf("segment 0 must stay while a reader holds the old bytes: %v", err)
	}
	got, err := io.ReadAll(held)
	if err != nil || !bytes.Equal(got, old) {
		t.Fatalf("the open reader must still read the bytes it resolved: %v", err)
	}
	if err := held.Close(); err != nil {
		t.Fatalf("close reader: %v", err)
	}

	if err := st.compactOnce(); err != nil {
		t.Fatalf("compact after the reader closed: %v", err)
	}
	if _, err := os.Stat(seg0); !os.IsNotExist(err) {
		t.Fatalf("segment 0 holds only dead bytes and must be reclaimed: %v", err)
	}

	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	st = openStoreIn(t, dir, testAEAD(t))
	if got := readValue(t, st, oh, 0); !bytes.Equal(got, replacement) {
		t.Fatal("after a restart the position must read the replacement")
	}
}
