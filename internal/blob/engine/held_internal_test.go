package engine

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"
)

// prepareHeld writes a held value without publishing it and returns its epoch.
func prepareHeld(t *testing.T, st *Store, oh [32]byte, idx uint32, body []byte) uint64 {
	t.Helper()
	epoch := nextTestEpoch()
	w, err := st.AppendHeld(oh, idx, int64(len(body)), epoch)
	if err != nil {
		t.Fatalf("append held (%x,%d): %v", oh[0], idx, err)
	}
	if _, err := w.Write(body); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	return epoch
}

// A held row waits for its writer's verdict however long that takes, while an
// unheld row beside it still ages out. A negative age stands in for a sweep
// run long after both rows were prepared.
func TestSweepPreparedKeepsAHeldRow(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x31}
	body := bytes.Repeat([]byte{0xa1}, 2*KiB)

	held := prepareHeld(t, st, oh, 0, body)
	unheld := prepareValue(t, st, oh, 1, body)

	reaped, err := st.sweepPrepared(-time.Hour)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if reaped != 1 {
		t.Fatalf("sweep reaped %d rows, want only the unheld one", reaped)
	}
	if _, err := st.Commit(oh, 1, unheld); !errors.Is(err, ErrNotPrepared) {
		t.Fatalf("Commit of the aged unheld row = %v, want ErrNotPrepared", err)
	}
	if _, err := st.Commit(oh, 0, held); err != nil {
		t.Fatalf("Commit of the held row after the sweep: %v", err)
	}
	if got := readValue(t, st, oh, 0); !bytes.Equal(got, body) {
		t.Fatalf("the held row committed different bytes")
	}
}

// Abort is the undo for a held row, and it reclaims the space as it does for
// an unheld one.
func TestAbortDropsAHeldRow(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x32}

	epoch := prepareHeld(t, st, oh, 0, bytes.Repeat([]byte{0xa2}, 2*KiB))
	if err := st.Abort(oh, 0, epoch); err != nil {
		t.Fatalf("abort: %v", err)
	}
	if _, err := st.Commit(oh, 0, epoch); !errors.Is(err, ErrNotPrepared) {
		t.Fatalf("Commit after abort = %v, want ErrNotPrepared", err)
	}
	if got := len(tombstones(t, st)); got != 1 {
		t.Fatalf("abort left %d tombstones, want 1", got)
	}
}

// DeleteEpoch removes the one generation it names, in whichever namespace that
// generation sits, and leaves every other generation of the position alone.
func TestDeleteEpochRemovesOnlyTheNamedGeneration(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x33}
	first := bytes.Repeat([]byte{0xb1}, 2*KiB)
	second := bytes.Repeat([]byte{0xb2}, 2*KiB)
	third := bytes.Repeat([]byte{0xb3}, 2*KiB)

	retained := putValue(t, st, oh, 0, first)
	live := putValue(t, st, oh, 0, second)
	prepared := prepareHeld(t, st, oh, 0, third)
	if got := readValueAt(t, st, oh, 0, retained); !bytes.Equal(got, first) {
		t.Fatalf("setup: the superseded generation is not retained")
	}

	deleted, err := st.DeleteEpoch(oh, 0, nextTestEpoch())
	if err != nil || deleted {
		t.Fatalf("DeleteEpoch of an unwritten epoch = %v, %v, want false, nil", deleted, err)
	}

	deleted, err = st.DeleteEpoch(oh, 0, retained)
	if err != nil || !deleted {
		t.Fatalf("DeleteEpoch of the retained epoch = %v, %v, want true, nil", deleted, err)
	}
	if _, err := st.LookupAt(oh, 0, retained); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("LookupAt the deleted retained epoch = %v, want ErrKeyNotFound", err)
	}
	if got := readValue(t, st, oh, 0); !bytes.Equal(got, second) {
		t.Fatalf("deleting the retained epoch changed the live value")
	}

	deleted, err = st.DeleteEpoch(oh, 0, live)
	if err != nil || !deleted {
		t.Fatalf("DeleteEpoch of the live epoch = %v, %v, want true, nil", deleted, err)
	}
	if _, err := st.Lookup(oh, 0); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Lookup after deleting the live epoch = %v, want ErrKeyNotFound", err)
	}

	if _, err := st.Commit(oh, 0, prepared); err != nil {
		t.Fatalf("Commit of the prepared epoch after both deletes: %v", err)
	}
	if got := readValue(t, st, oh, 0); !bytes.Equal(got, third) {
		t.Fatalf("the prepared epoch did not survive the deletes of the others")
	}
	if got := len(tombstones(t, st)); got != 2 {
		t.Fatalf("two deletes left %d tombstones, want 2", got)
	}
}

// A delete that names an older epoch than the live one must not take the live
// one, which is the stale tombstone a later write raced.
func TestDeleteEpochLeavesALaterLiveGeneration(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x34}
	body := bytes.Repeat([]byte{0xc1}, 2*KiB)

	stale := prepareValue(t, st, oh, 0, []byte("stale"))
	if err := st.Abort(oh, 0, stale); err != nil {
		t.Fatalf("abort: %v", err)
	}
	putValue(t, st, oh, 0, body)

	deleted, err := st.DeleteEpoch(oh, 0, stale)
	if err != nil || deleted {
		t.Fatalf("DeleteEpoch of a stale epoch = %v, %v, want false, nil", deleted, err)
	}
	if got := readValue(t, st, oh, 0); !bytes.Equal(got, body) {
		t.Fatalf("a stale DeleteEpoch removed the later live generation")
	}
}

// Deleting the prepared epoch drops the held row, so a commit after it finds
// nothing to publish.
func TestDeleteEpochDropsAHeldPreparedRow(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x35}

	epoch := prepareHeld(t, st, oh, 0, bytes.Repeat([]byte{0xd1}, 2*KiB))
	deleted, err := st.DeleteEpoch(oh, 0, epoch)
	if err != nil || !deleted {
		t.Fatalf("DeleteEpoch of the held epoch = %v, %v, want true, nil", deleted, err)
	}
	if _, err := st.Commit(oh, 0, epoch); !errors.Is(err, ErrNotPrepared) {
		t.Fatalf("Commit after DeleteEpoch = %v, want ErrNotPrepared", err)
	}
}

// Discard ends a writer with no prepared row, so a held body that will not be
// used never reaches the index, and its space is tombstoned.
func TestDiscardPreparesNothing(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x36}
	epoch := nextTestEpoch()

	w, err := st.AppendHeld(oh, 0, 4*KiB, epoch)
	if err != nil {
		t.Fatalf("append held: %v", err)
	}
	// A short body is what a cut-off held put leaves the writer with.
	if _, err := w.Write(bytes.Repeat([]byte{0xe1}, KiB)); !errors.Is(err, io.EOF) {
		t.Fatalf("short write = %v, want io.EOF", err)
	}
	if err := w.Discard(); err != nil {
		t.Fatalf("discard: %v", err)
	}
	if _, err := st.Commit(oh, 0, epoch); !errors.Is(err, ErrNotPrepared) {
		t.Fatalf("Commit after Discard = %v, want ErrNotPrepared", err)
	}
	if got := len(tombstones(t, st)); got != 1 {
		t.Fatalf("Discard left %d tombstones, want 1", got)
	}
	if err := w.Close(); !errors.Is(err, ErrClosedWriter) {
		t.Fatalf("Close after Discard = %v, want ErrClosedWriter", err)
	}
}
