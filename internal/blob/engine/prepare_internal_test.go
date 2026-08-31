package engine

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

// A prepared extent is durable and invisible. That is the entire point of the
// split: an overwrite keeps serving its previous generation until the whole
// stripe is in place, so a write torn halfway across the cluster cannot be
// spliced into a plausible wrong object.
func TestPreparedExtentIsNotVisibleUntilCommit(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x11}
	first := bytes.Repeat([]byte{0xaa}, 4*KiB)
	second := bytes.Repeat([]byte{0xbb}, 4*KiB)

	putValue(t, st, oh, 0, first)
	epoch := prepareValue(t, st, oh, 0, second)

	if got := readValue(t, st, oh, 0); !bytes.Equal(got, first) {
		t.Fatalf("a prepared overwrite was served before it was committed")
	}

	if _, err := st.Commit(oh, 0, epoch); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if got := readValue(t, st, oh, 0); !bytes.Equal(got, second) {
		t.Fatalf("the committed value was not served")
	}
}

// Lookup reports the epoch the value is stored under, which is what the read
// gate upstream compares against the placement record.
func TestLookupReportsTheCommittedEpoch(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x12}

	epoch := prepareValue(t, st, oh, 0, []byte("payload"))
	if _, err := st.Commit(oh, 0, epoch); err != nil {
		t.Fatalf("commit: %v", err)
	}

	r, err := st.Lookup(oh, 0)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	defer r.Close()

	if r.Epoch() != epoch {
		t.Fatalf("Epoch() = %d, want %d", r.Epoch(), epoch)
	}
}

// A commit is driven by whoever holds the published record, so it may be
// driven more than once. The second must report success rather than reporting
// a write that did land as failed.
func TestCommitIsIdempotent(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x13}
	body := []byte("payload")

	epoch := prepareValue(t, st, oh, 0, body)
	if _, err := st.Commit(oh, 0, epoch); err != nil {
		t.Fatalf("first commit: %v", err)
	}
	if _, err := st.Commit(oh, 0, epoch); err != nil {
		t.Fatalf("second commit: %v", err)
	}
	if got := readValue(t, st, oh, 0); !bytes.Equal(got, body) {
		t.Fatalf("the value changed under a repeated commit")
	}
}

// Two writers racing the same key each prepare under their own epoch, and
// neither may publish the other's bytes.
func TestCommitRefusesAnEpochItDidNotPrepare(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x14}
	first := bytes.Repeat([]byte{0xaa}, 2*KiB)

	putValue(t, st, oh, 0, first)
	prepared := prepareValue(t, st, oh, 0, bytes.Repeat([]byte{0xbb}, 2*KiB))

	if _, err := st.Commit(oh, 0, prepared+1); !errors.Is(err, ErrNotPrepared) {
		t.Fatalf("Commit(other epoch) = %v, want ErrNotPrepared", err)
	}
	if got := readValue(t, st, oh, 0); !bytes.Equal(got, first) {
		t.Fatalf("a refused commit changed the readable value")
	}

	// The prepared row is untouched, so its own writer can still publish it.
	if _, err := st.Commit(oh, 0, prepared); err != nil {
		t.Fatalf("commit after a refused one: %v", err)
	}
}

// Committing something never prepared is a failure and not a silent no-op: the
// caller's answer is to rewrite the shard, and it can only know that if it is
// told.
func TestCommitWithoutAPrepareFails(t *testing.T) {
	st, _ := openTestStore(t)

	if _, err := st.Commit([32]byte{0x15}, 0, 99); !errors.Is(err, ErrNotPrepared) {
		t.Fatalf("Commit(nothing prepared) = %v, want ErrNotPrepared", err)
	}
}

// Abort releases the space now rather than leaving the node to age it out, and
// the tombstone is what makes that space advertise itself to compaction.
func TestAbortDiscardsThePreparedExtentAndTombstonesIt(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x16}
	first := bytes.Repeat([]byte{0xaa}, 2*KiB)

	putValue(t, st, oh, 0, first)
	before := len(tombstones(t, st))
	epoch := prepareValue(t, st, oh, 0, bytes.Repeat([]byte{0xbb}, 2*KiB))

	if err := st.Abort(oh, 0, epoch); err != nil {
		t.Fatalf("abort: %v", err)
	}
	if got := len(tombstones(t, st)); got != before+1 {
		t.Fatalf("abort produced %d tombstones, want %d", got-before, 1)
	}
	if got := readValue(t, st, oh, 0); !bytes.Equal(got, first) {
		t.Fatalf("abort changed the readable value")
	}
	if _, err := st.Commit(oh, 0, epoch); !errors.Is(err, ErrNotPrepared) {
		t.Fatalf("Commit after abort = %v, want ErrNotPrepared", err)
	}
}

// Aborting something never prepared is success: the caller is asking that
// nothing be left pending, and nothing is.
func TestAbortWithoutAPrepareSucceeds(t *testing.T) {
	st, _ := openTestStore(t)

	if err := st.Abort([32]byte{0x17}, 0, 99); err != nil {
		t.Fatalf("Abort(nothing prepared) = %v, want nil", err)
	}
}

// A prepared row that survived a delete would resurrect the value on a later
// commit, which is a deleted object coming back.
func TestDeleteClearsAPreparedExtent(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x18}

	putValue(t, st, oh, 0, bytes.Repeat([]byte{0xaa}, 2*KiB))
	epoch := prepareValue(t, st, oh, 0, bytes.Repeat([]byte{0xbb}, 2*KiB))

	deleted, err := st.Delete(oh, 0)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !deleted {
		t.Fatal("delete reported no value where one was committed")
	}

	if _, err := st.Commit(oh, 0, epoch); !errors.Is(err, ErrNotPrepared) {
		t.Fatalf("Commit after delete = %v, want ErrNotPrepared", err)
	}
	if _, err := st.Lookup(oh, 0); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("a deleted value was resurrected: %v", err)
	}
}

// compactSegment decides a .idx row is dead when no badger key points at it,
// so an extent held only in memory between prepare and commit would be dropped
// out from under the commit. The prepared row is what keeps it live enough to
// be relocated instead.
func TestCompactionRelocatesAPreparedExtent(t *testing.T) {
	st, _ := openTestStore(t, WithMaxSegSize(64*KiB))
	oh := [32]byte{0x19}
	body := bytes.Repeat([]byte{0xcc}, 8*KiB)

	// Fill a segment with values that will die, so it becomes a candidate, and
	// leave a prepared extent in it as the only live thing.
	dead := [32]byte{0x1a}
	for i := range uint32(4) {
		putValue(t, st, dead, i, body)
	}
	epoch := prepareValue(t, st, oh, 0, body)
	for i := range uint32(4) {
		if _, err := st.Delete(dead, i); err != nil {
			t.Fatalf("delete: %v", err)
		}
	}

	// Roll off the segment holding it: compaction never selects the active one.
	if _, err := st.rollSegment(); err != nil {
		t.Fatalf("roll segment: %v", err)
	}
	before := preparedExtentOf(t, st, oh, 0, epoch)
	if err := st.compactOnce(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if after := preparedExtentOf(t, st, oh, 0, epoch); slotOf(after) == slotOf(before) {
		t.Fatalf("compaction left the prepared extent at %v, so this proves nothing", slotOf(before))
	}

	if _, err := st.Commit(oh, 0, epoch); err != nil {
		t.Fatalf("commit after compaction: %v", err)
	}
	if got := readValue(t, st, oh, 0); !bytes.Equal(got, body) {
		t.Fatalf("compaction relocated a prepared extent to the wrong bytes")
	}
}

// preparedExtentOf reads the extent a prepared row points at.
func preparedExtentOf(t *testing.T, st *Store, oh [32]byte, idx uint32, epoch uint64) extent {
	t.Helper()
	raw, err := st.indexGet(preparedKey(MakeKey(oh, idx), epoch))
	if err != nil {
		t.Fatalf("get prepared row: %v", err)
	}
	ext, _, err := decodeIndexValue(raw)
	if err != nil {
		t.Fatalf("decode prepared row: %v", err)
	}
	return ext
}

// A caller that dies between prepare and commit would otherwise pin its extent
// forever. Ageing is the only available test: the store cannot ask whether the
// writer still intends to commit.
func TestSweepPreparedReapsAbandonedExtents(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x1b}

	epoch := prepareValue(t, st, oh, 0, bytes.Repeat([]byte{0xdd}, 2*KiB))

	// Nothing is old enough yet, so a sweep at the production age must not
	// touch it — a live write must never be reaped out from under its commit.
	reaped, err := st.sweepPrepared(preparedMaxAge)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if reaped != 0 {
		t.Fatalf("sweep reaped %d fresh prepared extents, want 0", reaped)
	}

	reaped, err = st.sweepPrepared(-time.Second)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if reaped != 1 {
		t.Fatalf("sweep reaped %d abandoned prepared extents, want 1", reaped)
	}
	if _, err := st.Commit(oh, 0, epoch); !errors.Is(err, ErrNotPrepared) {
		t.Fatalf("Commit after the reaper = %v, want ErrNotPrepared", err)
	}
	if got := len(tombstones(t, st)); got != 1 {
		t.Fatalf("the reaper left %d tombstones, want 1: reaped space is never advertised", got)
	}
}

// The prepared namespace shares a keyspace with the live rows and the
// tombstones, and all three are told apart by width. Roughly one object hash in
// 256 begins with the prepared prefix, so a scan that matched on the prefix
// alone would decode a live row as a prepared one.
func TestSweepPreparedIgnoresLiveRowsUnderItsPrefix(t *testing.T) {
	st, _ := openTestStore(t)

	// A key whose first byte collides with the prepared namespace.
	oh := [32]byte{preparedPrefix}
	body := []byte("payload")
	putValue(t, st, oh, 0, body)

	reaped, err := st.sweepPrepared(-time.Second)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if reaped != 0 {
		t.Fatalf("the reaper took %d live rows out of the prepared namespace", reaped)
	}
	if got := readValue(t, st, oh, 0); !bytes.Equal(got, body) {
		t.Fatalf("the reaper damaged a live row sharing the prepared prefix")
	}
}
