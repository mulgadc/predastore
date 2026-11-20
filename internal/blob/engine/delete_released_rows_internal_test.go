package engine

import (
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// A release row is reachable only through the retained row it hangs off, so a
// delete that takes the retained row and leaves the release behind leaves a row
// nothing can ever find again.

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

// Why the delete's release scan needs releaseMu. A release committed between
// a scan and its commit is a key the scan never read, so Badger has nothing to
// conflict on and the row outlives the retained row it hangs off.
func TestAScanCannotSeeAReleaseCommittedAfterItsSnapshot(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x68}

	epochs, _, _ := raceWriters(t, st, oh, 2)
	idxKey := MakeKey(oh, 0)
	retained := retainedKey(idxKey, epochs[0])

	// The deleting transaction, open and already past its scan.
	del := st.index.NewTransaction(true)
	defer del.Discard()

	prefix := releasedScan(idxKey)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	it := del.NewIterator(opts)
	seen := 0
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		seen++
	}
	it.Close()
	if seen != 0 {
		t.Fatalf("fixture already had %d release rows before the race", seen)
	}

	// The release lands now, after that scan and before the delete commits.
	if err := st.ReleaseGeneration(oh, 0, epochs[0]); err != nil {
		t.Fatalf("release: %v", err)
	}

	// What a scan-based delete would do: remove only the rows it saw.
	if err := del.Delete(retained); err != nil {
		t.Fatalf("delete retained: %v", err)
	}
	if err := del.Commit(); err != nil {
		t.Fatalf("the delete conflicted, so the scan would have been safe: %v", err)
	}

	// It did not conflict, and the release row is now unreachable.
	if got := countRowsUnder(t, st, releasedScan(idxKey)); got != 1 {
		t.Fatalf("expected the orphan a scan-based delete leaves, got %d release rows", got)
	}
}

// A release that arrives while a delete is in progress waits for it, so it
// cannot commit between the delete's scan and its commit. The test holds
// releaseMu and runs the delete body, which is what Store.Delete does.
func TestAReleaseCannotCommitInsideADelete(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x62}

	epochs, _, _ := raceWriters(t, st, oh, 2)
	idxKey := MakeKey(oh, 0)

	st.releaseMu.Lock()
	del := st.index.NewTransaction(true)
	defer del.Discard()
	if _, err := deleteIn(del, idxKey); err != nil {
		st.releaseMu.Unlock()
		t.Fatalf("delete: %v", err)
	}

	released := make(chan error, 1)
	go func() { released <- st.ReleaseGeneration(oh, 0, epochs[0]) }()
	select {
	case err := <-released:
		st.releaseMu.Unlock()
		t.Fatalf("a release ran while a delete held releaseMu (err %v)", err)
	case <-time.After(100 * time.Millisecond):
	}

	err := del.Commit()
	st.releaseMu.Unlock()
	if err != nil {
		t.Fatalf("delete commit: %v", err)
	}
	if err := <-released; err != nil {
		t.Fatalf("release after the delete: %v", err)
	}

	if got := countRowsUnder(t, st, retainedScan(idxKey)); got != 0 {
		t.Fatalf("delete left %d retained rows", got)
	}
	if got := countRowsUnder(t, st, releasedScan(idxKey)); got != 0 {
		t.Fatalf("a release that waited on the delete wrote %d rows for a deleted generation", got)
	}
}

// And the other side of the lock: a delete waits for a release in progress,
// so its scan sees the release row once that release commits.
func TestADeleteWaitsForAReleaseInProgress(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x6a}

	epochs, _, _ := raceWriters(t, st, oh, 2)
	idxKey := MakeKey(oh, 0)

	st.releaseMu.Lock()
	rel := st.index.NewTransaction(true)
	defer rel.Discard()
	if err := releaseIn(rel, retainedKey(idxKey, epochs[0]), epochs[0]); err != nil {
		st.releaseMu.Unlock()
		t.Fatalf("release: %v", err)
	}

	deleted := make(chan error, 1)
	go func() {
		_, err := st.Delete(oh, 0)
		deleted <- err
	}()
	select {
	case err := <-deleted:
		st.releaseMu.Unlock()
		t.Fatalf("a delete ran while a release held releaseMu (err %v)", err)
	case <-time.After(100 * time.Millisecond):
	}

	err := rel.Commit()
	st.releaseMu.Unlock()
	if err != nil {
		t.Fatalf("release commit: %v", err)
	}
	if err := <-deleted; err != nil {
		t.Fatalf("delete after the release: %v", err)
	}

	if got := countRowsUnder(t, st, retainedScan(idxKey)); got != 0 {
		t.Fatalf("delete left %d retained rows", got)
	}
	if got := countRowsUnder(t, st, releasedScan(idxKey)); got != 0 {
		t.Fatalf("a release committed just before the delete survived it: %d rows", got)
	}
}

// The other order: a delete that commits after the release has read its
// retained row. The release conflicts on that read, and its retry finds the
// row gone and writes nothing.
func TestADeleteInsideTheReleaseWindowLeavesNothingToRelease(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x67}

	epochs, _, _ := raceWriters(t, st, oh, 2)
	idxKey := MakeKey(oh, 0)

	rel := st.index.NewTransaction(true)
	defer rel.Discard()
	if err := releaseIn(rel, retainedKey(idxKey, epochs[0]), epochs[0]); err != nil {
		t.Fatalf("release: %v", err)
	}

	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if err := rel.Commit(); !errors.Is(err, badger.ErrConflict) {
		t.Fatalf("a release whose retained row was deleted under it committed: %v", err)
	}
	if got := countRowsUnder(t, st, releasedScan(idxKey)); got != 0 {
		t.Fatalf("the conflicting release left %d release rows", got)
	}

	// The retry ReleaseGeneration makes on that conflict.
	if err := st.ReleaseGeneration(oh, 0, epochs[0]); err != nil {
		t.Fatalf("release retry: %v", err)
	}
	if got := countRowsUnder(t, st, releasedScan(idxKey)); got != 0 {
		t.Fatalf("the retried release wrote %d rows for a deleted generation", got)
	}
}

// A delete tombstones the extents its rows name, and a release row names none.
// Asserting the exact tombstone set rather than a count is what catches both a
// release row routed through the extent decoder and a lost live tombstone.
func TestDeleteTombstonesEveryExtentAndNoReleaseRow(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x63}

	epochs, _, _ := raceWriters(t, st, oh, 2)
	if err := st.ReleaseGeneration(oh, 0, epochs[0]); err != nil {
		t.Fatalf("release: %v", err)
	}
	prepareValue(t, st, oh, 0, []byte("a third generation, prepared and never committed"))

	idxKey := MakeKey(oh, 0)
	for name, prefix := range map[string][]byte{"live": idxKey, "retained": retainedScan(idxKey), "prepared": preparedScan(idxKey)} {
		if countRowsUnder(t, st, prefix) == 0 {
			t.Fatalf("fixture has no %s row, so its tombstone goes unchecked", name)
		}
	}

	// Every extent the position's rows name, before the delete removes them.
	want := map[string]int64{}
	err := st.index.View(func(txn *badger.Txn) error {
		for _, prefix := range [][]byte{idxKey, retainedScan(idxKey), preparedScan(idxKey)} {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefix
			it := txn.NewIterator(opts)
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				raw, err := it.Item().ValueCopy(nil)
				if err != nil {
					it.Close()

					return err
				}
				ext, _, err := decodeIndexValue(raw)
				if err != nil {
					continue
				}
				want[string(tombstoneKey(ext.SegNum, ext.Off))] = ext.PSize
			}
			it.Close()
		}

		return nil
	})
	if err != nil {
		t.Fatalf("collect extents: %v", err)
	}
	if len(want) != 3 {
		t.Fatalf("fixture named %d extents, want one live, one retained and one prepared", len(want))
	}

	before := countRowsUnder(t, st, []byte{tombstonePrefix})
	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Exactly the extents the rows named, each at its own recorded size.
	for key, psize := range want {
		var got int64
		err := st.index.View(func(txn *badger.Txn) error {
			raw, err := readRaw(txn, []byte(key))
			if err != nil {
				return err
			}
			got = int64(binary.BigEndian.Uint64(raw))

			return nil
		})
		if err != nil {
			t.Fatalf("delete wrote no tombstone for extent %x: %v", key, err)
		}
		if got != psize {
			t.Fatalf("tombstone for %x records %d dead bytes, want %d", key, got, psize)
		}
	}
	if after := countRowsUnder(t, st, []byte{tombstonePrefix}); after-before != len(want) {
		t.Fatalf("delete wrote %d tombstones, want %d: a release row names no extent and must not add one", after-before, len(want))
	}
	if got := countRowsUnder(t, st, releasedScan(idxKey)); got != 0 {
		t.Fatalf("delete left %d release rows", got)
	}
}

// An overwritten object whose old generation was never released, so the
// release scan finds nothing and the delete must still remove the rest.
func TestDeleteWithNoReleasedGenerationIsUnchanged(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x64}

	raceWriters(t, st, oh, 2)
	idxKey := MakeKey(oh, 0)
	if got := countRowsUnder(t, st, retainedScan(idxKey)); got != 1 {
		t.Fatalf("fixture has %d retained rows, want 1", got)
	}
	if got := countRowsUnder(t, st, releasedScan(idxKey)); got != 0 {
		t.Fatalf("fixture has %d release rows, want none", got)
	}

	deleted, err := st.Delete(oh, 0)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !deleted {
		t.Fatalf("delete reported nothing removed for a live object")
	}
	if _, err := st.Lookup(oh, 0); err == nil {
		t.Fatalf("the object still answers after a delete")
	}
	if got := countRowsUnder(t, st, retainedScan(idxKey)); got != 0 {
		t.Fatalf("delete left %d retained rows", got)
	}
	if got := countRowsUnder(t, st, releasedScan(idxKey)); got != 0 {
		t.Fatalf("a delete with nothing released wrote %d release rows", got)
	}
}

// Reclaim still pairs the two rows itself. Clearing releases on the delete
// path must not have moved that responsibility out of the sweep.
func TestReclaimStillTakesTheReleaseRowWithTheRetainedRow(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x65}

	epochs, _, _ := raceWriters(t, st, oh, 2)
	if err := st.ReleaseGeneration(oh, 0, epochs[0]); err != nil {
		t.Fatalf("release: %v", err)
	}

	// A cutoff in the past, so the release is outside its grace window without
	// a sleep.
	if _, err := st.sweepRetained(-1); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	idxKey := MakeKey(oh, 0)
	if got := countRowsUnder(t, st, retainedScan(idxKey)); got != 0 {
		t.Fatalf("the sweep left %d retained rows past the grace window", got)
	}
	if got := countRowsUnder(t, st, releasedScan(idxKey)); got != 0 {
		t.Fatalf("the sweep left %d release rows behind", got)
	}
}

// A delete reaches only its own position's release rows. A neighbour with its
// own released generation, and a live object whose hash begins with the
// release prefix byte, both survive it.
func TestDeleteLeavesAForeignKeyUnderTheReleasePrefix(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x66}

	epochs, _, _ := raceWriters(t, st, oh, 2)
	if err := st.ReleaseGeneration(oh, 0, epochs[0]); err != nil {
		t.Fatalf("release: %v", err)
	}

	neighbour := [32]byte{0x66, 0x01}
	nEpochs, _, _ := raceWriters(t, st, neighbour, 2)
	if err := st.ReleaseGeneration(neighbour, 0, nEpochs[0]); err != nil {
		t.Fatalf("release the neighbour: %v", err)
	}
	nKey := MakeKey(neighbour, 0)

	// A live row whose key starts with the release prefix byte.
	foreign := [32]byte{releasedPrefix, 0x67}
	foreignKey := MakeKey(foreign, 0)
	body := []byte("a live object whose hash starts with the release prefix")
	fe := prepareValue(t, st, foreign, 0, body)
	if _, err := st.Commit(foreign, 0, fe); err != nil {
		t.Fatalf("commit the foreign position: %v", err)
	}

	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if got := countRowsUnder(t, st, releasedScan(nKey)); got != 1 {
		t.Fatalf("deleting one position removed %d-of-1 release rows of its neighbour", 1-got)
	}
	if got := countRowsUnder(t, st, retainedScan(nKey)); got != 1 {
		t.Fatalf("deleting one position removed %d-of-1 retained rows of its neighbour", 1-got)
	}
	if got := countRowsUnder(t, st, foreignKey); got != 1 {
		t.Fatalf("deleting one position removed the live row of another under the same prefix byte: %d rows left", got)
	}
	r, err := st.Lookup(foreign, 0)
	if err != nil {
		t.Fatalf("the foreign object stopped answering after an unrelated delete: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// A live row and 52,426 unreleased retained rows is the largest history the
// delete fit in one transaction before release rows were cleared, measured on
// the prior code. Clearing them must not cost an unreleased history anything.
func TestDeleteStillFitsTheLargestUnreleasedHistoryItDidBefore(t *testing.T) {
	if testing.Short() {
		t.Skip("writes 52,426 retained rows")
	}
	st, _ := openTestStore(t)
	oh := [32]byte{0x69}
	idxKey := MakeKey(oh, 0)

	const rows = 52426
	if got := st.index.MaxBatchCount(); got != 104857 {
		t.Fatalf("maxBatchCount is %d, not the 104,857 that %d rows was measured against", got, rows)
	}
	live := prepareValue(t, st, oh, 0, []byte("the live generation"))
	if _, err := st.Commit(oh, 0, live); err != nil {
		t.Fatalf("commit: %v", err)
	}
	for start := 0; start < rows; start += 5000 {
		wb := st.index.NewWriteBatch()
		for i := start; i < min(start+5000, rows); i++ {
			e := uint64(i + 1)
			ext := extent{SegNum: e + 100, Off: int64(i) * 4096, PSize: 4096}
			if err := wb.Set(retainedKey(idxKey, e), encodeIndexValue(ext, e)); err != nil {
				t.Fatalf("stage retained row: %v", err)
			}
		}
		if err := wb.Flush(); err != nil {
			t.Fatalf("write retained rows: %v", err)
		}
	}

	deleted, err := st.Delete(oh, 0)
	if err != nil {
		t.Fatalf("a history the delete handled before now fails: %v", err)
	}
	if !deleted {
		t.Fatalf("delete reported nothing removed for a live object")
	}
	if got := countRowsUnder(t, st, retainedScan(idxKey)); got != 0 {
		t.Fatalf("delete left %d retained rows", got)
	}
}
