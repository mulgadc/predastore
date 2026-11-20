package engine

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

// A release row is reachable only through the retained row it hangs off, so a
// delete that takes the retained row leaves a release row that only the
// orphaned-release sweep can find again.

// releasedScan is the prefix every release row for a position shares.
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

// writeOrphans writes n release rows with no retained row behind them, the
// state a delete before this fix left in the index.
func writeOrphans(t *testing.T, st *Store, first byte, n int) {
	t.Helper()

	wb := st.index.NewWriteBatch()
	for i := range n {
		oh := [32]byte{first}
		binary.BigEndian.PutUint32(oh[1:5], uint32(i))
		key := releasedKeyFor(retainedKey(MakeKey(oh, 0), 1))
		if err := wb.Set(key, encodeReleasedValue(1)); err != nil {
			t.Fatalf("stage orphan: %v", err)
		}
	}
	if err := wb.Flush(); err != nil {
		t.Fatalf("write orphans: %v", err)
	}
}

// writeInUse writes n retained rows, each with the release row a publisher
// wrote for it, so every release row is one the sweep must keep.
func writeInUse(t *testing.T, st *Store, first byte, n int) {
	t.Helper()

	wb := st.index.NewWriteBatch()
	for i := range n {
		oh := [32]byte{first}
		binary.BigEndian.PutUint32(oh[1:5], uint32(i))
		retained := retainedKey(MakeKey(oh, 0), 1)
		ext := extent{SegNum: 1, Off: int64(i) * 4096, PSize: 4096}
		if err := wb.Set(retained, encodeIndexValue(ext, 1)); err != nil {
			t.Fatalf("stage retained row: %v", err)
		}
		if err := wb.Set(releasedKeyFor(retained), encodeReleasedValue(1)); err != nil {
			t.Fatalf("stage release row: %v", err)
		}
	}
	if err := wb.Flush(); err != nil {
		t.Fatalf("write in-use releases: %v", err)
	}
}

func sweepOrphans(t *testing.T, st *Store) int {
	t.Helper()

	n, err := st.sweepOrphanedReleases()
	if err != nil {
		t.Fatalf("orphan sweep: %v", err)
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
	if got := sweepOrphans(t, st); got != 1 {
		t.Fatalf("the sweep dropped %d orphaned release rows, want 1", got)
	}

	if got := countRowsUnder(t, st, retainedScan(idxKey)); got != 0 {
		t.Fatalf("delete left %d retained rows", got)
	}
	if got := countRowsUnder(t, st, releasedScan(idxKey)); got != 0 {
		t.Fatalf("delete left %d release rows behind, each reachable only through a retained row it just removed", got)
	}
}

// A release whose retained row still exists is the sweep's input, not
// garbage; dropping it would hold the generation's disk forever.
func TestTheOrphanSweepKeepsAReleaseWhoseRetainedRowExists(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x62}

	epochs, _, _ := raceWriters(t, st, oh, 2)
	if err := st.ReleaseGeneration(oh, 0, epochs[0]); err != nil {
		t.Fatalf("release: %v", err)
	}
	if got := sweepOrphans(t, st); got != 0 {
		t.Fatalf("the sweep dropped %d release rows whose retained row exists", got)
	}

	idxKey := MakeKey(oh, 0)
	if got := countRowsUnder(t, st, releasedScan(idxKey)); got != 1 {
		t.Fatalf("%d release rows after the sweep, want the 1 still in use", got)
	}

	// A cutoff in the past, so the grace window has elapsed without a sleep.
	if _, err := st.sweepRetained(-1); err != nil {
		t.Fatalf("retained sweep: %v", err)
	}
	if got := countRowsUnder(t, st, retainedScan(idxKey)); got != 0 {
		t.Fatalf("the retained sweep left %d retained rows", got)
	}
	if got := countRowsUnder(t, st, releasedScan(idxKey)); got != 0 {
		t.Fatalf("the retained sweep left %d release rows", got)
	}
}

// A release that arrives after the delete finds no retained row and writes
// nothing, which is what lets the sweep treat a lone release row as garbage.
func TestAReleaseAfterTheDeleteWritesNothing(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x67}

	epochs, _, _ := raceWriters(t, st, oh, 2)
	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := st.ReleaseGeneration(oh, 0, epochs[0]); err != nil {
		t.Fatalf("release: %v", err)
	}
	if got := countRowsUnder(t, st, releasedScan(MakeKey(oh, 0))); got != 0 {
		t.Fatalf("a release after the delete wrote %d rows", got)
	}
}

// Rows orphaned by deletes before this fix are found by the same sweep.
func TestTheOrphanSweepDropsOrphansLeftBeforeTheFix(t *testing.T) {
	st, _ := openTestStore(t)
	writeOrphans(t, st, 0x70, 25)

	if got := sweepOrphans(t, st); got != 25 {
		t.Fatalf("the sweep dropped %d of 25 historical orphans", got)
	}
	if got := countRowsUnder(t, st, []byte{releasedPrefix}); got != 0 {
		t.Fatalf("%d release rows left", got)
	}
}

// A backlog larger than one sweep's cap drains over successive sweeps, so the
// sweep's memory and each transaction stay bounded however large it is.
func TestTheOrphanSweepDrainsABacklogInBoundedSweeps(t *testing.T) {
	st, _ := openTestStore(t)
	writeOrphans(t, st, 0x71, orphanScanPerSweep+500)

	if got := sweepOrphans(t, st); got != orphanScanPerSweep {
		t.Fatalf("first sweep dropped %d, want the cap of %d", got, orphanScanPerSweep)
	}
	if got := sweepOrphans(t, st); got != 500 {
		t.Fatalf("second sweep dropped %d, want the remaining 500", got)
	}
	if got := sweepOrphans(t, st); got != 0 {
		t.Fatalf("third sweep dropped %d from an empty backlog", got)
	}
}

// A sweep reads a bounded window and the next resumes after it, so orphans
// sorted behind more in-use releases than one window holds are still reached,
// and the window wraps to find orphans written behind it.
func TestTheOrphanSweepResumesWhereTheLastOneStopped(t *testing.T) {
	st, _ := openTestStore(t)
	writeInUse(t, st, 0x10, orphanScanPerSweep)
	writeOrphans(t, st, 0x80, 5)

	if got := sweepOrphans(t, st); got != 0 {
		t.Fatalf("first sweep dropped %d from a window of in-use releases", got)
	}
	if got := sweepOrphans(t, st); got != 5 {
		t.Fatalf("second sweep dropped %d, want the 5 orphans past the first window", got)
	}
	writeOrphans(t, st, 0x05, 3)
	if got := sweepOrphans(t, st); got != 3 {
		t.Fatalf("after wrapping the sweep dropped %d, want the 3 orphans at the front", got)
	}
	if got := countRowsUnder(t, st, []byte{releasedPrefix}); got != orphanScanPerSweep {
		t.Fatalf("%d release rows left, want the %d in use", got, orphanScanPerSweep)
	}
}

// The retained cycle runs the orphan sweep beside the retained sweep.
func TestTheRetainedTickDropsOrphanedReleases(t *testing.T) {
	st, _ := openTestStore(t)
	writeOrphans(t, st, 0x72, 3)

	st.retainedTick()
	if got := countRowsUnder(t, st, []byte{releasedPrefix}); got != 0 {
		t.Fatalf("the retained tick left %d orphaned release rows", got)
	}
}

// A retained row written back after the drop read it as absent, but before the
// drop commits, fails the drop, so the release row it marks survives.
func TestARestoreInsideTheDropFailsTheDrop(t *testing.T) {
	st, _ := openTestStore(t)
	writeOrphans(t, st, 0x76, 1)

	orphans, err := st.collectOrphanedReleases()
	if err != nil || len(orphans) != 1 {
		t.Fatalf("collect: %d rows, %v", len(orphans), err)
	}
	txn := st.index.NewTransaction(true)
	defer txn.Discard()
	if n, err := dropOrphanBatch(txn, orphans); err != nil || n != 1 {
		t.Fatalf("stage the drop: %d rows, %v", n, err)
	}

	ext := extent{SegNum: 1, Off: 0, PSize: 4096}
	if err := st.index.Update(func(rt *badger.Txn) error {
		return rt.Set(retainedKeyFor(orphans[0]), encodeIndexValue(ext, 1))
	}); err != nil {
		t.Fatalf("restore the retained row: %v", err)
	}

	if err := txn.Commit(); !errors.Is(err, badger.ErrConflict) {
		t.Fatalf("the drop committed over a restore: %v", err)
	}
	if got := countRowsUnder(t, st, []byte{releasedPrefix}); got != 1 {
		t.Fatalf("%d release rows left, want the restored row's release", got)
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
	sweepOrphans(t, st)

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
		t.Fatalf("delete and sweep wrote %d tombstones, want %d: a release row names no extent and must not add one", after-before, len(want))
	}
	if got := countRowsUnder(t, st, releasedScan(idxKey)); got != 0 {
		t.Fatalf("delete and sweep left %d release rows", got)
	}
}

// An overwritten object whose old generation was never released: the delete
// and the sweep must leave nothing behind and add nothing.
func TestDeleteWithNoReleasedGenerationIsUnchanged(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x64}

	raceWriters(t, st, oh, 2)
	idxKey := MakeKey(oh, 0)
	if got := countRowsUnder(t, st, retainedScan(idxKey)); got != 1 {
		t.Fatalf("fixture has %d retained rows, want 1", got)
	}

	deleted, err := st.Delete(oh, 0)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !deleted {
		t.Fatalf("delete reported nothing removed for a live object")
	}
	if got := sweepOrphans(t, st); got != 0 {
		t.Fatalf("the sweep dropped %d rows after a delete with nothing released", got)
	}
	if _, err := st.Lookup(oh, 0); err == nil {
		t.Fatalf("the object still answers after a delete")
	}
	if got := countRowsUnder(t, st, retainedScan(idxKey)); got != 0 {
		t.Fatalf("delete left %d retained rows", got)
	}
}

// The sweep reaches only release rows whose retained row is gone. A neighbour
// with its own retained and released generation, and a live object whose hash
// begins with the release prefix byte, both survive it.
func TestTheOrphanSweepLeavesEverythingElseUnderTheReleasePrefix(t *testing.T) {
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

	// A live row whose 36-byte key starts with the release prefix byte, so the
	// sweep's namespace scan visits it.
	foreign := [32]byte{releasedPrefix, 0x67}
	foreignKey := MakeKey(foreign, 0)
	fe := prepareValue(t, st, foreign, 0, []byte("a live object whose hash starts with the release prefix"))
	if _, err := st.Commit(foreign, 0, fe); err != nil {
		t.Fatalf("commit the foreign position: %v", err)
	}

	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if got := sweepOrphans(t, st); got != 1 {
		t.Fatalf("the sweep dropped %d rows, want only the deleted position's release", got)
	}

	if got := countRowsUnder(t, st, releasedScan(nKey)); got != 1 {
		t.Fatalf("the sweep removed %d-of-1 release rows of a neighbour", 1-got)
	}
	if got := countRowsUnder(t, st, retainedScan(nKey)); got != 1 {
		t.Fatalf("the sweep removed %d-of-1 retained rows of a neighbour", 1-got)
	}
	if got := countRowsUnder(t, st, foreignKey); got != 1 {
		t.Fatalf("the sweep removed the live row of an object under the release prefix byte: %d rows left", got)
	}
	r, err := st.Lookup(foreign, 0)
	if err != nil {
		t.Fatalf("the foreign object stopped answering: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// A live row and 52,426 unreleased retained rows is the largest history the
// delete fits in one transaction on the base, measured. The fix must not move
// that ceiling, because it adds nothing to the delete transaction.
func TestDeleteStillFitsTheLargestHistoryItDidBefore(t *testing.T) {
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

// The scan collects only release rows whose retained row is gone, so a live
// release is never even proposed for deletion.
func TestTheOrphanScanProposesOnlyOrphans(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x73}

	epochs, _, _ := raceWriters(t, st, oh, 2)
	if err := st.ReleaseGeneration(oh, 0, epochs[0]); err != nil {
		t.Fatalf("release: %v", err)
	}
	writeOrphans(t, st, 0x74, 2)

	orphans, err := st.collectOrphanedReleases()
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if len(orphans) != 2 {
		t.Fatalf("the scan proposed %d rows, want the 2 orphans only", len(orphans))
	}
	for _, k := range orphans {
		if k[1] != 0x74 {
			t.Fatalf("the scan proposed a release whose retained row exists: %x", k)
		}
	}
}

// A retained row can be written again at the same epoch, as handoff repair
// does when it returns a shard. The deleting transaction checks again, so a
// row that stopped being an orphan after the scan survives.
func TestTheOrphanDropSparesARowWhoseRetainedRowReturned(t *testing.T) {
	st, _ := openTestStore(t)
	writeOrphans(t, st, 0x75, 1)

	orphans, err := st.collectOrphanedReleases()
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if len(orphans) != 1 {
		t.Fatalf("the scan proposed %d rows, want 1", len(orphans))
	}

	retained := retainedKeyFor(orphans[0])
	ext := extent{SegNum: 1, Off: 0, PSize: 4096}
	if err := st.index.Update(func(txn *badger.Txn) error {
		return txn.Set(retained, encodeIndexValue(ext, 1))
	}); err != nil {
		t.Fatalf("restore the retained row: %v", err)
	}

	dropped, err := st.dropOrphanedReleases(orphans)
	if err != nil {
		t.Fatalf("drop: %v", err)
	}
	if dropped != 0 {
		t.Fatalf("dropped %d release rows whose retained row exists again", dropped)
	}
	if got := countRowsUnder(t, st, []byte{releasedPrefix}); got != 1 {
		t.Fatalf("%d release rows left, want the 1 that is in use again", got)
	}
}
