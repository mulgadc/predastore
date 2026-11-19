package engine

import (
	"bytes"
	"io"
	"testing"
	"time"
)

// A generation is dead when a publisher says its record no longer names it,
// and at no other time. These cover the gap that elapsed time leaves open:
// a writer that commits and never publishes starts no clock anybody can see.

// The sweep must not destroy a generation on age alone. Nothing in this
// package knows which generation a placement record names, so an unreleased
// generation is one that something may still be naming.
func TestTheSweepKeepsAGenerationNobodyReleased(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x51}
	const writers = 3

	epochs, _, _ := raceWriters(t, st, oh, writers)

	// A cutoff in the future: every row is arbitrarily old without a sleep.
	if _, err := st.sweepRetained(-1); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	for i, epoch := range epochs[:writers-1] {
		r, err := st.LookupAt(oh, 0, epoch)
		if err != nil {
			t.Fatalf("generation %d (epoch %016x) was reclaimed though nobody released it: %v", i, epoch, err)
		}
		if err := r.Close(); err != nil {
			t.Fatalf("close %d: %v", i, err)
		}
	}
}

// The prod failure, in one position. A writer commits a newer generation and
// dies before publishing its record, so the record still names the older one.
// Reclaiming the older one on age makes an acknowledged object unreadable.
func TestADeadWritersCommitDoesNotDestroyTheNamedGeneration(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x52}
	body := []byte("the body the client was told it had stored")

	// Acknowledged, and the placement record names this epoch.
	named := prepareValue(t, st, oh, 0, body)
	if _, err := st.Commit(oh, 0, named); err != nil {
		t.Fatalf("commit the named generation: %v", err)
	}

	// A second writer commits and then dies: no publish, so no release.
	orphan := prepareValue(t, st, oh, 0, []byte("a write nobody was told about"))
	if _, err := st.Commit(oh, 0, orphan); err != nil {
		t.Fatalf("commit the orphan generation: %v", err)
	}

	if _, err := st.sweepRetained(-1); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	r, err := st.LookupAt(oh, 0, named)
	if err != nil {
		t.Fatalf("the generation the record names is gone, so the object reads as a failure: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// A writer overtaken at the blob layer can still win the placement swap,
// because the record that survives is the one that arrives last and not the
// one that started last. Its generation was never live, and it has to resolve
// for as long as the record names it.
func TestAnOvertakenWriterKeepsTheGenerationItsRecordWillName(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x56}
	body := []byte("the body the overtaken writer acknowledges")

	// Both prepared, then committed youngest first, so the older epoch is
	// overtaken before it was ever the live generation.
	overtaken := prepareValue(t, st, oh, 0, body)
	winner := prepareValue(t, st, oh, 0, []byte("the generation that commits first"))

	if _, err := st.Commit(oh, 0, winner); err != nil {
		t.Fatalf("commit the newer epoch: %v", err)
	}
	if _, err := st.Commit(oh, 0, overtaken); err != nil {
		t.Fatalf("commit the overtaken epoch: %v", err)
	}

	if _, err := st.sweepRetained(-1); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	// The publish lands here, naming the overtaken generation.
	r, err := st.LookupAt(oh, 0, overtaken)
	if err != nil {
		t.Fatalf("the overtaken generation was reclaimed before its record published: %v", err)
	}
	got, err := io.ReadAll(r)
	if closeErr := r.Close(); closeErr != nil {
		t.Fatalf("close: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("the overtaken generation read back as another writer's body")
	}
}

// reopen closes the store and opens the same directory again, which is what
// an upgrade or a rollback does to the rows already on disk.
func reopen(t *testing.T, st *Store, dir string) *Store {
	t.Helper()
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	next, err := Open(dir, WithFreeSpaceWatermark(0, 0), WithAEAD(testAEAD(t)))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = next.Close() })

	return next
}

// sweepRetainedByAge is the sweep an earlier release runs: it reclaims a
// retained generation whenever the stamp its commit wrote predates the cutoff,
// and knows nothing about a release. Running it against rows this engine wrote
// is what a rollback does to them.
func sweepRetainedByAge(t *testing.T, st *Store, maxAge time.Duration) {
	t.Helper()
	cutoff := time.Now().Add(-maxAge).UnixNano()

	var stale [][]byte
	if err := st.indexScan([]byte{retainedPrefix}, func(k, v []byte) error {
		if len(k) != retainedKeySize || len(v) != preparedValueSize {
			return nil
		}
		at, err := decodePreparedAt(v)
		if err != nil {
			return err
		}
		if at < cutoff {
			stale = append(stale, append([]byte(nil), k...))
		}

		return nil
	}); err != nil {
		t.Fatalf("scan retained: %v", err)
	}
	for _, k := range stale {
		if err := st.dropGeneration(k, func(uint64) bool { return true }); err != nil {
			t.Fatalf("drop: %v", err)
		}
	}
}

// An acknowledged object whose generation was demoted by an orphaned commit
// before the upgrade survives it. Rows written by an earlier release carry the
// time their commit demoted them and no release, and a release is the only
// thing that makes a generation reclaimable.
func TestAnAcknowledgedObjectSurvivesTheUpgrade(t *testing.T) {
	st, dir := openTestStore(t)
	oh := [32]byte{0x54}
	body := []byte("written before the upgrade and never released")

	// The state an earlier release leaves on disk: a generation a placement
	// record still names, demoted by a commit that never published.
	named := prepareValue(t, st, oh, 0, body)
	if _, err := st.Commit(oh, 0, named); err != nil {
		t.Fatalf("commit the named generation: %v", err)
	}
	orphan := prepareValue(t, st, oh, 0, []byte("a commit that never published"))
	if _, err := st.Commit(oh, 0, orphan); err != nil {
		t.Fatalf("commit the orphan generation: %v", err)
	}

	st = reopen(t, st, dir)

	if _, err := st.sweepRetained(-1); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	r, err := st.LookupAt(oh, 0, named)
	if err != nil {
		t.Fatalf("the upgrade reclaimed a generation the record still names: %v", err)
	}
	got, err := io.ReadAll(r)
	if closeErr := r.Close(); closeErr != nil {
		t.Fatalf("close: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("the object read back as another generation's body")
	}
}

// Rolling back reinstates a sweep that knows nothing about a release row and
// reclaims on the retained row's own stamp. That row is unchanged by this
// engine, so a rollback is the earlier release's behaviour and not a new
// hazard: an orphaned commit is reclaimed exactly as it was before, and a
// release this engine recorded reclaims nothing on its own.
func TestARollbackSeesTheRowsItWrote(t *testing.T) {
	st, dir := openTestStore(t)
	oh := [32]byte{0x55}

	epochs, _, _ := raceWriters(t, st, oh, 2)
	if err := st.ReleaseGeneration(oh, 0, epochs[0]); err != nil {
		t.Fatalf("release: %v", err)
	}

	st = reopen(t, st, dir)

	// An earlier release reclaims the demoted generation on its commit stamp.
	sweepRetainedByAge(t, st, -1)
	if r, err := st.LookupAt(oh, 0, epochs[0]); err == nil {
		_ = r.Close()
		t.Fatalf("the earlier sweep did not reclaim a generation past its cutoff")
	}

	// And the live generation is untouched by it, which is the property that
	// makes the rollback safe rather than merely unchanged.
	r, err := st.LookupAt(oh, 0, epochs[1])
	if err != nil {
		t.Fatalf("the earlier sweep took the live generation: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// Release is what starts the clock, and the grace window it promises has to
// be real: a reader that resolved the old record is still streaming from it.
func TestAReleasedGenerationSurvivesTheReaderGraceWindow(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x53}
	const writers = 2

	epochs, _, _ := raceWriters(t, st, oh, writers)

	if err := st.ReleaseGeneration(oh, 0, epochs[0]); err != nil {
		t.Fatalf("release: %v", err)
	}

	released, err := st.sweepRetained(retainedMaxAge)
	if err != nil {
		t.Fatalf("sweep inside the window: %v", err)
	}
	if released != 0 {
		t.Fatalf("sweep reclaimed %d generations inside the grace window, want none", released)
	}
	r, err := st.LookupAt(oh, 0, epochs[0])
	if err != nil {
		t.Fatalf("a released generation stopped answering inside its grace window: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Past the window, the release is honoured and the space comes back.
	released, err = st.sweepRetained(-1)
	if err != nil {
		t.Fatalf("sweep past the window: %v", err)
	}
	if released != 1 {
		t.Fatalf("sweep reclaimed %d generations past the window, want the released one", released)
	}
	if r, err := st.LookupAt(oh, 0, epochs[0]); err == nil {
		_ = r.Close()
		t.Fatalf("a released generation is still readable past its grace window")
	}
}
