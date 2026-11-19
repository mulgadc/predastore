package engine

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
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

// retainedStamps collects the timestamp field of every retained row of one
// shard position, which is what both upgrade and rollback turn on.
func retainedStamps(t *testing.T, st *Store, oh [32]byte, idx uint32) []int64 {
	t.Helper()
	var stamps []int64
	if err := st.indexScan(retainedScan(MakeKey(oh, idx)), func(k, v []byte) error {
		if len(k) != retainedKeySize || len(v) != preparedValueSize {
			return nil
		}
		at, err := decodePreparedAt(v)
		if err != nil {
			return err
		}
		stamps = append(stamps, at)
		return nil
	}); err != nil {
		t.Fatalf("scan retained: %v", err)
	}
	return stamps
}

// A row written before this change carries the time its commit demoted the
// generation, which cannot be told apart from the time a publisher released
// one. Reclaiming it on that stamp destroys whatever the placement record
// still names, so an upgrade has to hold it.
func TestALegacyRetainedRowIsHeld(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x54}

	epochs, _, _ := raceWriters(t, st, oh, 2)
	legacy := epochs[0]

	// Restamp the retained row the way the previous release wrote it.
	rowKey := retainedKey(MakeKey(oh, 0), legacy)
	if err := st.index.Update(func(txn *badger.Txn) error {
		raw, err := readRaw(txn, rowKey)
		if err != nil {
			return err
		}
		ext, held, err := decodeIndexValue(raw)
		if err != nil {
			return err
		}
		return txn.Set(rowKey, encodePreparedValue(ext, held, time.Now().UnixNano()))
	}); err != nil {
		t.Fatalf("write legacy row: %v", err)
	}

	if _, err := st.sweepRetained(-1); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	r, err := st.LookupAt(oh, 0, legacy)
	if err != nil {
		t.Fatalf("a generation retained before the upgrade was reclaimed on its commit stamp: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// Rolling back reinstates a sweep that reclaims a retained row whenever its
// stamp is older than the cutoff. A held generation must never satisfy that,
// or the rollback destroys acknowledged data within one sweep interval.
func TestAnOlderSweeperDoesNotReclaimAHeldGeneration(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x55}

	// Two writers leave one generation demoted, and nobody has released it.
	raceWriters(t, st, oh, 2)

	stamps := retainedStamps(t, st, oh, 0)
	if len(stamps) != 1 {
		t.Fatalf("expected one retained generation, got %d", len(stamps))
	}

	// The predicate the previous release sweeps with, at its own default age.
	cutoff := time.Now().Add(-retainedMaxAge).UnixNano()
	if stamps[0] < cutoff {
		t.Fatalf("a held generation reads as %d, older than an earlier release's cutoff %d, so a rollback reclaims it",
			stamps[0], cutoff)
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
