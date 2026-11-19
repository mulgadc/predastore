package engine

import (
	"testing"
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
