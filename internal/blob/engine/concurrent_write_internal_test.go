package engine

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"
)

// These are the properties a shard position has to hold when several writers
// race it, which is the ordinary case for a client that retries a slow PUT as
// much as for two clients writing one key. None of it needs a cluster: the
// contention is between two callers of one Store, so this is where it belongs
// and where -race can see it.

// raceWriters prepares and commits n generations of one shard position
// concurrently and returns each writer's epoch, body and verdict, in epoch
// order. Every writer gets its own epoch up front so the intended ordering is
// known before any of them runs.
func raceWriters(t *testing.T, st *Store, oh [32]byte, n int) (epochs []uint64, bodies [][]byte, published []bool) {
	t.Helper()

	epochs = make([]uint64, n)
	bodies = make([][]byte, n)
	published = make([]bool, n)
	errs := make([]error, n)
	for i := range n {
		epochs[i] = nextTestEpoch()
		bodies[i] = bytes.Repeat([]byte(fmt.Sprintf("g%d", i)), 3*KiB)
	}

	// Prepared first, all of them, so the commits contend rather than merely
	// following one another.
	for i := range n {
		w, err := st.Append(oh, 0, int64(len(bodies[i])), epochs[i])
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if _, err := w.Write(bodies[i]); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("close writer %d: %v", i, err)
		}
	}

	release := make(chan struct{})
	var ready, done sync.WaitGroup
	for i := range n {
		ready.Add(1)
		done.Go(func() {
			ready.Done()
			<-release
			published[i], errs[i] = st.Commit(oh, 0, epochs[i])
		})
	}
	ready.Wait()
	close(release)
	done.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("writer %d (epoch %016x) failed its commit: %v", i, epochs[i], err)
		}
	}
	return epochs, bodies, published
}

// Losing a race is not a failure. Every writer is acknowledged, and exactly one
// of them — the highest epoch — ends up as the live generation, whatever order
// the commits actually interleaved in.
func TestConcurrentWritersOfOneShardAllSucceedAndTheHighestEpochWins(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x41}
	const writers = 4

	epochs, bodies, published := raceWriters(t, st, oh, writers)

	if !published[writers-1] {
		t.Fatalf("the highest epoch %016x reports it published nothing", epochs[writers-1])
	}
	if got := readValue(t, st, oh, 0); !bytes.Equal(got, bodies[writers-1]) {
		t.Fatalf("live generation is not the highest epoch's body")
	}

	r, err := st.Lookup(oh, 0)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	live := r.Epoch()
	if err := r.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if live != epochs[writers-1] {
		t.Fatalf("live epoch %016x, want the highest %016x", live, epochs[writers-1])
	}
}

// A record naming any generation that committed must still resolve. This is
// what stops a placement record that lost its own race from pointing at
// nothing, which is the whole defect: the record and the shards used to pick
// their winner independently and could disagree for good.
func TestEveryCommittedGenerationStaysReadable(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x42}
	const writers = 4 // retainedGenerations, so none is pruned for count

	epochs, bodies, _ := raceWriters(t, st, oh, writers)

	for i, epoch := range epochs {
		r, err := st.LookupAt(oh, 0, epoch)
		if err != nil {
			t.Fatalf("generation %d (epoch %016x) is unreadable: %v", i, epoch, err)
		}
		got, err := io.ReadAll(r)
		if closeErr := r.Close(); closeErr != nil {
			t.Fatalf("close %d: %v", i, closeErr)
		}
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if !bytes.Equal(got, bodies[i]) {
			t.Fatalf("generation %d read back as another writer's body", i)
		}
	}
}

// No count bounds retention, however many writers race one position. This is
// the regression guard for acknowledged objects going unreadable: the cap that
// used to be here ranked generations by epoch, which is write *start* order,
// while the record that survives is whichever write finished last. Any writer
// the cap evicted could be the one the metadata went on to name.
//
// Every epoch here is fresh, so a generation missing after the sweep was
// dropped by a count and by nothing else.
func TestRetentionKeepsEveryFreshGeneration(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x43}
	const writers = 12

	epochs, _, _ := raceWriters(t, st, oh, writers)

	if _, err := st.sweepRetained(retainedMaxAge); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	kept := 0
	if err := countRetained(st, MakeKey(oh, 0), func() { kept++ }); err != nil {
		t.Fatalf("scan retained: %v", err)
	}
	if kept != writers-1 {
		t.Fatalf("retained %d generations of %d superseded, want all of them", kept, writers-1)
	}

	// Including the lowest epoch, which is the one a count bound drops first
	// and is as likely as any other to be the generation the record names.
	for i, epoch := range epochs[:writers-1] {
		r, err := st.LookupAt(oh, 0, epoch)
		if err != nil {
			t.Fatalf("generation %d of %d was reclaimed while fresh: %v", i, writers, err)
		}
		if err := r.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}
}

// A superseded generation is answerable, not immortal: the sweep is the only
// thing that decides one is dead, and it has to actually reclaim the space.
func TestTheSweepReleasesRetainedGenerations(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x44}
	const writers = 3

	epochs, _, _ := raceWriters(t, st, oh, writers)

	released, err := st.sweepRetained(-1)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if released != writers-1 {
		t.Fatalf("sweep released %d generations, want %d", released, writers-1)
	}
	if r, err := st.LookupAt(oh, 0, epochs[0]); err == nil {
		_ = r.Close()
		t.Fatalf("a released generation is still readable")
	}
	// The live row is not a retained generation and must survive the sweep.
	r, err := st.LookupAt(oh, 0, epochs[writers-1])
	if err != nil {
		t.Fatalf("the sweep took the live generation: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// A generation whose record has been replaced is released by the writer that
// replaced it, so retention is bounded by the writes in flight rather than by
// the age cutoff. The row is backdated rather than dropped: a reader that
// resolved the old record a moment ago is still streaming from it, and taking
// the bytes now would turn a rare lost write into a routine failed read.
func TestReleaseGenerationHandsBackASupersededGeneration(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x45}
	const writers = 3

	epochs, _, _ := raceWriters(t, st, oh, writers)

	if err := st.ReleaseGeneration(oh, 0, epochs[0]); err != nil {
		t.Fatalf("release: %v", err)
	}

	r, err := st.LookupAt(oh, 0, epochs[0])
	if err != nil {
		t.Fatalf("a released generation stopped answering before the sweep took it: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	released, err := st.sweepRetained(retainedMaxAge)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if released != 1 {
		t.Fatalf("sweep released %d generations, want only the one handed back", released)
	}
	if r, err := st.LookupAt(oh, 0, epochs[0]); err == nil {
		_ = r.Close()
		t.Fatalf("the released generation survived the sweep")
	}
	// The other superseded generation was never released and is still fresh.
	r, err = st.LookupAt(oh, 0, epochs[1])
	if err != nil {
		t.Fatalf("the sweep took a generation nobody released: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// Releasing reaches only the retained namespace, so a caller working from a
// stale placement record cannot ask a node to hand back the object it is
// currently serving.
func TestReleaseGenerationCannotTouchTheLiveGeneration(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x46}
	const writers = 2

	epochs, bodies, _ := raceWriters(t, st, oh, writers)
	live := epochs[writers-1]

	if err := st.ReleaseGeneration(oh, 0, live); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, err := st.sweepRetained(retainedMaxAge); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	if got := readValue(t, st, oh, 0); !bytes.Equal(got, bodies[writers-1]) {
		t.Fatalf("releasing the live epoch changed what the position serves")
	}
}

// countRetained calls seen once per retained row of one shard position.
func countRetained(st *Store, idxKey []byte, seen func()) error {
	prefix := retainedScan(idxKey)

	return st.indexScan(prefix, func(k, _ []byte) error {
		if len(k) == retainedKeySize {
			seen()
		}
		return nil
	})
}
