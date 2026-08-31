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

// Retention is bounded by count as well as age, or a hot key would pin a
// generation per overwrite for the whole window. The newest survive; the live
// row is never a candidate.
//
// The cap is asserted after a sweep because the sweep is what enforces it.
// Commit prunes too, but each writer's transaction sees its own snapshot, so
// concurrent commits of one position can leave more than the cap between them.
func TestRetentionKeepsOnlyTheNewestGenerations(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x43}
	const writers = retainedGenerations + 3

	epochs, _, _ := raceWriters(t, st, oh, writers)

	// Nothing is old enough to age out, so anything dropped here is dropped by
	// the count bound alone.
	if _, err := st.sweepRetained(retainedMaxAge, retainedGenerations); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	kept := 0
	if err := countRetained(st, MakeKey(oh, 0), func() { kept++ }); err != nil {
		t.Fatalf("scan retained: %v", err)
	}
	if kept != retainedGenerations {
		t.Fatalf("retained %d generations, want the cap of %d", kept, retainedGenerations)
	}

	// The oldest is gone and the newest superseded one is not.
	if r, err := st.LookupAt(oh, 0, epochs[0]); err == nil {
		_ = r.Close()
		t.Fatalf("the oldest generation outlived the cap")
	}
	r, err := st.LookupAt(oh, 0, epochs[writers-2])
	if err != nil {
		t.Fatalf("the newest superseded generation was pruned: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// A superseded generation is answerable, not immortal: the sweep is the only
// thing that decides one is dead, and it has to actually reclaim the space.
func TestTheSweepReleasesRetainedGenerations(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x44}
	const writers = 3

	epochs, _, _ := raceWriters(t, st, oh, writers)

	released, err := st.sweepRetained(-1, retainedGenerations)
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
