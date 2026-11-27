package blob

import "testing"

// A full set of positions whose shards are gone still admits a new failure:
// the oldest entry gives way, and the eviction is counted.
func TestAFullSuspectSetAdmitsANewFailure(t *testing.T) {
	s := newSuspectSet()
	for i := range suspectCap {
		s.add(shardPosition{key: [32]byte{byte(i), byte(i >> 8)}, index: 1}, 1)
	}
	fresh := shardPosition{key: [32]byte{0xff, 0xff}, index: 2}

	if !s.add(fresh, 9) {
		t.Fatal("a new failure must enter a full set")
	}
	if !s.suspect(fresh, 9) {
		t.Fatal("the new failure must be reported")
	}
	if s.suspect(shardPosition{key: [32]byte{0, 0}, index: 1}, 1) {
		t.Fatal("the oldest entry must be the one evicted")
	}
	if s.len() != suspectCap || s.evicted.Load() != 1 {
		t.Fatalf("len %d, evicted %d", s.len(), s.evicted.Load())
	}
}
