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

// Each generation of a position is marked on its own: an older read that
// fails after a newer one does not replace the newer mark, and clearing one
// generation, epoch zero included, leaves the others.
func TestSuspectMarksAreKeptPerGeneration(t *testing.T) {
	s := newSuspectSet()
	pos := shardPosition{key: [32]byte{1}, index: 2}
	s.add(pos, 8)
	s.add(pos, 7)

	s.clearEpoch(pos, 0)
	if !s.suspect(pos, 8) {
		t.Fatal("an older generation's failure, or a legacy delete, must not clear the live mark")
	}
	if s.suspect(pos, 7) {
		t.Fatal("a stat of the live generation forgets the older one")
	}
	s.clearPosition(pos)
	if s.suspect(pos, 8) || s.len() != 0 {
		t.Fatal("a delete of the position forgets every generation")
	}
}

// BenchmarkSuspectLookup is the set's part of a stat, empty and full.
func BenchmarkSuspectLookup(b *testing.B) {
	for _, n := range []int{0, suspectCap} {
		b.Run(map[int]string{0: "empty", suspectCap: "full"}[n], func(b *testing.B) {
			s := newSuspectSet()
			for i := range n {
				s.add(shardPosition{key: [32]byte{byte(i), byte(i >> 8)}, index: 1}, 1)
			}
			pos := shardPosition{key: [32]byte{0xfe, 0xfe}, index: 1}
			b.ReportAllocs()
			for b.Loop() {
				s.suspect(pos, 1)
			}
		})
	}
}
