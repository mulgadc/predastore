package placement

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/mulgadc/predastore/internal/config"
)

// spreadOf places sampleObjects synthetic objects and reports, per node, how
// many objects put a shard on it and how many of those were data shards.
func spreadOf(t *testing.T, nodes []config.NodeID, shards int) (
	placements map[string]int, touched, data map[config.NodeID]int,
) {
	t.Helper()
	ring := NewRing(nodes)
	placements = map[string]int{}
	touched = map[config.NodeID]int{}
	data = map[config.NodeID]int{}

	for i := range sampleObjects {
		hash := sha256.Sum256([]byte(fmt.Sprintf("bucket/object-%d.dat", i)))
		got, err := ring.Nodes(hash, shards)
		if err != nil {
			t.Fatalf("Nodes() error = %v", err)
		}
		if len(got) != shards {
			t.Fatalf("Nodes() returned %d nodes, want %d", len(got), shards)
		}
		placements[fmt.Sprint(got)]++
		for j, id := range got {
			touched[id]++
			if j < shards-1 {
				data[id]++
			}
		}
	}
	return placements, touched, data
}

// sampleObjects is large enough for the ratios below to be stable and small
// enough to stay well inside the unit-test budget.
const sampleObjects = 20000

// The ring's tuning is a durability property, not a preference. At
// partitionCount = 5 one node held a shard of every object in the cluster and
// another never held parity, so losing a single node degraded the entire
// keyspace at once. These are the properties that state cost us, asserted so
// that lowering the constant cannot pass review by accident.
func TestRingSpreadsShardsAcrossEveryNode(t *testing.T) {
	tests := []struct {
		name    string
		nodes   []config.NodeID
		shards  int
		wantAll int // every distinct set of nodes the ring can produce
	}{
		{"4 nodes, RS(2,1)", []config.NodeID{3, 6, 9, 12}, 3, 4},
		{"5 nodes, RS(2,1)", []config.NodeID{3, 6, 9, 12, 15}, 3, 5},
		{"5 nodes, RS(3,2)", []config.NodeID{3, 6, 9, 12, 15}, 5, 5},
		{"8 nodes, RS(3,2)", []config.NodeID{1, 2, 3, 4, 5, 6, 7, 8}, 5, 8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			placements, touched, data := spreadOf(t, tt.nodes, tt.shards)

			// Every rotation of the member order must be reachable. Three of
			// four here was the shape of the original defect.
			if len(placements) != tt.wantAll {
				t.Errorf("ring produced %d distinct placements, want %d: %v",
					len(placements), tt.wantAll, placements)
			}

			// The ideal is shards/len(nodes) of all objects on each node. A node
			// at 100% is a single point of correlated failure for the whole
			// cluster, which is the property that actually bit us.
			ideal := float64(tt.shards) / float64(len(tt.nodes))
			for _, id := range tt.nodes {
				got := float64(touched[id]) / float64(sampleObjects)
				if got > ideal*1.15 || got < ideal*0.85 {
					t.Errorf("node %d holds a shard of %.1f%% of objects, want within 15%% of %.1f%%",
						id, got*100, ideal*100)
				}
				// Node 12 held nothing but data shards under the old constants,
				// because the one placement putting it at a parity index never
				// occurred.
				if data[id] == 0 || data[id] == touched[id] {
					t.Errorf("node %d holds %d data shards of %d total: it never holds one kind",
						id, data[id], touched[id])
				}
			}
		})
	}
}

// Placement must be a pure function of the object hash and the member set:
// two rings built from the same nodes have to agree, or a second gate would
// write an object where the first cannot find it.
func TestRingIsDeterministicAcrossInstances(t *testing.T) {
	nodes := []config.NodeID{3, 6, 9, 12}
	a, b := NewRing(nodes), NewRing([]config.NodeID{12, 9, 6, 3})

	for i := range 1000 {
		hash := sha256.Sum256([]byte(fmt.Sprintf("bucket/object-%d.dat", i)))
		got, err := a.Nodes(hash, 3)
		if err != nil {
			t.Fatalf("Nodes() error = %v", err)
		}
		want, err := b.Nodes(hash, 3)
		if err != nil {
			t.Fatalf("Nodes() error = %v", err)
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("object %d placed at %v and %v: ring order changed placement", i, got, want)
		}
	}
}

// The library's distributor panics when no member has room for a partition,
// which is unreachable at load >= 1 and reachable below it.
func TestRingLoadFactorLeavesRoomForEveryPartition(t *testing.T) {
	if load < 1.0 {
		t.Fatalf("load = %v: below 1.0 the partition distributor can panic", load)
	}
	if partitionCount <= 16 {
		t.Errorf("partitionCount = %d is not far enough above any realistic member count",
			partitionCount)
	}
}
