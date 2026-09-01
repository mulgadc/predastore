package handlers

import (
	"testing"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/meta"
)

// TestPlacementEpochAcceptsTheCurrentEncoding pins meta.PlacementEpoch against
// this package's own encoder. If a future version bump or field move stops
// meta from reading the epoch it fences on, this must fail here rather than
// silently going dead the way the version-2 hardcode did.
func TestPlacementEpochAcceptsTheCurrentEncoding(t *testing.T) {
	const epoch = 0x1112131415161718

	encoded, err := EncodePlacement(ObjectToShardNodes{
		Size:           4096,
		WriteEpoch:     epoch,
		DataShardNodes: []config.NodeID{1},
	})
	if err != nil {
		t.Fatalf("EncodePlacement() error = %v", err)
	}

	got, ok := meta.PlacementEpoch(encoded)
	if !ok {
		t.Fatalf("meta.PlacementEpoch() ok = false, want true for a version %d record", placementVersion)
	}
	if got != epoch {
		t.Errorf("meta.PlacementEpoch() = %#x, want %#x", got, uint64(epoch))
	}
}
