package meta

import (
	"encoding/binary"
	"testing"
)

// TestPlacementEpoch covers the cases applyPutMax's fence depends on:
// a current-version record yields its epoch, a version 1 record (whose
// epoch is random, not a timestamp) is rejected, and anything too short
// or carrying the wrong magic byte is rejected rather than misread.
func TestPlacementEpoch(t *testing.T) {
	record := func(magic, version byte, epoch uint64, length int) []byte {
		b := make([]byte, length)
		if len(b) > 0 {
			b[0] = magic
		}
		if len(b) > 1 {
			b[1] = version
		}
		if len(b) >= 19 {
			binary.BigEndian.PutUint64(b[11:19], epoch)
		}
		return b
	}

	tests := []struct {
		name      string
		record    []byte
		wantEpoch uint64
		wantOK    bool
	}{
		{"current version accepted", record(0, 2, 42, 19), 42, true},
		{"version 1 rejected", record(0, 1, 42, 19), 0, false},
		{"short record rejected", record(0, 2, 42, 18), 0, false},
		{"non-zero magic rejected", record(1, 2, 42, 19), 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			epoch, ok := PlacementEpoch(tt.record)
			if ok != tt.wantOK {
				t.Fatalf("PlacementEpoch() ok = %v, want %v", ok, tt.wantOK)
			}
			if epoch != tt.wantEpoch {
				t.Errorf("PlacementEpoch() epoch = %d, want %d", epoch, tt.wantEpoch)
			}
		})
	}
}
