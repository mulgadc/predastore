package model

import "encoding/binary"

const (
	PlacementMagic       = 0
	PlacementEpochOffset = 11
	PlacementEpochEnd    = PlacementEpochOffset + 8
)

// PlacementEpoch reads the epoch in the fixed placement header.
func PlacementEpoch(record []byte) (uint64, bool) {
	if len(record) < PlacementEpochEnd || record[0] != PlacementMagic {
		return 0, false
	}
	switch record[1] {
	case 1:
	case 2:
		if len(record) < 27 {
			return 0, false
		}
	case 3, 4:
		if len(record) < 44 {
			return 0, false
		}
	default:
		return 0, false
	}
	return binary.BigEndian.Uint64(record[PlacementEpochOffset:PlacementEpochEnd]), true
}
