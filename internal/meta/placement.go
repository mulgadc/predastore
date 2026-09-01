package meta

import "encoding/binary"

// placementEpochMinVersion is the lowest placement version whose epoch orders
// writes. Version 1 draws a random epoch rather than a timestamp, so a large
// one would permanently block every later update to that key.
const placementEpochMinVersion = 2

// PlacementEpoch reports a placement record's write epoch and whether it
// orders writes. The offset has been stable across every version so far; the
// version byte gating it has not, so it is read rather than assumed.
func PlacementEpoch(record []byte) (uint64, bool) {
	if len(record) < 19 || record[0] != 0 || record[1] < placementEpochMinVersion {
		return 0, false
	}
	return binary.BigEndian.Uint64(record[11:19]), true
}
