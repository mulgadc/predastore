package meta

import (
	"bytes"
	"encoding/binary"
	"testing"

	"pgregory.net/rapid"
)

// placementRecordWithID builds a minimal version-2+ placement record carrying
// epoch at the offset PlacementEpoch reads, with a trailing id so writes that
// share an epoch can still be told apart byte-for-byte.
func placementRecordWithID(version byte, epoch uint64, id int) []byte {
	b := make([]byte, 19, 27)
	b[0] = 0
	b[1] = version
	binary.BigEndian.PutUint64(b[11:19], epoch)
	return binary.BigEndian.AppendUint64(b, uint64(id))
}

// TestApplyPutMaxHighestEpochWins is the invariant the fence exists to hold:
// after any interleaving of writes against one key -- including epochs tied
// with the running maximum -- the stored record is the one with the highest
// epoch ever offered, and among ties the last such write applied wins, since
// applyPutMax only rejects a strictly older epoch.
func TestApplyPutMaxHighestEpochWins(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		db := newTestDB(t)
		fsm := NewFSM(db)
		const key = "objects/property-key"

		epochs := rapid.SliceOfN(rapid.Uint64Range(0, 20), 1, 60).Draw(rt, "epochs")

		var maxEpoch uint64
		var wantID int
		for i, epoch := range epochs {
			rec := placementRecordWithID(placementEpochMinVersion, epoch, i)
			if err := fsm.applyPutMax(key, rec, epoch); err != nil {
				rt.Fatalf("applyPutMax(write %d, epoch %d): %v", i, epoch, err)
			}

			// The stored epoch can only ever go up or stay the same, so every
			// write whose epoch matches or exceeds the running max is the one
			// that ends up on disk once the sequence is done.
			if i == 0 || epoch >= maxEpoch {
				maxEpoch = epoch
				wantID = i
			}
		}

		got, err := dbGet(db, []byte(key))
		if err != nil {
			rt.Fatalf("dbGet: %v", err)
		}

		want := placementRecordWithID(placementEpochMinVersion, maxEpoch, wantID)
		if !bytes.Equal(got, want) {
			rt.Fatalf("stored record = %x, want %x (highest epoch %d, offered by write %d of %d)",
				got, want, maxEpoch, wantID, len(epochs))
		}
	})
}
