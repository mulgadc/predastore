package meta_test

import (
	"testing"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/meta"
)

// FuzzPlacementEpoch asserts meta.PlacementEpoch never panics. It indexes
// record[0], record[1] and record[11:19] directly, so a short or otherwise
// malformed record -- exactly what a corrupted or foreign value under a
// placement key would look like -- is the obvious way to crash the raft
// apply path that calls it on every stored value.
func FuzzPlacementEpoch(f *testing.F) {
	encoded, err := handlers.EncodePlacement(handlers.ObjectToShardNodes{
		Size:           4096,
		WriteEpoch:     0x0102030405060708,
		BlockSize:      1024,
		DataShardNodes: []config.NodeID{1, 2},
	})
	if err != nil {
		f.Fatalf("EncodePlacement() error = %v", err)
	}

	v1 := append([]byte(nil), encoded...)
	v1[1] = 1

	nonZeroMagic := append([]byte(nil), encoded...)
	nonZeroMagic[0] = 1

	f.Add(encoded)      // real encoder output (current version)
	f.Add(encoded[:10]) // truncated mid-header, shorter than the 19-byte fence needs
	f.Add([]byte{})     // empty
	f.Add(nonZeroMagic) // wrong magic
	f.Add(v1)           // version 1: epoch present but not a timestamp

	f.Fuzz(func(t *testing.T, record []byte) {
		meta.PlacementEpoch(record)
	})
}
