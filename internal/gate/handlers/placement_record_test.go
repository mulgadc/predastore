package handlers

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"testing"

	"github.com/mulgadc/predastore/internal/config"
)

func TestPlacementRecordRoundTrips(t *testing.T) {
	tests := []struct {
		name string
		rec  ObjectToShardNodes
	}{
		{"RS(2,1)", ObjectToShardNodes{
			Size: 4194304, WriteEpoch: 0x0123456789abcdef,
			DataShardNodes: []config.NodeID{6, 12}, ParityShardNodes: []config.NodeID{3},
		}},
		{"RS(1,0), no parity", ObjectToShardNodes{
			Size: 17, WriteEpoch: 1, DataShardNodes: []config.NodeID{9},
			ParityShardNodes: []config.NodeID{},
		}},
		{"RS(7,3)", ObjectToShardNodes{
			Size: 1 << 40, WriteEpoch: ^uint64(0),
			DataShardNodes:   []config.NodeID{1, 2, 3, 4, 5, 6, 7},
			ParityShardNodes: []config.NodeID{8, 9, 10},
		}},
		{"zero size and zero epoch", ObjectToShardNodes{
			Size: 0, WriteEpoch: 0,
			DataShardNodes: []config.NodeID{3, 6}, ParityShardNodes: []config.NodeID{9},
		}},
		{"node ids above one uvarint byte", ObjectToShardNodes{
			Size: 1, WriteEpoch: 2,
			DataShardNodes: []config.NodeID{127, 128}, ParityShardNodes: []config.NodeID{1 << 40},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := encodePlacement(tt.rec)
			if err != nil {
				t.Fatalf("encodePlacement() error = %v", err)
			}
			got, err := decodePlacement(encoded)
			if err != nil {
				t.Fatalf("decodePlacement() error = %v", err)
			}
			if got.Size != tt.rec.Size || got.WriteEpoch != tt.rec.WriteEpoch {
				t.Errorf("size/epoch = %d/%d, want %d/%d",
					got.Size, got.WriteEpoch, tt.rec.Size, tt.rec.WriteEpoch)
			}
			if !equalIDs(got.DataShardNodes, tt.rec.DataShardNodes) {
				t.Errorf("data nodes = %v, want %v", got.DataShardNodes, tt.rec.DataShardNodes)
			}
			if !equalIDs(got.ParityShardNodes, tt.rec.ParityShardNodes) {
				t.Errorf("parity nodes = %v, want %v", got.ParityShardNodes, tt.rec.ParityShardNodes)
			}
		})
	}
}

func equalIDs(a, b []config.NodeID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// The size claim is the whole reason for the format, so it is asserted rather
// than described: a change that quietly regrows the record fails here.
func TestPlacementRecordSize(t *testing.T) {
	tests := []struct {
		name string
		rec  ObjectToShardNodes
		want int
	}{
		{"RS(2,1)", ObjectToShardNodes{
			DataShardNodes: []config.NodeID{6, 12}, ParityShardNodes: []config.NodeID{3},
		}, 22},
		{"RS(7,3)", ObjectToShardNodes{
			DataShardNodes:   []config.NodeID{1, 2, 3, 4, 5, 6, 7},
			ParityShardNodes: []config.NodeID{8, 9, 10},
		}, 29},
		{"RS(17,3)", ObjectToShardNodes{
			DataShardNodes: make([]config.NodeID, 17), ParityShardNodes: make([]config.NodeID, 3),
		}, 39},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := encodePlacement(tt.rec)
			if err != nil {
				t.Fatalf("encodePlacement() error = %v", err)
			}
			if len(encoded) != tt.want {
				t.Errorf("record is %d bytes, want %d", len(encoded), tt.want)
			}
		})
	}
}

// The magic byte exists so a store written before the cutover fails loudly
// instead of being handed to a decoder that would read nonsense out of it.
func TestPlacementRecordRejectsAGobRecord(t *testing.T) {
	type legacy struct {
		Object           [32]byte
		Size             int64
		DataShardNodes   []config.NodeID
		ParityShardNodes []config.NodeID
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(legacy{
		Size: 4194304, DataShardNodes: []config.NodeID{6, 12}, ParityShardNodes: []config.NodeID{3},
	}); err != nil {
		t.Fatalf("encode legacy record: %v", err)
	}
	if buf.Bytes()[0] == placementMagic {
		t.Fatalf("a gob stream began with the magic byte, so it cannot discriminate")
	}

	if _, err := decodePlacement(buf.Bytes()); !errors.Is(err, errPlacementFormat) {
		t.Errorf("decodePlacement(gob) error = %v, want errPlacementFormat", err)
	}
}

func TestPlacementRecordRejectsMalformedInput(t *testing.T) {
	good, err := encodePlacement(ObjectToShardNodes{
		Size: 1, DataShardNodes: []config.NodeID{3, 6}, ParityShardNodes: []config.NodeID{9},
	})
	if err != nil {
		t.Fatalf("encodePlacement() error = %v", err)
	}

	truncatedIDs := append([]byte(nil), good[:placementFixedSize+1]...)

	unknownVersion := append([]byte(nil), good...)
	unknownVersion[1] = 0x02

	malformedUvarint := append([]byte(nil), good[:placementFixedSize]...)
	malformedUvarint = append(malformedUvarint, 0x80) // continuation bit with nothing after it

	tests := []struct {
		name  string
		input []byte
	}{
		{"empty", nil},
		{"shorter than the header", good[:placementFixedSize-1]},
		{"unknown version", unknownVersion},
		{"fewer node ids than k declares", truncatedIDs},
		{"malformed uvarint", malformedUvarint},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := decodePlacement(tt.input); err == nil {
				t.Errorf("decodePlacement(%x) succeeded, want an error", tt.input)
			}
		})
	}
}

// The parity slice shares its backing array with the data slice, so appending
// to one must not reach into the other.
func TestPlacementRecordSlicesDoNotAlias(t *testing.T) {
	encoded, err := encodePlacement(ObjectToShardNodes{
		Size: 1, DataShardNodes: []config.NodeID{3, 6}, ParityShardNodes: []config.NodeID{9},
	})
	if err != nil {
		t.Fatalf("encodePlacement() error = %v", err)
	}
	got, err := decodePlacement(encoded)
	if err != nil {
		t.Fatalf("decodePlacement() error = %v", err)
	}

	got.DataShardNodes = append(got.DataShardNodes, 99)
	if got.ParityShardNodes[0] != 9 {
		t.Errorf("appending to the data shards overwrote parity: got %v", got.ParityShardNodes)
	}
}

func TestPlacementRecordRejectsUnencodableValues(t *testing.T) {
	if _, err := encodePlacement(ObjectToShardNodes{Size: -1}); err == nil {
		t.Error("encodePlacement(negative size) succeeded, want an error")
	}
	if _, err := encodePlacement(ObjectToShardNodes{
		DataShardNodes: make([]config.NodeID, maxDataShards+1),
	}); err == nil {
		t.Error("encodePlacement(256 data shards) succeeded, want an error")
	}
}

// The header is a wire format: its field offsets are asserted directly so a
// reordering that still round-trips through this package cannot pass silently.
func TestPlacementRecordHeaderLayout(t *testing.T) {
	encoded, err := encodePlacement(ObjectToShardNodes{
		Size: 0x0102030405060708, WriteEpoch: 0x1112131415161718,
		DataShardNodes: []config.NodeID{6, 12}, ParityShardNodes: []config.NodeID{3},
	})
	if err != nil {
		t.Fatalf("encodePlacement() error = %v", err)
	}

	if encoded[0] != placementMagic || encoded[1] != placementVersion {
		t.Errorf("header = %x %x, want magic %x version %x",
			encoded[0], encoded[1], placementMagic, placementVersion)
	}
	if encoded[2] != 2 {
		t.Errorf("k = %d, want 2", encoded[2])
	}
	if got := binary.BigEndian.Uint64(encoded[3:11]); got != 0x0102030405060708 {
		t.Errorf("size at offset 3 = %#x", got)
	}
	if got := binary.BigEndian.Uint64(encoded[11:19]); got != 0x1112131415161718 {
		t.Errorf("epoch at offset 11 = %#x", got)
	}
	if want := []byte{6, 12, 3}; !bytes.Equal(encoded[19:], want) {
		t.Errorf("node ids = %v, want %v", encoded[19:], want)
	}
}
