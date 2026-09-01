package handlers

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"testing"
	"time"

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
			encoded, err := EncodePlacement(tt.rec)
			if err != nil {
				t.Fatalf("EncodePlacement() error = %v", err)
			}
			got, err := DecodePlacement(encoded)
			if err != nil {
				t.Fatalf("DecodePlacement() error = %v", err)
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
		}, 47},
		{"RS(7,3)", ObjectToShardNodes{
			DataShardNodes:   []config.NodeID{1, 2, 3, 4, 5, 6, 7},
			ParityShardNodes: []config.NodeID{8, 9, 10},
		}, 54},
		{"RS(17,3)", ObjectToShardNodes{
			DataShardNodes: make([]config.NodeID, 17), ParityShardNodes: make([]config.NodeID, 3),
		}, 64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := EncodePlacement(tt.rec)
			if err != nil {
				t.Fatalf("EncodePlacement() error = %v", err)
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

	if _, err := DecodePlacement(buf.Bytes()); !errors.Is(err, errPlacementFormat) {
		t.Errorf("DecodePlacement(gob) error = %v, want errPlacementFormat", err)
	}
}

func TestPlacementRecordRejectsMalformedInput(t *testing.T) {
	good, err := EncodePlacement(ObjectToShardNodes{
		Size: 1, DataShardNodes: []config.NodeID{3, 6}, ParityShardNodes: []config.NodeID{9},
	})
	if err != nil {
		t.Fatalf("EncodePlacement() error = %v", err)
	}

	// The digest marker is a single 0x00 byte here, since this record carries
	// no digest, so the node ids start one byte past the fixed header.
	nodeIDStart := placementFixedSize + 1
	truncatedIDs := append([]byte(nil), good[:nodeIDStart+1]...)

	unknownVersion := append([]byte(nil), good...)
	unknownVersion[1] = 0x04

	malformedUvarint := append([]byte(nil), good[:nodeIDStart]...)
	malformedUvarint = append(malformedUvarint, 0x80) // continuation bit with nothing after it

	malformedMarker := append([]byte(nil), good[:placementFixedSize]...)
	malformedMarker = append(malformedMarker, 0x80) // continuation bit with nothing after it

	tests := []struct {
		name  string
		input []byte
	}{
		{"empty", nil},
		{"shorter than the header", good[:placementFixedSize-1]},
		{"unknown version", unknownVersion},
		{"fewer node ids than k declares", truncatedIDs},
		{"malformed node id uvarint", malformedUvarint},
		{"malformed digest marker uvarint", malformedMarker},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := DecodePlacement(tt.input); err == nil {
				t.Errorf("DecodePlacement(%x) succeeded, want an error", tt.input)
			}
		})
	}
}

// The parity slice shares its backing array with the data slice, so appending
// to one must not reach into the other.
func TestPlacementRecordSlicesDoNotAlias(t *testing.T) {
	encoded, err := EncodePlacement(ObjectToShardNodes{
		Size: 1, DataShardNodes: []config.NodeID{3, 6}, ParityShardNodes: []config.NodeID{9},
	})
	if err != nil {
		t.Fatalf("EncodePlacement() error = %v", err)
	}
	got, err := DecodePlacement(encoded)
	if err != nil {
		t.Fatalf("DecodePlacement() error = %v", err)
	}

	got.DataShardNodes = append(got.DataShardNodes, 99)
	if got.ParityShardNodes[0] != 9 {
		t.Errorf("appending to the data shards overwrote parity: got %v", got.ParityShardNodes)
	}
}

func TestPlacementRecordRejectsUnencodableValues(t *testing.T) {
	if _, err := EncodePlacement(ObjectToShardNodes{Size: -1}); err == nil {
		t.Error("EncodePlacement(negative size) succeeded, want an error")
	}
	if _, err := EncodePlacement(ObjectToShardNodes{
		DataShardNodes: make([]config.NodeID, maxDataShards+1),
	}); err == nil {
		t.Error("EncodePlacement(256 data shards) succeeded, want an error")
	}
}

// The header is a wire format: its field offsets are asserted directly so a
// reordering that still round-trips through this package cannot pass silently.
func TestPlacementRecordHeaderLayout(t *testing.T) {
	digest := bytes.Repeat([]byte{0xab}, 16)
	encoded, err := EncodePlacement(ObjectToShardNodes{
		Size: 0x0102030405060708, WriteEpoch: 0x1112131415161718,
		BlockSize:      0x2122232425262728,
		Digest:         digest,
		PartCount:      5,
		DataShardNodes: []config.NodeID{6, 12}, ParityShardNodes: []config.NodeID{3},
	})
	if err != nil {
		t.Fatalf("EncodePlacement() error = %v", err)
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
	if got := binary.BigEndian.Uint64(encoded[19:27]); got != 0x2122232425262728 {
		t.Errorf("block size at offset 19 = %#x", got)
	}
	if !bytes.Equal(encoded[27:43], digest) {
		t.Errorf("digest at offset 27 = %x, want %x", encoded[27:43], digest)
	}
	// The marker is 1 + PartCount, so a composite of 5 parts is 6, which fits
	// in a single uvarint byte.
	if encoded[43] != 6 {
		t.Errorf("digest marker at offset 43 = %d, want 6", encoded[43])
	}
	if want := []byte{6, 12, 3}; !bytes.Equal(encoded[44:], want) {
		t.Errorf("node ids = %v, want %v", encoded[44:], want)
	}
}

// A record with no digest zero-fills the digest slot and marks it absent with
// a single 0x00 byte, so decoding it back leaves Digest nil rather than
// treating the zeros as a real MD5.
func TestPlacementRecordWithNoDigestRoundTrips(t *testing.T) {
	encoded, err := EncodePlacement(ObjectToShardNodes{
		Size: 1, DataShardNodes: []config.NodeID{3, 6}, ParityShardNodes: []config.NodeID{9},
	})
	if err != nil {
		t.Fatalf("EncodePlacement() error = %v", err)
	}
	if !bytes.Equal(encoded[27:43], make([]byte, 16)) {
		t.Errorf("digest slot = %x, want all zero", encoded[27:43])
	}
	if encoded[43] != 0 {
		t.Errorf("digest marker = %d, want 0", encoded[43])
	}

	decoded, err := DecodePlacement(encoded)
	if err != nil {
		t.Fatalf("DecodePlacement() error = %v", err)
	}
	if decoded.Digest != nil {
		t.Errorf("Digest = %x, want nil", decoded.Digest)
	}
	if etag, ok := decoded.ETag(); ok {
		t.Errorf("ETag() = %q, true, want ok = false", etag)
	}
}

// A plain content digest and a composite multipart digest both round-trip,
// and the marker tells them apart on the way back.
func TestPlacementRecordDigestRoundTrips(t *testing.T) {
	digest := bytes.Repeat([]byte{0x42}, 16)

	t.Run("plain digest", func(t *testing.T) {
		encoded, err := EncodePlacement(ObjectToShardNodes{
			Size: 4, Digest: digest,
			DataShardNodes: []config.NodeID{1}, ParityShardNodes: []config.NodeID{},
		})
		if err != nil {
			t.Fatalf("EncodePlacement() error = %v", err)
		}
		decoded, err := DecodePlacement(encoded)
		if err != nil {
			t.Fatalf("DecodePlacement() error = %v", err)
		}
		if !bytes.Equal(decoded.Digest, digest) || decoded.PartCount != 0 {
			t.Fatalf("decoded = digest=%x parts=%d, want digest=%x parts=0",
				decoded.Digest, decoded.PartCount, digest)
		}
		if etag, ok := decoded.ETag(); !ok || etag != fmt.Sprintf("\"%x\"", digest) {
			t.Errorf("ETag() = %q, %v, want %q, true", etag, ok, fmt.Sprintf("\"%x\"", digest))
		}
	})

	t.Run("composite digest", func(t *testing.T) {
		encoded, err := EncodePlacement(ObjectToShardNodes{
			Size: 4, Digest: digest, PartCount: 3,
			DataShardNodes: []config.NodeID{1}, ParityShardNodes: []config.NodeID{},
		})
		if err != nil {
			t.Fatalf("EncodePlacement() error = %v", err)
		}
		decoded, err := DecodePlacement(encoded)
		if err != nil {
			t.Fatalf("DecodePlacement() error = %v", err)
		}
		if !bytes.Equal(decoded.Digest, digest) || decoded.PartCount != 3 {
			t.Fatalf("decoded = digest=%x parts=%d, want digest=%x parts=3",
				decoded.Digest, decoded.PartCount, digest)
		}
		if etag, ok := decoded.ETag(); !ok || etag != fmt.Sprintf("\"%x-3\"", digest) {
			t.Errorf("ETag() = %q, %v, want %q, true", etag, ok, fmt.Sprintf("\"%x-3\"", digest))
		}
	})
}

// Versions 1 and 2 were written to real stores and predate content digests.
// They are refused by number rather than decoded, because an object whose ETag
// cannot be produced is worse than one that is missing.
func TestLegacyRecordVersionsAreRefused(t *testing.T) {
	v1 := []byte{placementMagic, 0x01, 1}
	v1 = binary.BigEndian.AppendUint64(v1, 4096)
	v1 = binary.BigEndian.AppendUint64(v1, 0x1112131415161718)
	v1 = append(v1, 6)
	v1 = append(v1, make([]byte, placementFixedSize-len(v1))...)

	v2 := []byte{placementMagic, 0x02, 1}
	v2 = binary.BigEndian.AppendUint64(v2, 4096)
	v2 = binary.BigEndian.AppendUint64(v2, 0x1112131415161718)
	v2 = binary.BigEndian.AppendUint64(v2, 4096)
	v2 = append(v2, 6)
	v2 = append(v2, make([]byte, placementFixedSize-len(v2))...)

	for name, raw := range map[string][]byte{"version 1": v1, "version 2": v2} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodePlacement(raw); !errors.Is(err, errPlacementLegacy) {
				t.Errorf("DecodePlacement(%s) error = %v, want errPlacementLegacy", name, err)
			}
		})
	}
}

// The write epoch is a minted timestamp, and ModifiedAt is what every surface
// serving a LastModified reads.
func TestARecordReportsItsModificationTime(t *testing.T) {
	minter := mustEpochs(3)
	at := time.UnixMilli(1_800_000_000_123)
	frozen(minter, at)
	epoch, err := minter.Next()
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}

	encoded, err := EncodePlacement(ObjectToShardNodes{
		Size: 4096, WriteEpoch: epoch, BlockSize: 4096,
		DataShardNodes: []config.NodeID{6}, ParityShardNodes: []config.NodeID{3},
	})
	if err != nil {
		t.Fatalf("EncodePlacement() error = %v", err)
	}
	fresh, err := DecodePlacement(encoded)
	if err != nil {
		t.Fatalf("DecodePlacement() error = %v", err)
	}
	if got := fresh.ModifiedAt(); !got.Equal(at) {
		t.Errorf("modification time = %s, want %s", got, at.UTC())
	}
}
