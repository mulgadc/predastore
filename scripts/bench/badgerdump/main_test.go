package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"strings"
	"testing"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/model"
)

func shardKey(hash [32]byte, index uint32) []byte {
	k := make([]byte, shardKeySize)
	copy(k, hash[:])
	binary.BigEndian.PutUint32(k[32:], index)
	return k
}

func extent(seg, off, psize, lsize uint64) []byte {
	v := make([]byte, extentSize)
	binary.BigEndian.PutUint64(v[0:8], seg)
	binary.BigEndian.PutUint64(v[8:16], off)
	binary.BigEndian.PutUint64(v[16:24], psize)
	binary.BigEndian.PutUint64(v[24:32], lsize)
	return v
}

func TestFormatBlobDecodesAShardRow(t *testing.T) {
	hash := model.ObjectHash("demo", "sample1.json")
	got := formatBlob(shardKey(hash, 1), extent(0, 2109454, 2109440, 2097152))

	for _, want := range []string{
		"shard", "key=f8c152e3aab9f63a..", "index=1",
		"seg=0", "off=2109454", "psize=2109440", "lsize=2097152",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("formatBlob() = %q, want it to contain %q", got, want)
		}
	}
}

func TestFormatBlobDecodesATombstoneRow(t *testing.T) {
	k := make([]byte, tombstoneKeySize)
	k[0] = tombstonePrefix
	binary.BigEndian.PutUint64(k[1:9], 0)
	binary.BigEndian.PutUint64(k[9:17], 14)
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, 2109440)

	got := formatBlob(k, v)
	if !strings.HasPrefix(got, "tomb") {
		t.Fatalf("formatBlob() = %q, want a tombstone row", got)
	}
	for _, want := range []string{"seg=0", "off=14", "psize=2109440"} {
		if !strings.Contains(got, want) {
			t.Errorf("formatBlob() = %q, want it to contain %q", got, want)
		}
	}
}

// A row the engine does not produce must still print rather than panic: the
// dumper's job is to report what is there, including whatever it cannot name.
func TestFormatBlobFallsBackOnAnUnknownRow(t *testing.T) {
	got := formatBlob([]byte("unexpected"), []byte{0x01, 0x02})
	if !strings.HasPrefix(got, "other") {
		t.Errorf("formatBlob() = %q, want the fallback row", got)
	}
	if !strings.Contains(got, "key=unexpected") {
		t.Errorf("formatBlob() = %q, want the key rendered as text", got)
	}
}

// A shard key with the wrong value width is not a shard row. Silently printing
// it as one would invent extent fields out of whatever bytes were there.
func TestFormatBlobRejectsAShardKeyWithAShortValue(t *testing.T) {
	hash := model.ObjectHash("demo", "sample1.json")
	got := formatBlob(shardKey(hash, 0), make([]byte, 16))
	if !strings.HasPrefix(got, "other") {
		t.Errorf("formatBlob() = %q, want the fallback row", got)
	}
}

func TestFormatMetaDecodesThePlacementRecord(t *testing.T) {
	hash := model.ObjectHash("demo", "sample1.json")
	var buf bytes.Buffer
	rec := handlers.ObjectToShardNodes{
		Object:           hash,
		Size:             4194304,
		DataShardNodes:   []config.NodeID{6, 12},
		ParityShardNodes: []config.NodeID{3},
	}
	if err := gob.NewEncoder(&buf).Encode(rec); err != nil {
		t.Fatalf("encode placement: %v", err)
	}

	key := append([]byte("objects/"), hash[:]...)
	got := formatMeta(key, buf.Bytes())

	for _, want := range []string{
		"placement", "size=4194304", "data=[6 12]", "parity=[3]",
		"object=f8c152e3aab9f63a..",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("formatMeta() = %q, want it to contain %q", got, want)
		}
	}
}

func TestFormatMetaDecodesTheNameRow(t *testing.T) {
	hash := model.ObjectHash("demo", "sample1.json")
	got := formatMeta([]byte("objects/arn:aws:s3:::demo/sample1.json"), hash[:])

	if !strings.Contains(got, "objects/arn:aws:s3:::demo/sample1.json") {
		t.Errorf("formatMeta() = %q, want the key rendered as text", got)
	}
	if !strings.Contains(got, "-> objecthash f8c152e3aab9f63a") {
		t.Errorf("formatMeta() = %q, want the object hash resolved", got)
	}
}

func TestFormatMetaPreviewsARowItCannotDecode(t *testing.T) {
	got := formatMeta([]byte("buckets/demo"), bytes.Repeat([]byte{0xff}, 269))
	if !strings.Contains(got, "buckets/demo") || !strings.Contains(got, "269 bytes") {
		t.Errorf("formatMeta() = %q, want the key and length reported", got)
	}
	if strings.Contains(got, "placement") {
		t.Errorf("formatMeta() = %q, want no placement claim for an undecodable row", got)
	}
}

// The object table mixes a text prefix with raw hash bytes, so neither pure
// rendering works on its own.
func TestPrintableRendersMixedKeysAsTextAndHex(t *testing.T) {
	hash := model.ObjectHash("demo", "sample1.json")
	tests := []struct {
		name string
		key  []byte
		want string
	}{
		{"all text", []byte("buckets/demo"), "buckets/demo"},
		{"text prefix, binary rest", append([]byte("objects/"), hash[:]...), "objects/<f8c152e3"},
		{"no separator, binary", []byte{0x00, 0x01}, "<0001>"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := printable(tt.key); !strings.HasPrefix(got, tt.want) {
				t.Errorf("printable() = %q, want prefix %q", got, tt.want)
			}
		})
	}
}

func TestPreviewTruncatesALongValue(t *testing.T) {
	if got := preview(bytes.Repeat([]byte{0xab}, 100)); !strings.HasSuffix(got, "..") {
		t.Errorf("preview() = %q, want it truncated", got)
	}
	if got := preview([]byte{0xab}); got != "ab" {
		t.Errorf("preview() = %q, want the whole value", got)
	}
}
