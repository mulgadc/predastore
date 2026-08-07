package engine

import (
	"bytes"
	"testing"

	"pgregory.net/rapid"
)

func TestIdxEntryRoundTrip(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		var key [36]byte
		copy(key[:], rapid.SliceOfN(rapid.Byte(), 36, 36).Draw(rt, "key"))
		e := idxEntry{
			Off:   rapid.Int64Range(0, 1<<48).Draw(rt, "off"),
			Key:   key,
			PSize: rapid.Int64Range(0, 1<<48).Draw(rt, "psize"),
		}

		b := e.encode()
		if len(b) != idxEntrySize {
			rt.Fatalf("encode length = %d, want %d", len(b), idxEntrySize)
		}

		got, err := decodeIdxEntry(b)
		if err != nil {
			rt.Fatalf("decode: %v", err)
		}
		if got != e {
			rt.Fatalf("round-trip mismatch: got %+v want %+v", got, e)
		}
	})
}

func TestDecodeIdxEntryRejectsWrongLength(t *testing.T) {
	for _, n := range []int{0, 1, idxEntrySize - 1, idxEntrySize + 1, 2 * idxEntrySize} {
		if _, err := decodeIdxEntry(make([]byte, n)); err == nil {
			t.Errorf("decodeIdxEntry(len %d): want error, got nil", n)
		}
	}
}

func TestTombstoneKeyRoundTrip(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		segNum := rapid.Uint64().Draw(rt, "segNum")
		off := rapid.Int64Range(0, 1<<48).Draw(rt, "off")

		key := tombstoneKey(segNum, off)
		if len(key) != tombstoneKeySize {
			rt.Fatalf("key length = %d, want %d", len(key), tombstoneKeySize)
		}
		if key[0] != tombstonePrefix {
			rt.Fatalf("prefix = %q, want %q", key[0], tombstonePrefix)
		}
		if got := tombstoneSegNum(key); got != segNum {
			rt.Fatalf("segNum round-trip: got %d want %d", got, segNum)
		}
	})
}

func TestTombstoneKeyGroupsBySegNum(t *testing.T) {
	a := tombstoneKey(7, 100)
	b := tombstoneKey(7, 200)
	c := tombstoneKey(8, 100)

	if !bytes.Equal(a[:9], b[:9]) {
		t.Errorf("same segNum should share the 9-byte d‖segNum sub-prefix")
	}
	if bytes.Equal(a[:9], c[:9]) {
		t.Errorf("different segNum should not share the sub-prefix")
	}
}
