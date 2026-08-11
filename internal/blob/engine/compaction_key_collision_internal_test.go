package engine

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// collidingHash builds a hash that starts with tombstonePrefix, so its index
// key shares the tombstone namespace's prefix. Roughly one hash in 256
// looks like this in the field; here it is chosen rather than waited for.
func collidingHash(tag byte) [32]byte {
	var oh [32]byte
	oh[0] = tombstonePrefix
	// Bytes 1..8 are what tombstoneSegNum would misread as a segment number.
	// Make them large and distinctive so a leaked one is unmistakable on sight.
	for i := 1; i < 9; i++ {
		oh[i] = 0xF0 | tag
	}
	oh[9] = tag
	return oh
}

// TestCandidateSegmentsIgnoresKeysInTombstoneNamespace asserts that a live
// value whose hash begins with tombstonePrefix is not mistaken for a
// tombstone. Keys carry no namespace prefix, so a prefix scan for
// tombstones also returns them; reading a segment number out of hash bytes 1..8
// invents one, and the append path would then materialise a segment file named
// after it.
func TestCandidateSegmentsIgnoresKeysInTombstoneNamespace(t *testing.T) {
	st, dir := openTestStore(t, WithMaxSegSize(40*KiB))

	body := bytes.Repeat([]byte{0xcc}, 12*KiB)

	// Live values only, every one colliding with the tombstone prefix. Nothing
	// is deleted, so a correct scan finds no tombstones and no candidates.
	const values = 4
	for i := range values {
		putValue(t, st, collidingHash(byte(i)), 0, body)
	}

	if got := tombstones(t, st); len(got) != 0 {
		t.Fatalf("no value was deleted, so the tombstone namespace must be empty, got %v", got)
	}

	cands, failed, err := st.candidateSegments()
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if failed != 0 {
		t.Errorf("live keys were read as unreadable segments: failed=%d", failed)
	}
	if len(cands) != 0 {
		t.Errorf("live keys were selected as compaction candidates: %v", cands)
	}

	// The invented segment number is the tell: hash bytes 1..8 as a big-endian
	// uint64. Nothing may exist on disk under that name.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	for _, e := range entries {
		for i := range values {
			oh := collidingHash(byte(i))
			ghost := fmt.Sprintf(segFilename, tombstoneSegNum(oh[:]))
			if e.Name() == filepath.Base(ghost) {
				t.Errorf("key fabricated segment %s from its own hash", e.Name())
			}
		}
	}
}

// TestDeleteTombstonesSparesKeysInTombstoneNamespace pins the contract that
// deleteTombstones removes tombstones and nothing else: its scan feeds deletes,
// so any non-tombstone key it returns is unlinked from the index while its bytes
// stay on disk — a live value lost silently.
//
// Unlike the candidateSegments collision, which needs only a hash starting with
// tombstonePrefix (~1 in 256, observed in the field), reaching this one through
// a real caller would need hash bytes 1..8 to equal an actual segment number as
// well, since callers pass real segment numbers. The width check is therefore
// defence in depth, and this test forces the collision directly rather than
// waiting on odds no one should rely on.
func TestDeleteTombstonesSparesKeysInTombstoneNamespace(t *testing.T) {
	st, _ := openTestStore(t, WithMaxSegSize(40*KiB))

	body := bytes.Repeat([]byte{0xcc}, 12*KiB)
	oh := collidingHash(0x01)
	putValue(t, st, oh, 0, body)

	// Target the exact segment number this value's key would be misread as, so
	// the prefix under deletion is the one its key sits beneath.
	ghostSeg := tombstoneSegNum(oh[:])
	if err := st.deleteTombstones(ghostSeg); err != nil {
		t.Fatalf("delete tombstones %d: %v", ghostSeg, err)
	}

	// The value must still resolve: its index row must have survived.
	r, err := st.Lookup(oh, 0)
	if err != nil {
		t.Fatalf("live value lost after deleteTombstones swept its key: %v", err)
	}
	defer r.Close()

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read surviving value: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Error("surviving value corrupted")
	}
}
