package engine

import (
	"bytes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
)

func testAEAD(t *testing.T) cipher.AEAD {
	t.Helper()
	aead, err := masterkey.NewAEAD(bytes.Repeat([]byte{0x42}, 32))
	if err != nil {
		t.Fatalf("aead: %v", err)
	}
	return aead
}

func openTestStore(t *testing.T, opts ...Option) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	// Default the watermark off (0, 0 never crosses either threshold) so
	// ordinary tests don't flake on ambient disk pressure; callers that want
	// to exercise it pass their own WithFreeSpaceWatermark, applied after.
	base := append([]Option{WithFreeSpaceWatermark(0, 0)}, opts...)
	st, err := Open(dir, append(base, WithAEAD(testAEAD(t)))...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st, dir
}

// putValue writes and publishes a value. Every epoch here is distinct, which
// is what a real overwrite does; a test that needs a specific one calls
// prepareValue and Commit itself.
func putValue(t *testing.T, st *Store, oh [32]byte, idx uint32, body []byte) {
	t.Helper()
	epoch := prepareValue(t, st, oh, idx, body)
	if _, err := st.Commit(oh, idx, epoch); err != nil {
		t.Fatalf("commit (%x,%d): %v", oh[0], idx, err)
	}
}

// prepareValue writes a value without publishing it and returns its epoch.
func prepareValue(t *testing.T, st *Store, oh [32]byte, idx uint32, body []byte) uint64 {
	t.Helper()
	epoch := nextTestEpoch()
	w, err := st.Append(oh, idx, int64(len(body)), epoch)
	if err != nil {
		t.Fatalf("append (%x,%d): %v", oh[0], idx, err)
	}
	if _, err := w.Write(body); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	return epoch
}

// testEpochs hands out distinct non-zero epochs. Zero is reserved as invalid,
// so a counter starting at one is enough.
var testEpochs atomic.Uint64

func nextTestEpoch() uint64 { return testEpochs.Add(1) }

func readValue(t *testing.T, st *Store, oh [32]byte, idx uint32) []byte {
	t.Helper()
	r, err := st.Lookup(oh, idx)
	if err != nil {
		t.Fatalf("lookup (%x,%d): %v", oh[0], idx, err)
	}
	defer r.Close()
	body, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return body
}

func extentOf(t *testing.T, st *Store, oh [32]byte, idx uint32) extent {
	t.Helper()
	raw, err := st.indexGet(MakeKey(oh, idx))
	if err != nil {
		t.Fatalf("index get: %v", err)
	}
	ext, _, err := decodeIndexValue(raw)
	if err != nil {
		t.Fatalf("decode extent: %v", err)
	}
	return ext
}

// fragNumsAt reads the fragNum header field of every fragment in an extent
// straight off disk, bypassing the store so the test sees raw on-disk bytes.
func fragNumsAt(t *testing.T, dir string, ext extent) []uint64 {
	t.Helper()
	f, err := os.Open(filepath.Join(dir, fmt.Sprintf(segFilename, ext.SegNum)))
	if err != nil {
		t.Fatalf("open seg: %v", err)
	}
	defer f.Close()

	count := ext.PSize / totalFragSize
	nums := make([]uint64, 0, count)
	hdr := make([]byte, fragHeaderSize)
	for i := range count {
		if _, err := f.ReadAt(hdr, ext.Off+i*totalFragSize); err != nil {
			t.Fatalf("read header: %v", err)
		}
		nums = append(nums, binary.BigEndian.Uint64(hdr[0:8]))
	}
	return nums
}

// Three ~12 KiB values under a 40 KiB cap pack two into segment 0 and roll the
// third into segment 1; deleting one of segment 0's values drops its live
// fraction below threshold, making it the sole compaction candidate.
func segment0LayoutStore(t *testing.T) (st *Store, dir string, oh [32]byte) {
	st, dir = openTestStore(t, WithMaxSegSize(40*KiB))
	oh = [32]byte{0x42}
	body := bytes.Repeat([]byte{0xcd}, 12*KiB)
	putValue(t, st, oh, 0, body)
	putValue(t, st, oh, 1, body)
	putValue(t, st, oh, 2, body)
	return st, dir, oh
}

func TestCompactionPreservesFragNum(t *testing.T) {
	st, dir, oh := segment0LayoutStore(t)

	before := extentOf(t, st, oh, 1)
	if before.SegNum != 0 {
		t.Fatalf("expected value 1 in segment 0, got %d", before.SegNum)
	}
	wantFragNums := fragNumsAt(t, dir, before)
	wantBody := readValue(t, st, oh, 1)

	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := st.compactOnce(); err != nil {
		t.Fatalf("compact: %v", err)
	}

	after := extentOf(t, st, oh, 1)
	if after.SegNum == before.SegNum && after.Off == before.Off {
		t.Fatalf("value 1 was not relocated; still at seg %d off %d", after.SegNum, after.Off)
	}

	gotFragNums := fragNumsAt(t, dir, after)
	if len(gotFragNums) != len(wantFragNums) {
		t.Fatalf("fragNum count changed: got %d want %d", len(gotFragNums), len(wantFragNums))
	}
	for i := range wantFragNums {
		if gotFragNums[i] != wantFragNums[i] {
			t.Fatalf("fragNum[%d] changed: got %d want %d", i, gotFragNums[i], wantFragNums[i])
		}
	}

	if got := readValue(t, st, oh, 1); !bytes.Equal(got, wantBody) {
		t.Fatalf("body changed after relocation")
	}
}

func TestCompactionFragNumsGloballyUnique(t *testing.T) {
	st, dir, oh := segment0LayoutStore(t)
	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := st.compactOnce(); err != nil {
		t.Fatalf("compact: %v", err)
	}

	seen := map[uint64]bool{}
	for _, idx := range []uint32{1, 2} {
		for _, n := range fragNumsAt(t, dir, extentOf(t, st, oh, idx)) {
			if seen[n] {
				t.Fatalf("duplicate fragNum %d after compaction", n)
			}
			seen[n] = true
		}
	}
}

func TestCandidateSelectionAllDeadSegmentIsCandidate(t *testing.T) {
	st, _, oh := segment0LayoutStore(t)
	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}

	cands, failed, err := st.candidateSegments()
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if failed != 0 {
		t.Fatalf("candidate scan reported %d failures, want 0", failed)
	}
	if len(cands) != 1 || cands[0] != 0 {
		t.Fatalf("expected segment 0 as sole candidate, got %v", cands)
	}
}

func TestCandidateSelectionAllLiveNoCandidates(t *testing.T) {
	st, _, _ := segment0LayoutStore(t)
	cands, failed, err := st.candidateSegments()
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if failed != 0 {
		t.Fatalf("candidate scan reported %d failures, want 0", failed)
	}
	if len(cands) != 0 {
		t.Fatalf("no deletes, expected no candidates, got %v", cands)
	}
}

func TestCandidateSelectionNeverSelectsActiveSegment(t *testing.T) {
	st, _ := openTestStore(t, WithMaxSegSize(64*KiB))
	oh := [32]byte{0x7}
	putValue(t, st, oh, 0, bytes.Repeat([]byte{0x1}, 4*KiB))
	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}

	cands, failed, err := st.candidateSegments()
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if failed != 0 {
		t.Fatalf("candidate scan reported %d failures, want 0", failed)
	}
	for _, c := range cands {
		if c == st.segNum {
			t.Fatalf("active segment %d selected as candidate", st.segNum)
		}
	}
}

// tombstones reads the whole slot-keyed tombstone namespace, mapping each
// dead slot to the byte count it records.
func tombstones(t *testing.T, st *Store) map[slot]int64 {
	t.Helper()
	found := map[slot]int64{}
	err := st.indexScan([]byte{tombstonePrefix}, func(k, v []byte) error {
		// Keys whose hash starts with tombstonePrefix share this scan, so
		// width-check before decoding a slot out of one.
		if len(k) != tombstoneKeySize {
			return nil
		}
		s := slot{segNum: tombstoneSegNum(k), off: int64(binary.BigEndian.Uint64(k[9:17]))}
		found[s] = int64(binary.BigEndian.Uint64(v))
		return nil
	})
	if err != nil {
		t.Fatalf("scan tombstones: %v", err)
	}
	return found
}

type slot struct {
	segNum uint64
	off    int64
}

func slotOf(ext extent) slot { return slot{segNum: ext.SegNum, off: ext.Off} }

// An overwrite supersedes the key's previous extent. Those bytes are retained
// rather than tombstoned, so a placement record still naming that generation
// can be served, and they become dead when the sweep releases them. The live
// slot must never be tombstoned at either point.
func TestOverwriteRetainsThenReclaimsSupersededExtent(t *testing.T) {
	st, _ := openTestStore(t, WithMaxSegSize(1*MiB))
	oh := [32]byte{0x11}

	putValue(t, st, oh, 0, bytes.Repeat([]byte{0xaa}, 12*KiB))
	old := extentOf(t, st, oh, 0)

	putValue(t, st, oh, 0, bytes.Repeat([]byte{0xbb}, 12*KiB))
	cur := extentOf(t, st, oh, 0)

	if slotOf(cur) == slotOf(old) {
		t.Fatalf("overwrite reused slot %v; test cannot distinguish live from dead", slotOf(old))
	}

	// Still answerable, so not yet dead. Marking it dead here is what would
	// let compaction reclaim bytes a record still points at.
	if _, ok := tombstones(t, st)[slotOf(old)]; ok {
		t.Fatalf("commit tombstoned superseded slot %v while it is still retained", slotOf(old))
	}

	released, err := st.sweepRetained(-time.Second, 0)
	if err != nil {
		t.Fatalf("sweep retained: %v", err)
	}
	if released != 1 {
		t.Fatalf("sweep released %d retained generations, want 1", released)
	}

	found := tombstones(t, st)
	if got, ok := found[slotOf(old)]; !ok {
		t.Fatalf("released slot %v has no tombstone; it is invisible to compaction", slotOf(old))
	} else if got != old.PSize {
		t.Fatalf("tombstone for %v records %d bytes, want PSize %d", slotOf(old), got, old.PSize)
	}
	if _, ok := found[slotOf(cur)]; ok {
		t.Fatalf("live slot %v was tombstoned", slotOf(cur))
	}
}

// A first write supersedes nothing, so it must leave the tombstone namespace
// untouched — a spurious tombstone would understate a segment's live bytes.
func TestFirstWriteLeavesNoTombstone(t *testing.T) {
	st, _ := openTestStore(t, WithMaxSegSize(1*MiB))
	oh := [32]byte{0x22}

	putValue(t, st, oh, 0, bytes.Repeat([]byte{0xcc}, 12*KiB))

	if found := tombstones(t, st); len(found) != 0 {
		t.Fatalf("first write produced %d tombstone(s): %v", len(found), found)
	}
}

// Append only reserves space; the old extent stays live and readable until the
// writer commits. The tombstone must land at commit and not one moment sooner,
// or an abandoned write would permanently mark still-live bytes dead.
func TestTombstoneLandsAtCommitNotAppend(t *testing.T) {
	st, _ := openTestStore(t, WithMaxSegSize(1*MiB))
	oh := [32]byte{0x33}
	body := bytes.Repeat([]byte{0xdd}, 12*KiB)

	putValue(t, st, oh, 0, body)
	old := extentOf(t, st, oh, 0)

	// Prepare an overwrite. Closing the writer makes it durable but does not
	// publish it, so nothing about the readable value may change yet.
	epoch := prepareValue(t, st, oh, 0, bytes.Repeat([]byte{0xee}, 12*KiB))

	if found := tombstones(t, st); len(found) != 0 {
		t.Fatalf("prepared append produced %d tombstone(s): %v", len(found), found)
	}
	if got := extentOf(t, st, oh, 0); slotOf(got) != slotOf(old) {
		t.Fatalf("prepared append moved the index off %v to %v", slotOf(old), slotOf(got))
	}
	if got := readValue(t, st, oh, 0); !bytes.Equal(got, body) {
		t.Fatalf("prepared append changed the readable body")
	}

	// Committing is what supersedes the old extent. It is retained, not dead,
	// and the sweep is what makes it dead.
	if _, err := st.Commit(oh, 0, epoch); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, ok := tombstones(t, st)[slotOf(old)]; ok {
		t.Fatalf("commit tombstoned superseded slot %v instead of retaining it", slotOf(old))
	}
	if _, err := st.sweepRetained(-time.Second, 0); err != nil {
		t.Fatalf("sweep retained: %v", err)
	}
	if _, ok := tombstones(t, st)[slotOf(old)]; !ok {
		t.Fatalf("the sweep did not tombstone released slot %v", slotOf(old))
	}
}

// The reclaim path this bug is about: a segment emptied purely by overwrite
// churn (no Delete) must become a candidate and drain. Before the fix its dead
// bytes carried no tombstone, so it read as fully live and was never selected.
func TestOverwriteChurnDrainsSegment(t *testing.T) {
	st, dir, oh := segment0LayoutStore(t)
	body := bytes.Repeat([]byte{0x99}, 12*KiB)

	// Values 0 and 1 fill segment 0; rewriting both relocates them and leaves
	// segment 0 entirely dead.
	putValue(t, st, oh, 0, body)
	putValue(t, st, oh, 1, body)

	// Overwriting retains the previous generation, so the space is not dead
	// until it is released. Draining is a property of the reclaim path, not of
	// how long the store holds a generation answerable.
	if _, err := st.sweepRetained(-time.Second, 0); err != nil {
		t.Fatalf("sweep retained: %v", err)
	}

	cands, failed, err := st.candidateSegments()
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if failed != 0 {
		t.Fatalf("candidate scan reported %d failures, want 0", failed)
	}
	if len(cands) != 1 || cands[0] != 0 {
		t.Fatalf("expected overwrite-dead segment 0 as sole candidate, got %v", cands)
	}

	if err := st.compactOnce(); err != nil {
		t.Fatalf("compact: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, fmt.Sprintf(segFilename, uint64(0)))); !os.IsNotExist(err) {
		t.Fatalf("drained segment 0 still on disk: %v", err)
	}
	for _, idx := range []uint32{0, 1} {
		if got := readValue(t, st, oh, idx); !bytes.Equal(got, body) {
			t.Fatalf("value %d body wrong after overwrite churn + compaction", idx)
		}
	}
}

// Every index-committed extent must already be enumerable from its segment's
// .idx, or a drop could lose a live extent. Assert the back-check slot for each
// live value appears in scanIdx of its segment.
func TestIdxCoversEveryCommittedExtent(t *testing.T) {
	st, dir := openTestStore(t, WithMaxSegSize(40*KiB))
	oh := [32]byte{0x55}
	bodies := map[uint32][]byte{
		0: bytes.Repeat([]byte{0xa}, 1),
		1: bytes.Repeat([]byte{0xb}, 12*KiB),
		2: bytes.Repeat([]byte{0xc}, 20*KiB),
		3: bytes.Repeat([]byte{0xd}, 5*KiB),
	}
	for idx, body := range bodies {
		putValue(t, st, oh, idx, body)
	}

	for idx := range bodies {
		ext := extentOf(t, st, oh, idx)
		entries, err := scanIdx(dir, ext.SegNum)
		if err != nil {
			t.Fatalf("scan idx %d: %v", ext.SegNum, err)
		}
		wantKey := [36]byte(MakeKey(oh, idx))
		found := false
		for _, e := range entries {
			if e.Off == ext.Off && e.Key == wantKey {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("committed value %d (seg %d off %d) missing from .idx", idx, ext.SegNum, ext.Off)
		}
	}
}
