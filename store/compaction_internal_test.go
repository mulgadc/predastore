package store

import (
	"bytes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/mulgadc/predastore/pkg/masterkey"
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
	st, err := Open(dir, append(opts, WithAEAD(testAEAD(t)))...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st, dir
}

func putShard(t *testing.T, st *Store, oh [32]byte, idx uint32, body []byte) {
	t.Helper()
	w, err := st.Append(oh, idx, int64(len(body)))
	if err != nil {
		t.Fatalf("append (%x,%d): %v", oh[0], idx, err)
	}
	if _, err := w.Write(body); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
}

func readShard(t *testing.T, st *Store, oh [32]byte, idx uint32) []byte {
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
	raw, err := st.index.Get(MakeShardKey(oh, idx))
	if err != nil {
		t.Fatalf("index get: %v", err)
	}
	ext, err := decodeExtent(raw)
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

// Three ~12 KiB shards under a 40 KiB cap pack two into segment 0 and roll the
// third into segment 1; deleting one of segment 0's shards drops its live
// fraction below threshold, making it the sole compaction candidate.
func segment0LayoutStore(t *testing.T) (st *Store, dir string, oh [32]byte) {
	st, dir = openTestStore(t, WithMaxSegSize(40*KiB))
	oh = [32]byte{0x42}
	body := bytes.Repeat([]byte{0xcd}, 12*KiB)
	putShard(t, st, oh, 0, body)
	putShard(t, st, oh, 1, body)
	putShard(t, st, oh, 2, body)
	return st, dir, oh
}

func TestCompactionPreservesFragNum(t *testing.T) {
	st, dir, oh := segment0LayoutStore(t)

	before := extentOf(t, st, oh, 1)
	if before.SegNum != 0 {
		t.Fatalf("expected shard 1 in segment 0, got %d", before.SegNum)
	}
	wantFragNums := fragNumsAt(t, dir, before)
	wantBody := readShard(t, st, oh, 1)

	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := st.compactOnce(); err != nil {
		t.Fatalf("compact: %v", err)
	}

	after := extentOf(t, st, oh, 1)
	if after.SegNum == before.SegNum && after.Off == before.Off {
		t.Fatalf("shard 1 was not relocated; still at seg %d off %d", after.SegNum, after.Off)
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

	if got := readShard(t, st, oh, 1); !bytes.Equal(got, wantBody) {
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

	cands, err := st.candidateSegments()
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if len(cands) != 1 || cands[0] != 0 {
		t.Fatalf("expected segment 0 as sole candidate, got %v", cands)
	}
}

func TestCandidateSelectionAllLiveNoCandidates(t *testing.T) {
	st, _, _ := segment0LayoutStore(t)
	cands, err := st.candidateSegments()
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if len(cands) != 0 {
		t.Fatalf("no deletes, expected no candidates, got %v", cands)
	}
}

func TestCandidateSelectionNeverSelectsActiveSegment(t *testing.T) {
	st, _ := openTestStore(t, WithMaxSegSize(64*KiB))
	oh := [32]byte{0x7}
	putShard(t, st, oh, 0, bytes.Repeat([]byte{0x1}, 4*KiB))
	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}

	cands, err := st.candidateSegments()
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	for _, c := range cands {
		if c == st.segNum {
			t.Fatalf("active segment %d selected as candidate", st.segNum)
		}
	}
}

// Every index-committed extent must already be enumerable from its segment's
// .idx, or a drop could lose a live extent. Assert the back-check slot for each
// live shard appears in scanIdx of its segment.
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
		putShard(t, st, oh, idx, body)
	}

	for idx := range bodies {
		ext := extentOf(t, st, oh, idx)
		entries, err := scanIdx(dir, ext.SegNum)
		if err != nil {
			t.Fatalf("scan idx %d: %v", ext.SegNum, err)
		}
		wantKey := [36]byte(MakeShardKey(oh, idx))
		found := false
		for _, e := range entries {
			if e.Off == ext.Off && e.Key == wantKey {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("committed shard %d (seg %d off %d) missing from .idx", idx, ext.SegNum, ext.Off)
		}
	}
}
