package engine

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func readValueAt(t *testing.T, st *Store, oh [32]byte, idx uint32, epoch uint64) []byte {
	t.Helper()
	r, err := st.LookupAt(oh, idx, epoch)
	if err != nil {
		t.Fatalf("lookup (%x,%d)@%d: %v", oh[0], idx, epoch, err)
	}
	defer r.Close()
	body, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return body
}

// An overwrite landing between a relocation's copy and its swap demotes the
// row being moved into the retained namespace, still naming the source. The
// generation a placement record names must stay readable after the pass.
func TestCompactionKeepsAGenerationAnOverwriteDemotedMidRelocation(t *testing.T) {
	st, _, oh := segment0LayoutStore(t)
	oldBody := readValue(t, st, oh, 1)
	oldEpoch := liveEpochOf(t, st, oh, 1)
	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}

	newBody := bytes.Repeat([]byte{0xee}, 12*KiB)
	fired := false
	st.beforeRelocateSwap = func() {
		if fired {
			return
		}
		fired = true
		putValue(t, st, oh, 1, newBody)
	}
	if err := st.compactOnce(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	st.beforeRelocateSwap = nil
	if !fired {
		t.Fatal("the overwrite never ran between copy and swap")
	}

	if got := readValueAt(t, st, oh, 1, oldEpoch); !bytes.Equal(got, oldBody) {
		t.Fatal("the demoted generation no longer reads back")
	}
	if got := readValue(t, st, oh, 1); !bytes.Equal(got, newBody) {
		t.Fatal("the overwrite's generation did not survive")
	}
}

// A commit landing in the same window moves a prepared row being relocated to
// the live namespace, still naming the source; the published value must read.
func TestCompactionKeepsAPreparedRowCommittedMidRelocation(t *testing.T) {
	st, _ := openTestStore(t, WithMaxSegSize(40*KiB))
	oh := [32]byte{0x43}
	body := bytes.Repeat([]byte{0xcd}, 12*KiB)
	putValue(t, st, oh, 0, body)
	prepared := prepareValue(t, st, oh, 1, body)
	putValue(t, st, oh, 2, body)
	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}

	fired := false
	st.beforeRelocateSwap = func() {
		if fired {
			return
		}
		fired = true
		if _, err := st.Commit(oh, 1, prepared); err != nil {
			t.Errorf("commit: %v", err)
		}
	}
	if err := st.compactOnce(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	st.beforeRelocateSwap = nil
	if !fired {
		t.Fatal("the commit never ran between copy and swap")
	}
	if got := readValue(t, st, oh, 1); !bytes.Equal(got, body) {
		t.Fatal("the committed value no longer reads back")
	}
}

// A pass that relocates everything and then fails to sync the index leaves the
// source for the next pass, which moves nothing. That pass must still sync
// before it unlinks, or a power loss could revert the repoints to a dropped file.
func TestCompactionSyncsTheIndexBeforeAZeroMoveDrop(t *testing.T) {
	st, dir, oh := segment0LayoutStore(t)
	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}
	seg0 := filepath.Join(dir, fmt.Sprintf(segFilename, 0))

	errSync := errors.New("injected index sync failure")
	st.syncIndexHook = func() error { return errSync }
	if err := st.compactOnce(); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	if _, err := os.Stat(seg0); err != nil {
		t.Fatalf("segment 0 was dropped after its index sync failed: %v", err)
	}

	syncs := 0
	st.syncIndexHook = func() error {
		if _, err := os.Stat(seg0); err != nil {
			t.Errorf("index synced only after segment 0 was unlinked")
		}
		syncs++
		return st.index.Sync()
	}
	if err := st.compactOnce(); err != nil {
		t.Fatalf("second pass: %v", err)
	}
	st.syncIndexHook = nil
	if _, err := os.Stat(seg0); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("segment 0 was not dropped on the retry: %v", err)
	}
	if syncs == 0 {
		t.Fatal("the retry unlinked segment 0 without syncing the index")
	}
}
