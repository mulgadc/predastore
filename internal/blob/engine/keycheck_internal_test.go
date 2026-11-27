package engine

import (
	"bytes"
	"crypto/cipher"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
)

func wrongAEAD(t *testing.T) cipher.AEAD {
	t.Helper()
	aead, err := masterkey.NewAEAD(bytes.Repeat([]byte{0x43}, 32))
	if err != nil {
		t.Fatalf("aead: %v", err)
	}
	return aead
}

func openStoreIn(t *testing.T, dir string, aead cipher.AEAD, opts ...Option) *Store {
	t.Helper()
	st, err := Open(dir, append([]Option{WithFreeSpaceWatermark(0, 0), WithAEAD(aead)}, opts...)...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func closeStore(t *testing.T, st *Store) {
	t.Helper()
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// tryRead reads a whole value and reports whether every fragment opened.
func tryRead(st *Store, oh [32]byte, idx uint32) bool {
	_, err := readValueErr(st, oh, idx)
	return err == nil
}

// becomesVerified waits for the seal that a read starts in the background.
func becomesVerified(st *Store) bool {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if st.KeyVerified() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

// removeKeyCheck makes dir look like a store an earlier release created.
func removeKeyCheck(t *testing.T, dir string) {
	t.Helper()
	if err := os.Remove(filepath.Join(dir, keyCheckFilename)); err != nil {
		t.Fatalf("remove key check: %v", err)
	}
}

// storeFromAnEarlierRelease writes one value and removes the key check.
func storeFromAnEarlierRelease(t *testing.T, oh [32]byte) string {
	t.Helper()
	dir := t.TempDir()
	st := openStoreIn(t, dir, testAEAD(t))
	putValue(t, st, oh, 0, bytes.Repeat([]byte("old"), 2*KiB))
	closeStore(t, st)
	removeKeyCheck(t, dir)
	return dir
}

func TestANewStoreVerifiesItsKeyAtOpen(t *testing.T) {
	dir := t.TempDir()
	st := openStoreIn(t, dir, testAEAD(t))
	if !st.KeyVerified() {
		t.Fatal("a new store holds only fragments its own key seals, so its key is verified")
	}
	closeStore(t, st)

	st = openStoreIn(t, dir, wrongAEAD(t))
	if st.KeyVerified() {
		t.Fatal("another key must not open the key check")
	}
	closeStore(t, st)
	if st := openStoreIn(t, dir, testAEAD(t)); !st.KeyVerified() {
		t.Fatal("the store's own key must open the key check")
	}
}

// A sealed check copied from another store does not open, because every
// fragment of this store depends on its own identity.
func TestAKeyCheckFromAnotherStoreDoesNotVerify(t *testing.T) {
	from, to := t.TempDir(), t.TempDir()
	closeStore(t, openStoreIn(t, from, testAEAD(t)))
	closeStore(t, openStoreIn(t, to, testAEAD(t)))
	raw, err := os.ReadFile(filepath.Join(from, keyCheckFilename))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(to, keyCheckFilename), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if st := openStoreIn(t, to, testAEAD(t)); st.KeyVerified() {
		t.Fatal("a key check sealed for another store identity must not verify this one")
	}
}

// Under a wrong key, nothing this release writes can verify the key, in this
// start or a later one.
func TestAnEarlierStoreUnderAWrongKeyNeverVerifies(t *testing.T) {
	old := [32]byte{0x71}
	dir := storeFromAnEarlierRelease(t, old)

	st := openStoreIn(t, dir, wrongAEAD(t))
	if st.KeyVerified() || tryRead(st, old, 0) {
		t.Fatal("an earlier store has no evidence for a wrong key, and its old value must not open")
	}
	fresh := [32]byte{0x72}
	putValue(t, st, fresh, 0, bytes.Repeat([]byte("new"), 2*KiB))
	if !tryRead(st, fresh, 0) || becomesVerified(st) {
		t.Fatal("a value this release sealed must not verify the key")
	}
	closeStore(t, st)

	st = openStoreIn(t, dir, wrongAEAD(t))
	if !tryRead(st, fresh, 0) || becomesVerified(st) {
		t.Fatal("a later start keeps the first boundary, so the new value still proves nothing")
	}
}

// Under the right key, a value from before the first start verifies the key,
// and the next start finds the sealed check.
func TestAnEarlierStoreVerifiesOnAReadOfItsOldData(t *testing.T) {
	old := [32]byte{0x73}
	dir := storeFromAnEarlierRelease(t, old)

	st := openStoreIn(t, dir, testAEAD(t))
	if st.KeyVerified() {
		t.Fatal("no fragment has opened yet")
	}
	if !tryRead(st, old, 0) || !becomesVerified(st) {
		t.Fatal("a value an earlier release sealed must verify the key")
	}
	closeStore(t, st)

	st = openStoreIn(t, dir, testAEAD(t))
	if !st.KeyVerified() {
		t.Fatal("the sealed key check must verify the key at the next start")
	}
	closeStore(t, st)
	if st := openStoreIn(t, dir, wrongAEAD(t)); st.KeyVerified() {
		t.Fatal("the sealed key check must refuse another key")
	}
}

// Compaction moves an old value with its fragment numbers, so a read of it
// at its new position still verifies the key.
func TestARelocatedOldValueStillVerifiesTheKey(t *testing.T) {
	st, dir, oh := segment0LayoutStore(t)
	closeStore(t, st)
	removeKeyCheck(t, dir)

	st = openStoreIn(t, dir, testAEAD(t), WithMaxSegSize(40*KiB))
	if _, err := st.Delete(oh, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}
	before := extentOf(t, st, oh, 1)
	if err := st.compactOnce(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if after := extentOf(t, st, oh, 1); after.SegNum == before.SegNum && after.Off == before.Off {
		t.Fatal("compaction did not move the value, so this proves nothing")
	}
	if st.KeyVerified() {
		t.Fatal("compaction copies ciphertext without opening it, so it is no evidence")
	}
	if !tryRead(st, oh, 1) || !becomesVerified(st) {
		t.Fatal("the relocated value was sealed by an earlier start and must verify the key")
	}
}

// A damaged key check is kept for diagnosis, and the store still opens and
// serves; it only leaves repair off.
func TestADamagedKeyCheckLeavesTheStoreServing(t *testing.T) {
	oh := [32]byte{0x74}
	for name, raw := range map[string][]byte{
		"garbage":          []byte("not a key check"),
		"short":            []byte("PDKC"),
		"pending past end": append([]byte("PDKC\x01p"), bytes.Repeat([]byte{0xff}, 12)...),
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			st := openStoreIn(t, dir, testAEAD(t))
			putValue(t, st, oh, 0, bytes.Repeat([]byte("v"), KiB))
			closeStore(t, st)
			path := filepath.Join(dir, keyCheckFilename)
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}

			st = openStoreIn(t, dir, testAEAD(t))
			if !tryRead(st, oh, 0) {
				t.Fatal("the store must serve its data")
			}
			if becomesVerified(st) {
				t.Fatal("unreadable evidence must leave the key unverified")
			}
			kept, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(kept, raw) {
				t.Fatalf("the damaged key check must be kept as it was: %v", err)
			}
		})
	}
}

// A seal that keeps failing is attempted once for reads of many fragments,
// not once for each fragment.
func TestAFailingSealIsAttemptedOncePerRetryWindow(t *testing.T) {
	old := [32]byte{0x75}
	dir := t.TempDir()
	st := openStoreIn(t, dir, testAEAD(t))
	putValue(t, st, old, 0, bytes.Repeat([]byte("m"), 64*KiB))
	closeStore(t, st)
	removeKeyCheck(t, dir)

	st = openStoreIn(t, dir, testAEAD(t))
	var attempts atomic.Int64
	st.keyCheck.write = func([]byte) error {
		attempts.Add(1)
		return errors.New("injected key check write failure")
	}
	if !tryRead(st, old, 0) || !tryRead(st, old, 0) {
		t.Fatal("the value must read")
	}
	if becomesVerified(st) {
		t.Fatal("a seal that failed must not verify the key")
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("%d seal attempts for two reads of eight fragments, want 1", got)
	}
}
