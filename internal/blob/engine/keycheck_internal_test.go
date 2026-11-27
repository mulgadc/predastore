package engine

import (
	"bytes"
	"crypto/cipher"
	"os"
	"path/filepath"
	"testing"

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

func openStoreIn(t *testing.T, dir string, aead cipher.AEAD) *Store {
	t.Helper()
	st, err := Open(dir, WithFreeSpaceWatermark(0, 0), WithAEAD(aead))
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
func tryRead(st *Store, oh [32]byte) bool {
	_, err := readValueErr(st, oh, 0)
	return err == nil
}

// storeFromAnEarlierRelease writes one value and removes the key check, as a
// store that an earlier release created holds no key check.
func storeFromAnEarlierRelease(t *testing.T, oh [32]byte) string {
	t.Helper()
	dir := t.TempDir()
	st := openStoreIn(t, dir, testAEAD(t))
	putValue(t, st, oh, 0, bytes.Repeat([]byte("old"), 2*KiB))
	closeStore(t, st)
	if err := os.Remove(filepath.Join(dir, keyCheckFilename)); err != nil {
		t.Fatalf("remove key check: %v", err)
	}
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

// Under a wrong key, nothing this release writes can verify the key, in this
// start or a later one.
func TestAnEarlierStoreUnderAWrongKeyNeverVerifies(t *testing.T) {
	old := [32]byte{0x71}
	dir := storeFromAnEarlierRelease(t, old)

	st := openStoreIn(t, dir, wrongAEAD(t))
	if st.KeyVerified() {
		t.Fatal("an earlier store has no evidence for any key at its first start")
	}
	if tryRead(st, old) {
		t.Fatal("the old value must not open under the wrong key")
	}
	fresh := [32]byte{0x72}
	putValue(t, st, fresh, 0, bytes.Repeat([]byte("new"), 2*KiB))
	if !tryRead(st, fresh) || st.KeyVerified() {
		t.Fatal("a value this release sealed must not verify the key")
	}
	closeStore(t, st)

	st = openStoreIn(t, dir, wrongAEAD(t))
	if !tryRead(st, fresh) || st.KeyVerified() {
		t.Fatal("a later start must keep the first high-water, so the new value still proves nothing")
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
	if !tryRead(st, old) || !st.KeyVerified() {
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
