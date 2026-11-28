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

func keyCheckExists(t *testing.T, dir string) bool {
	t.Helper()
	_, err := os.Stat(filepath.Join(dir, keyCheckFilename))
	return err == nil
}

func TestANewStoreVerifiesItsKeyAtOpen(t *testing.T) {
	dir := t.TempDir()
	st := openStoreIn(t, dir, testAEAD(t))
	if !st.KeyVerified() {
		t.Fatal("a new store holds only fragments its own key seals, so its key is verified")
	}
	putValue(t, st, [32]byte{0x70}, 0, bytes.Repeat([]byte("n"), KiB))
	closeStore(t, st)

	st = openStoreIn(t, dir, wrongAEAD(t))
	if st.KeyVerified() {
		t.Fatal("another key must not open the key check")
	}
	closeStore(t, st)
	if st := openStoreIn(t, dir, testAEAD(t)); !st.KeyVerified() {
		t.Fatal("the store's own key must open the key check after a restart")
	}
}

// A store created before the key check existed is never verified: it gets no
// key check, whatever it reads or writes, in this start or a later one.
func TestAStoreFromAnEarlierReleaseIsNeverVerified(t *testing.T) {
	old := [32]byte{0x71}
	dir := t.TempDir()
	st := openStoreIn(t, dir, testAEAD(t))
	putValue(t, st, old, 0, bytes.Repeat([]byte("old"), 2*KiB))
	closeStore(t, st)
	if err := os.Remove(filepath.Join(dir, keyCheckFilename)); err != nil {
		t.Fatal(err)
	}

	for start := range 2 {
		st = openStoreIn(t, dir, testAEAD(t))
		fresh := [32]byte{0x72, byte(start)}
		putValue(t, st, fresh, 0, bytes.Repeat([]byte("new"), 2*KiB))
		if !tryRead(st, old) || !tryRead(st, fresh) {
			t.Fatal("the store must serve its data")
		}
		if st.KeyVerified() || keyCheckExists(t, dir) {
			t.Fatalf("start %d: a store from an earlier release must not be verified or get a key check", start)
		}
		closeStore(t, st)
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

// A damaged key check is kept for diagnosis, and the store still opens and
// serves; it only leaves repair off.
func TestADamagedKeyCheckLeavesTheStoreServing(t *testing.T) {
	oh := [32]byte{0x74}
	for name, raw := range map[string][]byte{
		"garbage":      []byte("not a key check"),
		"short":        []byte("PDKC"),
		"unknown form": append([]byte("PDKC\x01p"), bytes.Repeat([]byte{0xff}, 40)...),
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
			if !tryRead(st, oh) {
				t.Fatal("the store must serve its data")
			}
			if st.KeyVerified() {
				t.Fatal("unreadable evidence must leave the key unverified")
			}
			kept, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(kept, raw) {
				t.Fatalf("the damaged key check must be kept as it was: %v", err)
			}
		})
	}
}
