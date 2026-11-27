package engine

import (
	"bytes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// keyCheckFilename holds the evidence that the store's master key is the key
// that sealed its data. A node under another key fails every fragment, and
// only this evidence tells that apart from corruption.
const keyCheckFilename = "keycheck"

const (
	keyCheckMagic   = "PDKC"
	keyCheckVersion = 1

	keyCheckSealed  = 's'
	keyCheckPending = 'p'
)

var (
	keyCheckPlaintext = []byte("predastore key check")
	keyCheckAAD       = []byte("predastore keycheck v1")
)

// keyCheck tracks whether the store's key is verified. While it is pending,
// a fragment that authenticates below the high-water was sealed by an earlier
// release, which proves the key, and the first one seals the file.
type keyCheck struct {
	dir  string
	aead cipher.AEAD

	verified atomic.Bool

	// The high-water from the first start on this release, or zero when the
	// check is not pending. Fragments this release writes lie at or above it.
	hwSeg, hwOff uint64
	pending      bool

	sealMu sync.Mutex
}

// openKeyCheck reads or creates the key check. hasData reports whether any
// segment held data at open, and segNum and segSize name the append position.
func openKeyCheck(dir string, aead cipher.AEAD, hasData bool, segNum uint64, segSize int64) (*keyCheck, error) {
	kc := &keyCheck{dir: dir, aead: aead}
	raw, err := os.ReadFile(filepath.Join(dir, keyCheckFilename))
	switch {
	case errors.Is(err, os.ErrNotExist) && !hasData:
		if err := kc.writeSealed(); err != nil {
			return nil, err
		}
		kc.verified.Store(true)
		return kc, nil
	case errors.Is(err, os.ErrNotExist):
		kc.hwSeg, kc.hwOff, kc.pending = segNum, uint64(max(segSize, 0)), true //nolint:gosec // G115: clamped non-negative.
		if err := kc.write(kc.pendingBytes()); err != nil {
			return nil, err
		}
		return kc, nil
	case err != nil:
		return nil, fmt.Errorf("read key check: %w", err)
	}

	if len(raw) < len(keyCheckMagic)+2 || string(raw[:len(keyCheckMagic)]) != keyCheckMagic || raw[len(keyCheckMagic)] != keyCheckVersion {
		return nil, fmt.Errorf("key check %s does not decode", filepath.Join(dir, keyCheckFilename))
	}
	body := raw[len(keyCheckMagic)+2:]
	switch raw[len(keyCheckMagic)+1] {
	case keyCheckSealed:
		nonceSize := aead.NonceSize()
		if len(body) < nonceSize {
			return nil, fmt.Errorf("key check %s is short", filepath.Join(dir, keyCheckFilename))
		}
		plain, err := aead.Open(nil, body[:nonceSize], body[nonceSize:], keyCheckAAD)
		if err == nil && bytes.Equal(plain, keyCheckPlaintext) {
			kc.verified.Store(true)
			return kc, nil
		}
		slog.Error("the master key does not open this store's key check; integrity failures are not marked for repair",
			"dir", dir)
		return kc, nil
	case keyCheckPending:
		if len(body) != 16 {
			return nil, fmt.Errorf("key check %s is short", filepath.Join(dir, keyCheckFilename))
		}
		kc.hwSeg, kc.hwOff, kc.pending = binary.BigEndian.Uint64(body[:8]), binary.BigEndian.Uint64(body[8:]), true
		return kc, nil
	default:
		return nil, fmt.Errorf("key check %s has an unknown form", filepath.Join(dir, keyCheckFilename))
	}
}

// below reports whether a fragment ending at end in segment seg lies wholly
// below the high-water.
func (kc *keyCheck) below(seg uint64, end int64) bool {
	return seg < kc.hwSeg || (seg == kc.hwSeg && end >= 0 && uint64(end) <= kc.hwOff)
}

// authenticated records a fragment that opened under the current key. One
// below the high-water proves the key and seals the file.
func (kc *keyCheck) authenticated(seg uint64, end int64) {
	if !kc.pending || kc.verified.Load() || !kc.below(seg, end) {
		return
	}
	kc.sealMu.Lock()
	defer kc.sealMu.Unlock()
	if kc.verified.Load() {
		return
	}
	if err := kc.writeSealed(); err != nil {
		slog.Warn("could not seal the key check; trying again on a later read", "dir", kc.dir, "error", err)
		return
	}
	kc.verified.Store(true)
}

func (kc *keyCheck) pendingBytes() []byte {
	out := append([]byte(keyCheckMagic), keyCheckVersion, keyCheckPending)
	out = binary.BigEndian.AppendUint64(out, kc.hwSeg)
	return binary.BigEndian.AppendUint64(out, kc.hwOff)
}

func (kc *keyCheck) writeSealed() error {
	nonce := make([]byte, kc.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return fmt.Errorf("key check nonce: %w", err)
	}
	out := append([]byte(keyCheckMagic), keyCheckVersion, keyCheckSealed)
	out = append(out, nonce...)
	return kc.write(kc.aead.Seal(out, nonce, keyCheckPlaintext, keyCheckAAD))
}

// write replaces the file through a synced temporary file and a synced rename.
func (kc *keyCheck) write(data []byte) error {
	path := filepath.Join(kc.dir, keyCheckFilename)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("write key check: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("write key check: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync key check: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close key check: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("install key check: %w", err)
	}
	return syncDirectory(kc.dir)
}

// KeyVerified reports whether the store has evidence that its master key is
// the key that sealed its data. Without it, a failed read may be a wrong key.
func (store *Store) KeyVerified() bool {
	return store.keyCheck != nil && store.keyCheck.verified.Load()
}

// storeHasData reports whether any segment in dir holds more than a header,
// and the size of segment num.
func storeHasData(dir string, num uint64) (hasData bool, numSize int64, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, 0, err
	}
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".seg" {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return false, 0, err
		}
		if info.Size() > segHeaderSize {
			hasData = true
		}
		if e.Name() == fmt.Sprintf(segFilename, num) {
			numSize = info.Size()
		}
	}
	return hasData, numSize, nil
}
