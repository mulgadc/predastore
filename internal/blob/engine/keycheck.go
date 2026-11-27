package engine

import (
	"bytes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
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

	keyCheckHeader     = len(keyCheckMagic) + 2
	keyCheckPendingLen = keyCheckHeader + 8 + 4
)

// keyCheckRetry spaces the attempts to seal the key check after one fails.
var keyCheckRetry = time.Minute

var keyCheckPlaintext = []byte("predastore key check")

// keyCheckAAD binds the sealed check to the store identity that every
// fragment's nonce carries, so a check copied from another store fails.
func keyCheckAAD(storeID uint32) []byte {
	return binary.BigEndian.AppendUint32([]byte("predastore keycheck v1"), storeID)
}

// keyCheck tracks whether the store's key is verified. While it is pending,
// a fragment numbered below the boundary was sealed before this release first
// started on the store, so one that authenticates proves the key.
type keyCheck struct {
	dir     string
	aead    cipher.AEAD
	storeID uint32

	verified atomic.Bool

	// boundary is the first fragment number this release could have sealed,
	// read from the pending file. Zero means the check is not pending.
	boundary uint64

	// sealing is set while a seal is in flight or waiting for its retry, so
	// reads only ever start one.
	sealing atomic.Bool
	write   func([]byte) error
}

// openKeyCheck reads or creates the key check. hasData reports whether any
// segment held data at open, and nextFrag is the first fragment number this
// start can seal. Evidence that cannot be read leaves the store unverified.
func openKeyCheck(dir string, aead cipher.AEAD, storeID uint32, hasData bool, nextFrag uint64) *keyCheck {
	kc := &keyCheck{dir: dir, aead: aead, storeID: storeID}
	kc.write = kc.writeFile
	unverified := func(reason string, err error) *keyCheck {
		slog.Error("the store's key check is unusable; integrity failures are not marked for repair",
			"dir", dir, "reason", reason, "error", err)
		return kc
	}

	raw, err := os.ReadFile(filepath.Join(dir, keyCheckFilename))
	switch {
	case errors.Is(err, os.ErrNotExist) && !hasData:
		if err := kc.writeSealed(); err != nil {
			return unverified("write", err)
		}
		kc.verified.Store(true)
		return kc
	case errors.Is(err, os.ErrNotExist):
		kc.boundary = nextFrag
		if err := kc.write(kc.pendingBytes()); err != nil {
			kc.boundary = 0
			return unverified("write", err)
		}
		return kc
	case err != nil:
		return unverified("read", err)
	}

	if len(raw) < keyCheckHeader || string(raw[:len(keyCheckMagic)]) != keyCheckMagic || raw[len(keyCheckMagic)] != keyCheckVersion {
		return unverified("does not decode", nil)
	}
	body := raw[keyCheckHeader:]
	switch raw[len(keyCheckMagic)+1] {
	case keyCheckSealed:
		nonceSize := aead.NonceSize()
		if len(body) < nonceSize {
			return unverified("short", nil)
		}
		plain, err := aead.Open(nil, body[:nonceSize], body[nonceSize:], keyCheckAAD(storeID))
		if err != nil || !bytes.Equal(plain, keyCheckPlaintext) {
			slog.Error("the master key does not open this store's key check; integrity failures are not marked for repair",
				"dir", dir)
			return kc
		}
		kc.verified.Store(true)
		return kc
	case keyCheckPending:
		if len(raw) != keyCheckPendingLen ||
			crc32.ChecksumIEEE(raw[:keyCheckPendingLen-4]) != binary.BigEndian.Uint32(raw[keyCheckPendingLen-4:]) {
			return unverified("pending record fails its checksum", nil)
		}
		boundary := binary.BigEndian.Uint64(body[:8])
		// Fragment numbers only grow, so a boundary past this start's is not
		// one this store wrote.
		if boundary == 0 || boundary > nextFrag {
			return unverified("pending boundary is past the store's fragment numbers", nil)
		}
		kc.boundary = boundary
		return kc
	default:
		return unverified("unknown form", nil)
	}
}

// pending reports whether a read can still supply the evidence.
func (kc *keyCheck) pending() bool { return kc.boundary != 0 && !kc.verified.Load() }

// authenticated records a fragment that opened under the current key. One
// numbered below the boundary proves the key; sealing the file runs off the
// read, one attempt at a time.
func (kc *keyCheck) authenticated(fragNum uint64) {
	if fragNum >= kc.boundary || !kc.pending() || !kc.sealing.CompareAndSwap(false, true) {
		return
	}
	go kc.seal()
}

func (kc *keyCheck) seal() {
	if err := kc.writeSealed(); err != nil {
		slog.Warn("could not seal the key check; trying again later", "dir", kc.dir, "error", err)
		time.AfterFunc(keyCheckRetry, func() { kc.sealing.Store(false) })
		return
	}
	kc.verified.Store(true)
}

func (kc *keyCheck) pendingBytes() []byte {
	out := append([]byte(keyCheckMagic), keyCheckVersion, keyCheckPending)
	out = binary.BigEndian.AppendUint64(out, kc.boundary)
	return binary.BigEndian.AppendUint32(out, crc32.ChecksumIEEE(out))
}

func (kc *keyCheck) writeSealed() error {
	nonce := make([]byte, kc.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return fmt.Errorf("key check nonce: %w", err)
	}
	out := append([]byte(keyCheckMagic), keyCheckVersion, keyCheckSealed)
	out = append(out, nonce...)
	return kc.write(kc.aead.Seal(out, nonce, keyCheckPlaintext, keyCheckAAD(kc.storeID)))
}

// writeFile replaces the file through a synced temporary file and a synced
// rename.
func (kc *keyCheck) writeFile(data []byte) error {
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

// storeHasData reports whether any segment in dir holds more than a header.
func storeHasData(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, err
	}
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".seg" {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return false, err
		}
		if info.Size() > segHeaderSize {
			return true, nil
		}
	}
	return false, nil
}
