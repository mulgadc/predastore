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
)

// keyCheckFilename holds the evidence that the store's master key is the key
// that sealed its data. A node under another key fails every fragment, and
// only this evidence tells that apart from corruption.
const keyCheckFilename = "keycheck"

// keyCheckRefusedFilename keeps a key check that another key failed to open.
// Its presence keeps the store unverified for good: it may hold shards that
// other key sealed, so the original key returning proves nothing.
const keyCheckRefusedFilename = "keycheck.refused"

const (
	keyCheckMagic   = "PDKC"
	keyCheckVersion = 1
	keyCheckSealed  = 's'
	keyCheckHeader  = len(keyCheckMagic) + 2
)

var keyCheckPlaintext = []byte("predastore key check")

// keyCheckAAD binds the check to the store identity that every fragment's
// nonce carries, so a check copied from another store fails.
func keyCheckAAD(storeID uint32) []byte {
	return binary.BigEndian.AppendUint32([]byte("predastore keycheck v1"), storeID)
}

// segmentsHoldData reports whether a store directory holds data. A test
// replaces it to fail the scan.
var segmentsHoldData = storeHasData

// openKeyCheck reports whether the store's key is verified. Only a new store
// gets a key check, sealed here; a store that held data before the check
// existed has none and is never verified. Unreadable evidence never stops the
// store from opening, except a refusal that cannot be made durable.
func openKeyCheck(dir string, aead cipher.AEAD, storeID uint32) (bool, error) {
	path := filepath.Join(dir, keyCheckFilename)
	if _, err := os.Stat(filepath.Join(dir, keyCheckRefusedFilename)); err == nil {
		slog.Warn("this store once opened under another master key; suspect-shard repair stays off on it", "dir", dir)
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		slog.Error("could not read the store's refused key check; integrity failures are not marked for repair",
			"dir", dir, "error", err)
		return false, nil
	}
	raw, err := os.ReadFile(path)
	hasData := true
	if errors.Is(err, os.ErrNotExist) {
		// Only a store with no check needs to know whether it is new. A scan
		// that cannot tell leaves repair off rather than the store closed.
		var scanErr error
		if hasData, scanErr = segmentsHoldData(dir); scanErr != nil {
			slog.Error("could not tell whether the store is new; integrity failures are not marked for repair",
				"dir", dir, "error", scanErr)
			return false, nil
		}
	}
	switch {
	case errors.Is(err, os.ErrNotExist) && !hasData:
		if err := writeKeyCheck(dir, aead, storeID); err != nil {
			slog.Error("could not write the store's key check; integrity failures are not marked for repair",
				"dir", dir, "error", err)
			return false, nil
		}
		return true, nil
	case errors.Is(err, os.ErrNotExist):
		slog.Warn("this store was created before the key check; suspect-shard repair is off on it "+
			"until a check across nodes can confirm its key", "dir", dir)
		return false, nil
	case err != nil:
		slog.Error("could not read the store's key check; integrity failures are not marked for repair",
			"dir", dir, "error", err)
		return false, nil
	}

	nonceSize := aead.NonceSize()
	if len(raw) < keyCheckHeader+nonceSize || string(raw[:len(keyCheckMagic)]) != keyCheckMagic ||
		raw[len(keyCheckMagic)] != keyCheckVersion || raw[len(keyCheckMagic)+1] != keyCheckSealed {
		slog.Error("the store's key check does not decode; integrity failures are not marked for repair", "dir", dir)
		return false, nil
	}
	body := raw[keyCheckHeader:]
	plain, err := aead.Open(nil, body[:nonceSize], body[nonceSize:], keyCheckAAD(storeID))
	if err != nil || !bytes.Equal(plain, keyCheckPlaintext) {
		// Writes go on under this key, so no later start may trust the
		// check again; the store keeps serving.
		refuseErr := os.Rename(path, filepath.Join(dir, keyCheckRefusedFilename))
		if refuseErr == nil {
			refuseErr = syncDirectory(dir)
		}
		if refuseErr != nil {
			// Until the refusal is on disk a restart could trust the check
			// again, so the store takes no write under this key.
			return false, fmt.Errorf("the master key does not open the store's key check, and refusing it failed: %w", refuseErr)
		}
		slog.Error("the master key does not open this store's key check; suspect-shard repair is off on this store for good",
			"dir", dir)
		return false, nil
	}
	return true, nil
}

// writeKeyCheck seals the check through a synced temporary file and a synced
// rename.
func writeKeyCheck(dir string, aead cipher.AEAD, storeID uint32) error {
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return fmt.Errorf("key check nonce: %w", err)
	}
	out := append([]byte(keyCheckMagic), keyCheckVersion, keyCheckSealed)
	out = append(out, nonce...)
	data := aead.Seal(out, nonce, keyCheckPlaintext, keyCheckAAD(storeID))

	path := filepath.Join(dir, keyCheckFilename)
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
	return syncDirectory(dir)
}

// KeyVerified reports whether the store has evidence that its master key is
// the key that sealed its data. Without it, a failed read may be a wrong key.
func (store *Store) KeyVerified() bool { return store.keyVerified }

// storeHasData reports whether dir holds any segment file. Only a vault with
// none is new; one with only segment headers may come from an earlier release.
func storeHasData(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, err
	}
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".seg" {
			return true, nil
		}
	}
	return false, nil
}
