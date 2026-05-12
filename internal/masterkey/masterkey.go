// Package masterkey loads the cluster master key from disk and turns it into
// an AES-256-GCM cipher.AEAD used to seal and open shard fragments at rest.
//
// Load is the only path that touches the raw 32-byte key on disk. It returns
// a *Key whose AEAD field is the only handle production code holds — the
// underlying key bytes are not retained on the Key. Callers identify a key
// via Key.Fingerprint instead of logging raw bytes.
package masterkey

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
)

// MasterKeySize is the required length of an AES-256 master key in bytes.
const MasterKeySize = 32

// Key is the runtime handle for a loaded master key. AEAD is safe for
// concurrent use per the crypto/cipher contract; Fingerprint is a short,
// log-safe identifier (16 hex chars) derived from the key bytes.
type Key struct {
	AEAD        cipher.AEAD
	Fingerprint string
}

// Load reads a 32-byte AES-256 master key from disk, builds the AEAD, and
// returns a *Key. The raw key bytes are not retained on the returned value —
// only the AEAD and its fingerprint survive past this call. It is fail-closed
// on loose permissions: any group/other-readable mode (mode & 0077 != 0) is
// rejected outright with no override. The master key gates plaintext access
// to every object cluster-wide; warn-and-allow would put us one ignored log
// line from a permanent breach.
func Load(path string) (*Key, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat master key %s: %w", path, err)
	}

	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		return nil, fmt.Errorf("master key %s has permissions %#o: must be 0600 (group/other access disallowed)", path, perm)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read master key %s: %w", path, err)
	}

	if len(raw) != MasterKeySize {
		return nil, fmt.Errorf("master key %s must be %d bytes, got %d", path, MasterKeySize, len(raw))
	}

	aead, err := NewAEAD(raw)
	if err != nil {
		return nil, err
	}
	return &Key{AEAD: aead, Fingerprint: Fingerprint(raw)}, nil
}

// NewAEAD builds an AES-256-GCM AEAD from a 32-byte master key. Exposed for
// tests and for callers that already hold the key bytes (e.g. deterministic
// test fixtures). Production code should prefer Load.
func NewAEAD(key []byte) (cipher.AEAD, error) {
	if len(key) != MasterKeySize {
		return nil, fmt.Errorf("master key must be %d bytes, got %d", MasterKeySize, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create AES cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}
	return aead, nil
}

// Fingerprint returns a short, log-safe identifier for a master key:
// hex of sha256(key)[:8] (16 hex chars). Distinct from a crypto-grade
// identifier — just enough to disambiguate a handful of keys in operator logs.
func Fingerprint(key []byte) string {
	sum := sha256.Sum256(key)
	return hex.EncodeToString(sum[:8])
}
