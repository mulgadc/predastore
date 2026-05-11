// Package crypto loads and identifies the cluster master key used to seal and
// open shard fragments at rest. The key is held as a raw 32-byte slice — the
// same on-disk format as the IAM master key — and never logged in raw form;
// callers identify it via Fingerprint instead.
package crypto

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
)

// MasterKeySize is the required length of an AES-256 master key in bytes.
const MasterKeySize = 32

// LoadMasterKey reads a 32-byte AES-256 master key from disk. It is fail-closed
// on loose permissions: any group/other-readable mode (mode & 0077 != 0) is
// rejected outright with no override. The master key gates plaintext access to
// every object cluster-wide; warn-and-allow would put us one ignored log line
// from a permanent breach.
func LoadMasterKey(path string) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat master key %s: %w", path, err)
	}

	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		return nil, fmt.Errorf("master key %s has permissions %#o: must be 0600 (group/other access disallowed)", path, perm)
	}

	key, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read master key %s: %w", path, err)
	}

	if len(key) != MasterKeySize {
		return nil, fmt.Errorf("master key %s must be %d bytes, got %d", path, MasterKeySize, len(key))
	}

	return key, nil
}

// Fingerprint returns a short, log-safe identifier for a master key:
// hex of sha256(key)[:8] (16 hex chars). Distinct from a crypto-grade
// identifier — just enough to disambiguate a handful of keys in operator logs.
func Fingerprint(key []byte) string {
	sum := sha256.Sum256(key)
	return hex.EncodeToString(sum[:8])
}
