package masterkey_test

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/mulgadc/predastore/internal/masterkey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeKeyFile writes contents to a fresh path under t.TempDir() and explicitly
// chmods to mode so the test result is independent of the process umask.
func writeKeyFile(t *testing.T, name string, contents []byte, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, contents, mode))
	require.NoError(t, os.Chmod(path, mode))
	return path
}

func TestLoad_HappyPath(t *testing.T) {
	raw := make([]byte, masterkey.MasterKeySize)
	_, err := rand.Read(raw)
	require.NoError(t, err)

	path := writeKeyFile(t, "key", raw, 0o600)

	k, err := masterkey.Load(path)
	require.NoError(t, err)
	require.NotNil(t, k)
	assert.NotNil(t, k.AEAD, "Load must construct a usable AEAD")
	assert.Equal(t, masterkey.Fingerprint(raw), k.Fingerprint,
		"Fingerprint on the returned Key must match Fingerprint(rawBytes)")
}

func TestLoad_TightPermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission semantics not enforced on Windows")
	}

	raw := make([]byte, masterkey.MasterKeySize)

	for _, mode := range []os.FileMode{0o600, 0o400, 0o700} {
		t.Run(fmt.Sprintf("mode_%#o", mode), func(t *testing.T) {
			path := writeKeyFile(t, "key", raw, mode)
			_, err := masterkey.Load(path)
			assert.NoError(t, err, "mode %#o has no group/other bits set; should be accepted", mode)
		})
	}
}

func TestLoad_LoosePermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission semantics not enforced on Windows")
	}

	raw := make([]byte, masterkey.MasterKeySize)

	// Each of these has at least one bit set in the 0o077 (group/other) mask
	// and MUST be rejected outright — the loader is fail-closed with no
	// override flag.
	for _, mode := range []os.FileMode{0o644, 0o640, 0o604, 0o601, 0o610, 0o660, 0o666, 0o777} {
		t.Run(fmt.Sprintf("mode_%#o", mode), func(t *testing.T) {
			path := writeKeyFile(t, "key", raw, mode)
			_, err := masterkey.Load(path)
			require.Error(t, err, "mode %#o exposes group/other access; loader must reject", mode)
			assert.Contains(t, err.Error(), "permissions",
				"error should explain the perm failure to the operator")
		})
	}
}

func TestLoad_WrongLength(t *testing.T) {
	for _, n := range []int{0, 1, 16, 24, 31, 33, 64, 128} {
		t.Run(fmt.Sprintf("len_%d", n), func(t *testing.T) {
			path := writeKeyFile(t, "key", make([]byte, n), 0o600)
			_, err := masterkey.Load(path)
			require.Error(t, err, "length %d must be rejected", n)
			assert.Contains(t, err.Error(), fmt.Sprintf("%d bytes", masterkey.MasterKeySize),
				"error should cite the required size")
		})
	}
}

func TestLoad_MissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "does-not-exist")
	_, err := masterkey.Load(path)
	require.Error(t, err)
	assert.True(t, os.IsNotExist(err) || strings.Contains(err.Error(), "no such file"),
		"error should signal the missing path: %v", err)
}

func TestFingerprint_Stable(t *testing.T) {
	raw := make([]byte, masterkey.MasterKeySize)
	_, err := rand.Read(raw)
	require.NoError(t, err)

	fp1 := masterkey.Fingerprint(raw)
	fp2 := masterkey.Fingerprint(raw)
	assert.Equal(t, fp1, fp2, "fingerprint must be deterministic for the same key")

	// 8 bytes of sha256 → 16 hex chars; anything else is a contract change.
	assert.Len(t, fp1, 16)
	_, err = hex.DecodeString(fp1)
	assert.NoError(t, err, "fingerprint must be valid hex")
}

func TestFingerprint_Distinct(t *testing.T) {
	seen := make(map[string][]byte)
	for range 16 {
		raw := make([]byte, masterkey.MasterKeySize)
		_, err := rand.Read(raw)
		require.NoError(t, err)

		fp := masterkey.Fingerprint(raw)
		if prev, ok := seen[fp]; ok {
			t.Fatalf("collision: fingerprint %s for distinct keys %x and %x", fp, prev, raw)
		}
		seen[fp] = raw
	}
}
