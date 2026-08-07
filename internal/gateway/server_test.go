package gateway

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewServer_MissingEncryptionKeyFile verifies that NewServer fails fast
// when no key path is configured. The check happens before any backend init,
// so no goroutines or sockets are spawned by this test.
func TestNewServer_MissingEncryptionKeyFile(t *testing.T) {
	_, err := NewServer()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "encryption key file is required",
		"error must direct the operator at the missing flag/env var")
}

func TestNewServer_EncryptionKeyFile_NotFound(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-key")
	_, err := NewServer(WithEncryptionKeyFile(missing))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "load master key",
		"error must come from the key load step, not a later init failure")
}

func TestNewServer_EncryptionKeyFile_WrongLength(t *testing.T) {
	for _, n := range []int{0, 16, 24, 64} {
		path := filepath.Join(t.TempDir(), "key")
		require.NoError(t, os.WriteFile(path, make([]byte, n), 0o600))

		_, err := NewServer(WithEncryptionKeyFile(path))
		require.Errorf(t, err, "length %d must be rejected", n)
		assert.Contains(t, err.Error(), "load master key")
	}
}

func TestNewServer_EncryptionKeyFile_LoosePermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission semantics not enforced on Windows")
	}

	path := filepath.Join(t.TempDir(), "key")
	require.NoError(t, os.WriteFile(path, make([]byte, 32), 0o644))
	require.NoError(t, os.Chmod(path, 0o644))

	_, err := NewServer(WithEncryptionKeyFile(path))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "load master key")
	assert.Contains(t, err.Error(), "permissions",
		"loose-perm rejection should surface up through NewServer")
}
