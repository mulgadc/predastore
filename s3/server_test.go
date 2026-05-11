package s3

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

func TestCheckBaseDir(t *testing.T) {
	tests := []struct {
		name     string
		baseDir  string
		path     string
		expected string
	}{
		{
			name:     "relative path with base-dir set",
			baseDir:  "/home/ben/spinifex/predastore",
			path:     "distributed/db/node-1/",
			expected: "/home/ben/spinifex/predastore/distributed/db/node-1", // filepath.Join normalizes trailing slashes
		},
		{
			name:     "absolute path with base-dir set (should not change)",
			baseDir:  "/home/ben/spinifex/predastore",
			path:     "/var/data/node-1/",
			expected: "/var/data/node-1/",
		},
		{
			name:     "relative path with empty base-dir",
			baseDir:  "",
			path:     "distributed/db/node-1/",
			expected: "distributed/db/node-1/",
		},
		{
			name:     "empty path",
			baseDir:  "/home/ben/spinifex/predastore",
			path:     "",
			expected: "",
		},
		{
			name:     "both empty",
			baseDir:  "",
			path:     "",
			expected: "",
		},
		{
			name:     "relative path without trailing slash",
			baseDir:  "/home/ben/spinifex/predastore",
			path:     "distributed/nodes/node-2",
			expected: "/home/ben/spinifex/predastore/distributed/nodes/node-2",
		},
		{
			name:     "base-dir with trailing slash",
			baseDir:  "/home/ben/spinifex/predastore/",
			path:     "distributed/db/node-1/",
			expected: "/home/ben/spinifex/predastore/distributed/db/node-1", // filepath.Join normalizes
		},
		{
			name:     "simple relative path",
			baseDir:  "/data",
			path:     "db",
			expected: "/data/db",
		},
		{
			name:     "dot-relative path cleaned by filepath.Join",
			baseDir:  "/home/user/data",
			path:     "./subdir/file",
			expected: "/home/user/data/subdir/file", // filepath.Join cleans ./
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := checkBaseDir(tt.baseDir, tt.path)
			assert.Equal(t, tt.expected, result, "checkBaseDir(%q, %q)", tt.baseDir, tt.path)
		})
	}
}

func TestCheckBaseDir_IntegrationScenarios(t *testing.T) {
	// Simulate the actual config scenario from predastore.toml
	baseDir := "/home/ben/spinifex/predastore"

	// DB node paths from config (filepath.Join normalizes trailing slashes)
	dbPaths := []string{
		"distributed/db/node-1/",
		"distributed/db/node-2/",
		"distributed/db/node-3/",
	}

	expectedDBPaths := []string{
		"/home/ben/spinifex/predastore/distributed/db/node-1",
		"/home/ben/spinifex/predastore/distributed/db/node-2",
		"/home/ben/spinifex/predastore/distributed/db/node-3",
	}

	for i, path := range dbPaths {
		result := checkBaseDir(baseDir, path)
		assert.Equal(t, expectedDBPaths[i], result, "DB node %d path", i+1)
	}

	// QUIC node paths from config
	quicPaths := []string{
		"distributed/nodes/node-1/",
		"distributed/nodes/node-2/",
		"distributed/nodes/node-3/",
	}

	expectedQUICPaths := []string{
		"/home/ben/spinifex/predastore/distributed/nodes/node-1",
		"/home/ben/spinifex/predastore/distributed/nodes/node-2",
		"/home/ben/spinifex/predastore/distributed/nodes/node-3",
	}

	for i, path := range quicPaths {
		result := checkBaseDir(baseDir, path)
		assert.Equal(t, expectedQUICPaths[i], result, "QUIC node %d path", i+1)
	}
}

func TestCheckBaseDir_NoBaseDirSet(t *testing.T) {
	// When base-dir is not set, paths should remain unchanged
	baseDir := ""

	paths := []string{
		"distributed/db/node-1/",
		"distributed/nodes/node-1/",
		"/absolute/path/node-1/",
	}

	expectedPaths := []string{
		"distributed/db/node-1/",
		"distributed/nodes/node-1/",
		"/absolute/path/node-1/",
	}

	for i, path := range paths {
		result := checkBaseDir(baseDir, path)
		assert.Equal(t, expectedPaths[i], result, "Path %d should be unchanged when no base-dir", i+1)
	}
}
