package s3

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/mulgadc/predastore/backend"
	"github.com/stretchr/testify/require"
)

// BackendType represents the type of backend to use for testing
type BackendType string

const (
	BackendFilesystem  BackendType = "filesystem"
	BackendDistributed BackendType = "distributed"
)

// TestBackend holds the configuration and app for a specific backend
type TestBackend struct {
	Type    BackendType
	Config  *Config
	App     *fiber.App
	Backend backend.Backend
	Cleanup func()
}

// setupFilesystemBackend creates a filesystem backend for testing
func setupFilesystemBackend(t *testing.T) *TestBackend {
	t.Helper()

	s3 := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := s3.ReadConfig()
	require.NoError(t, err, "Should read config without error")

	be := s3.createFilesystemBackend()
	app := s3.SetupRoutesWithBackend(be)

	return &TestBackend{
		Type:    BackendFilesystem,
		Config:  s3,
		App:     app,
		Backend: be,
		Cleanup: func() {
			be.Close()
		},
	}
}

// setupDistributedBackend creates a distributed backend for testing
func setupDistributedBackend(t *testing.T) *TestBackend {
	t.Helper()

	// Create a temporary directory for badger
	tmpDir, err := os.MkdirTemp("", "distributed-test-*")
	require.NoError(t, err, "Should create temp directory")

	s3 := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "cluster.toml"),
	})
	err = s3.ReadConfig()
	require.NoError(t, err, "Should read config without error")

	// Override badger dir with temp directory
	s3.BadgerDir = tmpDir

	be := s3.createDistributedBackend()
	app := s3.SetupRoutesWithBackend(be)

	return &TestBackend{
		Type:    BackendDistributed,
		Config:  s3,
		App:     app,
		Backend: be,
		Cleanup: func() {
			be.Close()
			os.RemoveAll(tmpDir)
		},
	}
}

// RunWithBackends runs a test function against multiple backends
func RunWithBackends(t *testing.T, backends []BackendType, testFn func(t *testing.T, tb *TestBackend)) {
	for _, backendType := range backends {
		backendType := backendType // capture range variable
		t.Run(string(backendType), func(t *testing.T) {
			var tb *TestBackend
			switch backendType {
			case BackendFilesystem:
				tb = setupFilesystemBackend(t)
			case BackendDistributed:
				tb = setupDistributedBackend(t)
			default:
				t.Fatalf("Unknown backend type: %s", backendType)
			}
			defer tb.Cleanup()

			testFn(t, tb)
		})
	}
}

// AllBackends returns all available backend types for testing
func AllBackends() []BackendType {
	return []BackendType{BackendFilesystem, BackendDistributed}
}

// FilesystemOnly returns only the filesystem backend for testing
func FilesystemOnly() []BackendType {
	return []BackendType{BackendFilesystem}
}

// DistributedOnly returns only the distributed backend for testing
func DistributedOnly() []BackendType {
	return []BackendType{BackendDistributed}
}
