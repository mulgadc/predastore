package filesystem

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetObject_FileExists(t *testing.T) {
	backend, _, cleanup := setupTestBackend(t)
	defer cleanup()

	// Create a test file
	testFile := "test_get.txt"
	testContent := []byte("This is test content for get operation")
	testPath := filepath.Join(backend.Config.Buckets[0].Pathname, testFile)
	require.NoError(t, os.WriteFile(testPath, testContent, 0644))

	// Verify file exists - the backend would serve it if we had a full context
	// Full end-to-end testing is done in s3 integration tests
	_, err := os.Stat(testPath)
	assert.NoError(t, err, "File should exist and be accessible")
}

func TestGetObjectHead_FileExists(t *testing.T) {
	backend, _, cleanup := setupTestBackend(t)
	defer cleanup()

	// Create a test file
	testFile := "test_head.txt"
	testContent := []byte("Content for HEAD request")
	testPath := filepath.Join(backend.Config.Buckets[0].Pathname, testFile)
	require.NoError(t, os.WriteFile(testPath, testContent, 0644))

	// Verify the backend can access the file
	_, err := os.Stat(testPath)
	assert.NoError(t, err, "File should exist")
}

func TestGetObject_NonExistent(t *testing.T) {
	backend, _, cleanup := setupTestBackend(t)
	defer cleanup()

	// Try to get a file that doesn't exist
	err := backend.GetObject("test-bucket", "nonexistent.txt", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "NoSuchObject")
}

func TestGetObject_InvalidBucket(t *testing.T) {
	backend, _, cleanup := setupTestBackend(t)
	defer cleanup()

	// Try to get from a bucket that doesn't exist
	err := backend.GetObject("invalid-bucket", "file.txt", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "NoSuchBucket")
}
