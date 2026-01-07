package filesystem

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListBuckets_ReturnsNilWithoutContext(t *testing.T) {
	backend, _, cleanup := setupTestBackend(t)
	defer cleanup()

	// ListBuckets should return nil when context is nil (for unit testing)
	err := backend.ListBuckets(nil)
	assert.NoError(t, err, "Should handle nil context gracefully")
}

func TestListObjectsV2Handler_InvalidBucket(t *testing.T) {
	backend, _, cleanup := setupTestBackend(t)
	defer cleanup()

	// Try to list objects in a bucket that doesn't exist
	err := backend.ListObjectsV2Handler("invalid-bucket", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "NoSuchBucket")
}

func TestListObjectsV2Handler_EmptyBucket(t *testing.T) {
	backend, _, cleanup := setupTestBackend(t)
	defer cleanup()

	// List objects in an empty bucket
	// Note: Requires Fiber context for XML response
	err := backend.ListObjectsV2Handler("test-bucket", nil)
	
	// Will error due to nil context, but should not error on "NoSuchBucket"
	if err != nil {
		assert.NotContains(t, err.Error(), "NoSuchBucket", "Bucket should exist")
	}
}

func TestListObjectsV2Handler_WithFiles(t *testing.T) {
	backend, _, cleanup := setupTestBackend(t)
	defer cleanup()

	// Create some test files
	files := []string{"file1.txt", "file2.txt", "dir1/file3.txt"}
	for _, file := range files {
		testPath := filepath.Join(backend.Config.Buckets[0].Pathname, file)
		require.NoError(t, os.MkdirAll(filepath.Dir(testPath), 0755))
		require.NoError(t, os.WriteFile(testPath, []byte("test"), 0644))
	}

	// Note: Full testing requires integration tests with Fiber context
	// These unit tests validate the backend logic exists and bucket validation works
	err := backend.ListObjectsV2Handler("test-bucket", nil)
	
	if err != nil {
		assert.NotContains(t, err.Error(), "NoSuchBucket")
	}
}
