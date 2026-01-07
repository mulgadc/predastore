package filesystem

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mulgadc/predastore/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestBackend(t *testing.T) (*Backend, *s3.Config, func()) {
	// Create a temporary directory for test data
	tmpDir := t.TempDir()
	testBucketDir := filepath.Join(tmpDir, "test-bucket")
	require.NoError(t, os.MkdirAll(testBucketDir, 0755))

	config := &s3.Config{
		Region: "us-east-1",
		Buckets: []s3.S3_Buckets{
			{
				Name:     "test-bucket",
				Pathname: testBucketDir,
				Type:     "fs",
			},
		},
		Auth: []s3.AuthEntry{
			{
				AccessKeyID:     "test-access-key",
				SecretAccessKey: "test-secret-key",
			},
		},
	}

	backend, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, backend)

	cleanup := func() {
		// Cleanup is handled by t.TempDir()
	}

	return backend, config, cleanup
}

func TestDeleteObject(t *testing.T) {
	backend, _, cleanup := setupTestBackend(t)
	defer cleanup()

	// Create a test file
	testFile := "test_delete.txt"
	testContent := []byte("This is a test file for deletion")
	testPath := filepath.Join(backend.Config.Buckets[0].Pathname, testFile)
	require.NoError(t, os.WriteFile(testPath, testContent, 0644))

	// Verify file exists
	_, err := os.Stat(testPath)
	require.NoError(t, err)

	// Delete the object (pass nil for fiber.Ctx since we're testing backend directly)
	err = backend.DeleteObject("test-bucket", testFile, nil)
	assert.NoError(t, err)

	// Verify file no longer exists
	_, err = os.Stat(testPath)
	assert.True(t, os.IsNotExist(err), "File should no longer exist")
}

func TestDeleteObject_NonExistent(t *testing.T) {
	backend, _, cleanup := setupTestBackend(t)
	defer cleanup()

	// Try to delete a file that doesn't exist
	err := backend.DeleteObject("test-bucket", "nonexistent.txt", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "NoSuchObject")
}

func TestDeleteObject_InvalidBucket(t *testing.T) {
	backend, _, cleanup := setupTestBackend(t)
	defer cleanup()

	// Try to delete from a bucket that doesn't exist
	err := backend.DeleteObject("invalid-bucket", "file.txt", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "NoSuchBucket")
}

func TestDeleteObject_WithNestedDirectories(t *testing.T) {
	backend, _, cleanup := setupTestBackend(t)
	defer cleanup()

	// Create a nested file structure
	testFile := "level1/level2/level3/nested.txt"
	testContent := []byte("Nested file content")
	testPath := filepath.Join(backend.Config.Buckets[0].Pathname, testFile)
	
	// Create parent directories
	require.NoError(t, os.MkdirAll(filepath.Dir(testPath), 0755))
	require.NoError(t, os.WriteFile(testPath, testContent, 0644))

	// Delete the nested file
	err := backend.DeleteObject("test-bucket", testFile, nil)
	assert.NoError(t, err)

	// Verify file is deleted
	_, err = os.Stat(testPath)
	assert.True(t, os.IsNotExist(err))

	// Verify empty parent directories are cleaned up
	level3Dir := filepath.Join(backend.Config.Buckets[0].Pathname, "level1/level2/level3")
	_, err = os.Stat(level3Dir)
	assert.True(t, os.IsNotExist(err), "Empty directories should be cleaned up")
}

func TestDeleteEmptyParentDirs(t *testing.T) {
	backend, _, cleanup := setupTestBackend(t)
	defer cleanup()

	bucketPath := backend.Config.Buckets[0].Pathname

	// Create a nested directory structure with a file
	nestedPath := filepath.Join(bucketPath, "a/b/c/d/file.txt")
	require.NoError(t, os.MkdirAll(filepath.Dir(nestedPath), 0755))
	require.NoError(t, os.WriteFile(nestedPath, []byte("test"), 0644))

	// Delete the file
	require.NoError(t, os.Remove(nestedPath))

	// Call deleteEmptyParentDirs
	err := deleteEmptyParentDirs(nestedPath, bucketPath)
	assert.NoError(t, err)

	// Verify all empty directories are removed
	_, err = os.Stat(filepath.Join(bucketPath, "a"))
	assert.True(t, os.IsNotExist(err), "Empty parent directories should be removed")
}

func TestDeleteEmptyParentDirs_StopsAtNonEmptyDir(t *testing.T) {
	backend, _, cleanup := setupTestBackend(t)
	defer cleanup()

	bucketPath := backend.Config.Buckets[0].Pathname

	// Create a nested structure with two files at different levels
	file1Path := filepath.Join(bucketPath, "a/b/file1.txt")
	file2Path := filepath.Join(bucketPath, "a/b/c/file2.txt")
	
	require.NoError(t, os.MkdirAll(filepath.Dir(file1Path), 0755))
	require.NoError(t, os.MkdirAll(filepath.Dir(file2Path), 0755))
	require.NoError(t, os.WriteFile(file1Path, []byte("test1"), 0644))
	require.NoError(t, os.WriteFile(file2Path, []byte("test2"), 0644))

	// Delete file2
	require.NoError(t, os.Remove(file2Path))

	// Call deleteEmptyParentDirs
	err := deleteEmptyParentDirs(file2Path, bucketPath)
	assert.NoError(t, err)

	// Verify directory 'c' is removed but 'b' remains (it has file1)
	_, err = os.Stat(filepath.Join(bucketPath, "a/b/c"))
	assert.True(t, os.IsNotExist(err), "Empty 'c' directory should be removed")

	_, err = os.Stat(filepath.Join(bucketPath, "a/b"))
	assert.False(t, os.IsNotExist(err), "Non-empty 'b' directory should remain")

	// Verify file1 still exists
	_, err = os.Stat(file1Path)
	assert.NoError(t, err, "file1 should still exist")
}
