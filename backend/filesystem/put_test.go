package filesystem

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPutObject_InvalidBucket(t *testing.T) {
	backend, _, cleanup := setupTestBackend(t)
	defer cleanup()

	// Try to put to a bucket that doesn't exist
	err := backend.PutObject("invalid-bucket", "file.txt", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "NoSuchBucket")
}

func TestPutObject_BucketValidation(t *testing.T) {
	backend, _, cleanup := setupTestBackend(t)
	defer cleanup()

	// PutObject with invalid bucket should fail early
	testFile := "test.txt"
	
	err := backend.PutObject("invalid-bucket", testFile, nil)
	
	// Should fail with NoSuchBucket before needing context
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "NoSuchBucket")
}

func TestPutObject_ValidatesKeyName(t *testing.T) {
	backend, _, cleanup := setupTestBackend(t)
	defer cleanup()

	// Try to put with an invalid key (non-UTF8)
	invalidKey := string([]byte{0xff, 0xfe, 0xfd})
	
	err := backend.PutObject("test-bucket", invalidKey, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "InvalidKey")
}
