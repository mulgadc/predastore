package filesystem

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateMultipartUpload_InvalidBucket(t *testing.T) {
	backend, _, cleanup := setupTestBackend(t)
	defer cleanup()

	// Try to create multipart upload for invalid bucket
	err := backend.CreateMultipartUpload("invalid-bucket", "file.txt", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "NoSuchBucket")
}

func TestCreateMultipartUpload_InvalidKey(t *testing.T) {
	backend, _, cleanup := setupTestBackend(t)
	defer cleanup()

	// Try with invalid key name (non-UTF8)
	invalidKey := string([]byte{0xff, 0xfe, 0xfd})
	
	err := backend.CreateMultipartUpload("test-bucket", invalidKey, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "InvalidKey")
}

func TestCompleteMultipartUpload_InvalidBucket(t *testing.T) {
	backend, _, cleanup := setupTestBackend(t)
	defer cleanup()

	// Try to complete multipart upload for invalid bucket
	err := backend.CompleteMultipartUpload("invalid-bucket", "file.txt", "upload-id-123", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "NoSuchBucket")
}

func TestPutObjectPart_InvalidBucket(t *testing.T) {
	backend, _, cleanup := setupTestBackend(t)
	defer cleanup()

	// Try to put object part for invalid bucket
	err := backend.PutObjectPart("invalid-bucket", "file.txt", 1, "upload-id-123", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "NoSuchBucket")
}

func TestPutObjectPart_InvalidKey(t *testing.T) {
	backend, _, cleanup := setupTestBackend(t)
	defer cleanup()

	// Try with invalid key name
	invalidKey := string([]byte{0xff, 0xfe, 0xfd})
	
	err := backend.PutObjectPart("test-bucket", invalidKey, 1, "upload-id-123", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "InvalidKey")
}
