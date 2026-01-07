package backend

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestS3Error_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      *S3Error
		expected string
	}{
		{
			name:     "without resource",
			err:      &S3Error{Code: ErrNoSuchBucket, Message: "Bucket not found"},
			expected: "NoSuchBucket: Bucket not found",
		},
		{
			name:     "with resource",
			err:      &S3Error{Code: ErrNoSuchKey, Message: "Key not found", Resource: "my-key"},
			expected: "NoSuchKey: Key not found (resource: my-key)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.err.Error())
		})
	}
}

func TestS3Error_Is(t *testing.T) {
	err1 := &S3Error{Code: ErrNoSuchBucket}
	err2 := &S3Error{Code: ErrNoSuchBucket}
	err3 := &S3Error{Code: ErrNoSuchKey}

	assert.True(t, err1.Is(err2))
	assert.False(t, err1.Is(err3))
	assert.False(t, err1.Is(errors.New("other error")))
}

func TestS3Error_WithResource(t *testing.T) {
	original := ErrNoSuchBucketError
	withResource := original.WithResource("my-bucket")

	assert.Equal(t, original.Code, withResource.Code)
	assert.Equal(t, original.Message, withResource.Message)
	assert.Equal(t, original.StatusCode, withResource.StatusCode)
	assert.Equal(t, "my-bucket", withResource.Resource)
	assert.Empty(t, original.Resource) // Original unchanged
}

func TestNewS3Error(t *testing.T) {
	err := NewS3Error(ErrInternalError, "Custom message", 500)

	assert.Equal(t, ErrInternalError, err.Code)
	assert.Equal(t, "Custom message", err.Message)
	assert.Equal(t, 500, err.StatusCode)
}

func TestIsS3Error(t *testing.T) {
	s3err := &S3Error{Code: ErrNoSuchBucket}
	regularErr := errors.New("regular error")

	result, ok := IsS3Error(s3err)
	assert.True(t, ok)
	assert.Equal(t, s3err, result)

	result, ok = IsS3Error(regularErr)
	assert.False(t, ok)
	assert.Nil(t, result)
}

func TestGetHTTPStatus(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected int
	}{
		{
			name:     "S3Error with status",
			err:      &S3Error{Code: ErrNoSuchBucket, StatusCode: 404},
			expected: 404,
		},
		{
			name:     "regular error",
			err:      errors.New("regular error"),
			expected: 500,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, GetHTTPStatus(tt.err))
		})
	}
}

func TestPredefinedErrors(t *testing.T) {
	tests := []struct {
		err        *S3Error
		code       S3ErrorCode
		statusCode int
	}{
		{ErrNoSuchBucketError, ErrNoSuchBucket, 404},
		{ErrNoSuchKeyError, ErrNoSuchKey, 404},
		{ErrNoSuchUploadError, ErrNoSuchUpload, 404},
		{ErrInvalidKeyError, ErrInvalidKey, 400},
		{ErrInvalidPartError, ErrInvalidPart, 400},
		{ErrAccessDeniedError, ErrAccessDenied, 403},
		{ErrInternalServerError, ErrInternalError, 500},
		{ErrInvalidRangeError, ErrInvalidRange, 416},
	}

	for _, tt := range tests {
		t.Run(string(tt.code), func(t *testing.T) {
			assert.Equal(t, tt.code, tt.err.Code)
			assert.Equal(t, tt.statusCode, tt.err.StatusCode)
			assert.NotEmpty(t, tt.err.Message)
		})
	}
}
