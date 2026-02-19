package filesystem

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mulgadc/predastore/backend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testSetup creates a test backend with a temporary directory
func testSetup(t *testing.T) (*Backend, string, func()) {
	t.Helper()

	tmpDir := t.TempDir()
	bucketDir := filepath.Join(tmpDir, "test-bucket")
	require.NoError(t, os.MkdirAll(bucketDir, 0755))

	config := &Config{
		Buckets: []BucketConfig{
			{
				Name:     "test-bucket",
				Pathname: bucketDir,
				Region:   "us-east-1",
				Type:     "fs",
			},
		},
		TempDir:   tmpDir,
		OwnerID:   "test-owner",
		OwnerName: "Test Owner",
	}

	be, err := New(config)
	require.NoError(t, err)

	cleanup := func() {
		be.Close()
	}

	return be.(*Backend), tmpDir, cleanup
}

// createTestFile creates a test file in the bucket
func createTestFile(t *testing.T, be *Backend, key string, content []byte) {
	t.Helper()
	bucket := be.config.Buckets[0]
	path := filepath.Join(bucket.Pathname, key)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
	require.NoError(t, os.WriteFile(path, content, 0644))
}

func TestNew(t *testing.T) {
	tests := []struct {
		name    string
		config  any
		wantErr bool
	}{
		{
			name: "valid config",
			config: &Config{
				Buckets: []BucketConfig{{Name: "test", Pathname: "/tmp"}},
			},
			wantErr: false,
		},
		{
			name:    "invalid config type",
			config:  "invalid",
			wantErr: true,
		},
		{
			name:    "nil config",
			config:  nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			be, err := New(tt.config)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, be)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, be)
			}
		})
	}
}

func TestBackend_Type(t *testing.T) {
	be, _, cleanup := testSetup(t)
	defer cleanup()

	assert.Equal(t, "filesystem", be.Type())
}

func TestBackend_PutObject(t *testing.T) {
	be, _, cleanup := testSetup(t)
	defer cleanup()
	ctx := context.Background()

	tests := []struct {
		name    string
		req     *backend.PutObjectRequest
		wantErr bool
		errCode backend.S3ErrorCode
	}{
		{
			name: "successful put",
			req: &backend.PutObjectRequest{
				Bucket: "test-bucket",
				Key:    "test-file.txt",
				Body:   strings.NewReader("hello world"),
			},
			wantErr: false,
		},
		{
			name: "put with nested path",
			req: &backend.PutObjectRequest{
				Bucket: "test-bucket",
				Key:    "dir1/dir2/nested-file.txt",
				Body:   strings.NewReader("nested content"),
			},
			wantErr: false,
		},
		{
			name: "invalid bucket",
			req: &backend.PutObjectRequest{
				Bucket: "nonexistent-bucket",
				Key:    "test.txt",
				Body:   strings.NewReader("content"),
			},
			wantErr: true,
			errCode: backend.ErrNoSuchBucket,
		},
		{
			name: "invalid key",
			req: &backend.PutObjectRequest{
				Bucket: "test-bucket",
				Key:    string([]byte{0xff, 0xfe}),
				Body:   strings.NewReader("content"),
			},
			wantErr: true,
			errCode: backend.ErrInvalidKey,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := be.PutObject(ctx, tt.req)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errCode != "" {
					s3err, ok := backend.IsS3Error(err)
					assert.True(t, ok, "expected S3Error")
					assert.Equal(t, tt.errCode, s3err.Code)
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, resp)
				assert.NotEmpty(t, resp.ETag)
			}
		})
	}
}

func TestBackend_GetObject(t *testing.T) {
	be, _, cleanup := testSetup(t)
	defer cleanup()
	ctx := context.Background()

	// Setup test file
	content := []byte("test content for get object")
	createTestFile(t, be, "get-test.txt", content)

	tests := []struct {
		name        string
		req         *backend.GetObjectRequest
		wantErr     bool
		errCode     backend.S3ErrorCode
		wantContent []byte
		wantStatus  int
	}{
		{
			name: "successful get",
			req: &backend.GetObjectRequest{
				Bucket:     "test-bucket",
				Key:        "get-test.txt",
				RangeStart: -1,
				RangeEnd:   -1,
			},
			wantErr:     false,
			wantContent: content,
			wantStatus:  200,
		},
		{
			name: "range request",
			req: &backend.GetObjectRequest{
				Bucket:     "test-bucket",
				Key:        "get-test.txt",
				RangeStart: 0,
				RangeEnd:   4,
			},
			wantErr:     false,
			wantContent: []byte("test "),
			wantStatus:  206,
		},
		{
			name: "nonexistent file",
			req: &backend.GetObjectRequest{
				Bucket:     "test-bucket",
				Key:        "nonexistent.txt",
				RangeStart: -1,
				RangeEnd:   -1,
			},
			wantErr: true,
			errCode: backend.ErrNoSuchKey,
		},
		{
			name: "nonexistent bucket",
			req: &backend.GetObjectRequest{
				Bucket:     "nonexistent-bucket",
				Key:        "file.txt",
				RangeStart: -1,
				RangeEnd:   -1,
			},
			wantErr: true,
			errCode: backend.ErrNoSuchBucket,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := be.GetObject(ctx, tt.req)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errCode != "" {
					s3err, ok := backend.IsS3Error(err)
					assert.True(t, ok, "expected S3Error")
					assert.Equal(t, tt.errCode, s3err.Code)
				}
			} else {
				require.NoError(t, err)
				assert.NotNil(t, resp)
				assert.Equal(t, tt.wantStatus, resp.StatusCode)

				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				require.NoError(t, err)
				assert.Equal(t, tt.wantContent, body)
			}
		})
	}
}

func TestBackend_HeadObject(t *testing.T) {
	be, _, cleanup := testSetup(t)
	defer cleanup()
	ctx := context.Background()

	content := []byte("head test content")
	createTestFile(t, be, "head-test.txt", content)

	tests := []struct {
		name    string
		bucket  string
		key     string
		wantErr bool
		errCode backend.S3ErrorCode
	}{
		{
			name:    "successful head",
			bucket:  "test-bucket",
			key:     "head-test.txt",
			wantErr: false,
		},
		{
			name:    "nonexistent file",
			bucket:  "test-bucket",
			key:     "nonexistent.txt",
			wantErr: true,
			errCode: backend.ErrNoSuchKey,
		},
		{
			name:    "nonexistent bucket",
			bucket:  "nonexistent-bucket",
			key:     "file.txt",
			wantErr: true,
			errCode: backend.ErrNoSuchBucket,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := be.HeadObject(ctx, tt.bucket, tt.key)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errCode != "" {
					s3err, ok := backend.IsS3Error(err)
					assert.True(t, ok)
					assert.Equal(t, tt.errCode, s3err.Code)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, int64(len(content)), resp.ContentLength)
				assert.NotEmpty(t, resp.ETag)
			}
		})
	}
}

func TestBackend_DeleteObject(t *testing.T) {
	be, _, cleanup := testSetup(t)
	defer cleanup()
	ctx := context.Background()

	tests := []struct {
		name    string
		setup   func()
		req     *backend.DeleteObjectRequest
		wantErr bool
		errCode backend.S3ErrorCode
	}{
		{
			name: "successful delete",
			setup: func() {
				createTestFile(t, be, "delete-test.txt", []byte("to be deleted"))
			},
			req: &backend.DeleteObjectRequest{
				Bucket: "test-bucket",
				Key:    "delete-test.txt",
			},
			wantErr: false,
		},
		{
			name: "delete with empty parent cleanup",
			setup: func() {
				createTestFile(t, be, "dir1/dir2/nested.txt", []byte("nested"))
			},
			req: &backend.DeleteObjectRequest{
				Bucket: "test-bucket",
				Key:    "dir1/dir2/nested.txt",
			},
			wantErr: false,
		},
		{
			name:  "nonexistent file",
			setup: func() {},
			req: &backend.DeleteObjectRequest{
				Bucket: "test-bucket",
				Key:    "nonexistent.txt",
			},
			wantErr: true,
			errCode: backend.ErrNoSuchKey,
		},
		{
			name:  "nonexistent bucket",
			setup: func() {},
			req: &backend.DeleteObjectRequest{
				Bucket: "nonexistent-bucket",
				Key:    "file.txt",
			},
			wantErr: true,
			errCode: backend.ErrNoSuchBucket,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()

			err := be.DeleteObject(ctx, tt.req)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errCode != "" {
					s3err, ok := backend.IsS3Error(err)
					assert.True(t, ok)
					assert.Equal(t, tt.errCode, s3err.Code)
				}
			} else {
				assert.NoError(t, err)
				// Verify file was deleted
				path := filepath.Join(be.config.Buckets[0].Pathname, tt.req.Key)
				_, err := os.Stat(path)
				assert.True(t, os.IsNotExist(err))
			}
		})
	}
}

func TestBackend_ListBuckets(t *testing.T) {
	be, _, cleanup := testSetup(t)
	defer cleanup()
	ctx := context.Background()

	resp, err := be.ListBuckets(ctx, "test-owner")
	require.NoError(t, err)
	assert.Len(t, resp.Buckets, 1)
	assert.Equal(t, "test-bucket", resp.Buckets[0].Name)
	assert.Equal(t, "test-owner", resp.Owner.ID)
	assert.Equal(t, "Test Owner", resp.Owner.DisplayName)
}

func TestBackend_ListObjects(t *testing.T) {
	be, _, cleanup := testSetup(t)
	defer cleanup()
	ctx := context.Background()

	// Setup test files
	createTestFile(t, be, "file1.txt", []byte("content1"))
	createTestFile(t, be, "file2.txt", []byte("content2"))
	createTestFile(t, be, "subdir/file3.txt", []byte("content3"))

	tests := []struct {
		name         string
		req          *backend.ListObjectsRequest
		wantErr      bool
		wantFiles    int
		wantPrefixes int
	}{
		{
			name: "list root",
			req: &backend.ListObjectsRequest{
				Bucket: "test-bucket",
			},
			wantErr:      false,
			wantFiles:    2,
			wantPrefixes: 1, // subdir/
		},
		{
			name: "list with prefix",
			req: &backend.ListObjectsRequest{
				Bucket: "test-bucket",
				Prefix: "file",
			},
			wantErr:      false,
			wantFiles:    2,
			wantPrefixes: 0,
		},
		{
			name: "nonexistent bucket",
			req: &backend.ListObjectsRequest{
				Bucket: "nonexistent-bucket",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := be.ListObjects(ctx, tt.req)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantFiles, len(resp.Contents))
				assert.Equal(t, tt.wantPrefixes, len(resp.CommonPrefixes))
			}
		})
	}
}

func TestBackend_MultipartUpload(t *testing.T) {
	be, _, cleanup := testSetup(t)
	defer cleanup()
	ctx := context.Background()

	// Test create multipart upload
	createResp, err := be.CreateMultipartUpload(ctx, &backend.CreateMultipartUploadRequest{
		Bucket: "test-bucket",
		Key:    "multipart-test.txt",
	})
	require.NoError(t, err)
	assert.NotEmpty(t, createResp.UploadID)
	assert.Equal(t, "test-bucket", createResp.Bucket)
	assert.Equal(t, "multipart-test.txt", createResp.Key)

	// Upload parts
	part1Content := bytes.Repeat([]byte("a"), 5*1024*1024) // 5MB minimum
	part1Resp, err := be.UploadPart(ctx, &backend.UploadPartRequest{
		Bucket:     "test-bucket",
		Key:        "multipart-test.txt",
		UploadID:   createResp.UploadID,
		PartNumber: 1,
		Body:       bytes.NewReader(part1Content),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, part1Resp.ETag)

	part2Content := []byte("last part")
	part2Resp, err := be.UploadPart(ctx, &backend.UploadPartRequest{
		Bucket:     "test-bucket",
		Key:        "multipart-test.txt",
		UploadID:   createResp.UploadID,
		PartNumber: 2,
		Body:       bytes.NewReader(part2Content),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, part2Resp.ETag)

	// Complete multipart upload
	completeResp, err := be.CompleteMultipartUpload(ctx, &backend.CompleteMultipartUploadRequest{
		Bucket:   "test-bucket",
		Key:      "multipart-test.txt",
		UploadID: createResp.UploadID,
		Parts: []backend.CompletedPart{
			{PartNumber: 1, ETag: part1Resp.ETag},
			{PartNumber: 2, ETag: part2Resp.ETag},
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, completeResp.ETag)
	assert.Contains(t, completeResp.ETag, "-2") // Multipart ETag format

	// Verify file was created
	getResp, err := be.GetObject(ctx, &backend.GetObjectRequest{
		Bucket:     "test-bucket",
		Key:        "multipart-test.txt",
		RangeStart: -1,
		RangeEnd:   -1,
	})
	require.NoError(t, err)
	defer getResp.Body.Close()

	data, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)
	assert.Equal(t, len(part1Content)+len(part2Content), len(data))
}

func TestBackend_AbortMultipartUpload(t *testing.T) {
	be, _, cleanup := testSetup(t)
	defer cleanup()
	ctx := context.Background()

	// Create a multipart upload
	createResp, err := be.CreateMultipartUpload(ctx, &backend.CreateMultipartUploadRequest{
		Bucket: "test-bucket",
		Key:    "abort-test.txt",
	})
	require.NoError(t, err)

	// Upload a part
	_, err = be.UploadPart(ctx, &backend.UploadPartRequest{
		Bucket:     "test-bucket",
		Key:        "abort-test.txt",
		UploadID:   createResp.UploadID,
		PartNumber: 1,
		Body:       strings.NewReader("part content"),
	})
	require.NoError(t, err)

	// Abort the upload
	err = be.AbortMultipartUpload(ctx, "test-bucket", "abort-test.txt", createResp.UploadID)
	assert.NoError(t, err)

	// Verify upload directory was cleaned up
	uploadDir := filepath.Join(be.config.TempDir, createResp.UploadID)
	_, err = os.Stat(uploadDir)
	assert.True(t, os.IsNotExist(err))
}

func TestDeleteEmptyParentDirs(t *testing.T) {
	tmpDir := t.TempDir()
	bucketPath := filepath.Join(tmpDir, "bucket")
	require.NoError(t, os.MkdirAll(bucketPath, 0755))

	// Create nested directory with file
	nestedPath := filepath.Join(bucketPath, "a/b/c/d/file.txt")
	require.NoError(t, os.MkdirAll(filepath.Dir(nestedPath), 0755))
	require.NoError(t, os.WriteFile(nestedPath, []byte("test"), 0644))

	// Remove the file
	require.NoError(t, os.Remove(nestedPath))

	// Clean up empty directories
	err := deleteEmptyParentDirs(nestedPath, bucketPath)
	assert.NoError(t, err)

	// Verify all empty dirs are removed
	_, err = os.Stat(filepath.Join(bucketPath, "a"))
	assert.True(t, os.IsNotExist(err))
}

func TestDeleteEmptyParentDirs_StopsAtNonEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	bucketPath := filepath.Join(tmpDir, "bucket")

	// Create structure with another file
	file1 := filepath.Join(bucketPath, "a/b/file1.txt")
	file2 := filepath.Join(bucketPath, "a/b/c/file2.txt")
	require.NoError(t, os.MkdirAll(filepath.Dir(file1), 0755))
	require.NoError(t, os.MkdirAll(filepath.Dir(file2), 0755))
	require.NoError(t, os.WriteFile(file1, []byte("test1"), 0644))
	require.NoError(t, os.WriteFile(file2, []byte("test2"), 0644))

	// Remove file2
	require.NoError(t, os.Remove(file2))

	// Clean up
	err := deleteEmptyParentDirs(file2, bucketPath)
	assert.NoError(t, err)

	// "c" should be removed but "b" should remain (has file1)
	_, err = os.Stat(filepath.Join(bucketPath, "a/b/c"))
	assert.True(t, os.IsNotExist(err))

	_, err = os.Stat(filepath.Join(bucketPath, "a/b"))
	assert.False(t, os.IsNotExist(err))
}

func TestValidateKey(t *testing.T) {
	tests := []struct {
		key     string
		wantErr bool
	}{
		{"valid-key.txt", false},
		{"path/to/file.txt", false},
		{"unicode-文件.txt", false},
		{"", true},
		{string([]byte{0xff, 0xfe}), true}, // Invalid UTF-8
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			err := validateKey(tt.key)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateBucketName(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{"valid-bucket", false},
		{"bucket123", false},
		{"my.bucket.name", false},
		{"ab", true},                          // Too short
		{"a" + strings.Repeat("b", 64), true}, // Too long
		{"Invalid-Bucket", true},              // Uppercase
		{"bucket_name", true},                 // Underscore
		{"192.168.1.1", true},                 // IP address
		{"xn--bucket", true},                  // Reserved prefix
		{"bucket-s3alias", true},              // Reserved suffix
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBucketName(tt.name)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
