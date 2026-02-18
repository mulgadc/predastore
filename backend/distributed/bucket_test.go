package distributed

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mulgadc/predastore/backend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupBucketTest(t *testing.T) (*Backend, func()) {
	t.Helper()

	// Create temp directory for test
	tmpDir, err := os.MkdirTemp("", "bucket-test-*")
	require.NoError(t, err)

	badgerDir := filepath.Join(tmpDir, "badger")
	dataDir := filepath.Join(tmpDir, "data")

	config := &Config{
		BadgerDir:         badgerDir,
		DataDir:           dataDir,
		DataShards:        3,
		ParityShards:      2,
		PartitionCount:    5,
		ReplicationFactor: 100,
		UseQUIC:           false,
	}

	be, err := New(config)
	require.NoError(t, err)

	cleanup := func() {
		be.Close()
		os.RemoveAll(tmpDir)
	}

	return be.(*Backend), cleanup
}

func TestCreateBucket(t *testing.T) {
	be, cleanup := setupBucketTest(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("creates bucket successfully", func(t *testing.T) {
		resp, err := be.CreateBucket(ctx, &backend.CreateBucketRequest{
			Bucket:           "test-bucket",
			Region:           "us-east-1",
			OwnerID:          "AKIAEXAMPLE",
			OwnerDisplayName: "Test User",
		})
		require.NoError(t, err)
		assert.Equal(t, "/test-bucket", resp.Location)

		// Verify bucket exists via HeadBucket
		headResp, err := be.HeadBucket(ctx, &backend.HeadBucketRequest{Bucket: "test-bucket"})
		require.NoError(t, err)
		assert.Equal(t, "us-east-1", headResp.Region)
	})

	t.Run("returns error for invalid bucket name", func(t *testing.T) {
		_, err := be.CreateBucket(ctx, &backend.CreateBucketRequest{
			Bucket:  "A", // Too short and uppercase
			OwnerID: "AKIAEXAMPLE",
		})
		require.Error(t, err)
		s3err, ok := backend.IsS3Error(err)
		require.True(t, ok)
		assert.Equal(t, backend.ErrInvalidBucketName, s3err.Code)
	})

	t.Run("returns BucketAlreadyOwnedByYou for same owner", func(t *testing.T) {
		// Create bucket first
		_, err := be.CreateBucket(ctx, &backend.CreateBucketRequest{
			Bucket:  "owned-bucket",
			OwnerID: "AKIAEXAMPLE",
		})
		require.NoError(t, err)

		// Try to create same bucket with same owner
		_, err = be.CreateBucket(ctx, &backend.CreateBucketRequest{
			Bucket:  "owned-bucket",
			OwnerID: "AKIAEXAMPLE",
		})
		require.Error(t, err)
		s3err, ok := backend.IsS3Error(err)
		require.True(t, ok)
		assert.Equal(t, backend.ErrBucketAlreadyOwnedByYou, s3err.Code)
	})

	t.Run("returns BucketAlreadyExists for different owner", func(t *testing.T) {
		// Create bucket first
		_, err := be.CreateBucket(ctx, &backend.CreateBucketRequest{
			Bucket:  "taken-bucket",
			OwnerID: "AKIAEXAMPLE",
		})
		require.NoError(t, err)

		// Try to create same bucket with different owner
		_, err = be.CreateBucket(ctx, &backend.CreateBucketRequest{
			Bucket:  "taken-bucket",
			OwnerID: "AKIAOTHER",
		})
		require.Error(t, err)
		s3err, ok := backend.IsS3Error(err)
		require.True(t, ok)
		assert.Equal(t, backend.ErrBucketAlreadyExists, s3err.Code)
	})
}

func TestHeadBucket(t *testing.T) {
	be, cleanup := setupBucketTest(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("returns error for non-existent bucket", func(t *testing.T) {
		_, err := be.HeadBucket(ctx, &backend.HeadBucketRequest{Bucket: "non-existent"})
		require.Error(t, err)
		s3err, ok := backend.IsS3Error(err)
		require.True(t, ok)
		assert.Equal(t, backend.ErrNoSuchBucket, s3err.Code)
	})

	t.Run("finds created bucket", func(t *testing.T) {
		// Create bucket
		_, err := be.CreateBucket(ctx, &backend.CreateBucketRequest{
			Bucket:  "new-bucket",
			Region:  "ap-southeast-2",
			OwnerID: "AKIAEXAMPLE",
		})
		require.NoError(t, err)

		// HeadBucket should find it
		resp, err := be.HeadBucket(ctx, &backend.HeadBucketRequest{Bucket: "new-bucket"})
		require.NoError(t, err)
		assert.Equal(t, "ap-southeast-2", resp.Region)
		assert.Equal(t, "new-bucket", resp.Name)
	})
}

func TestDeleteBucket(t *testing.T) {
	be, cleanup := setupBucketTest(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("deletes empty bucket", func(t *testing.T) {
		// Create bucket
		_, err := be.CreateBucket(ctx, &backend.CreateBucketRequest{
			Bucket:  "to-delete",
			OwnerID: "AKIAEXAMPLE",
		})
		require.NoError(t, err)

		// Delete it
		err = be.DeleteBucket(ctx, &backend.DeleteBucketRequest{
			Bucket:  "to-delete",
			OwnerID: "AKIAEXAMPLE",
		})
		require.NoError(t, err)

		// Verify it's gone
		_, err = be.HeadBucket(ctx, &backend.HeadBucketRequest{Bucket: "to-delete"})
		require.Error(t, err)
	})

	t.Run("returns error for non-existent bucket", func(t *testing.T) {
		err := be.DeleteBucket(ctx, &backend.DeleteBucketRequest{
			Bucket:  "non-existent",
			OwnerID: "AKIAEXAMPLE",
		})
		require.Error(t, err)
		s3err, ok := backend.IsS3Error(err)
		require.True(t, ok)
		assert.Equal(t, backend.ErrNoSuchBucket, s3err.Code)
	})
}

func TestListBuckets(t *testing.T) {
	be, cleanup := setupBucketTest(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("returns empty list initially", func(t *testing.T) {
		resp, err := be.ListBuckets(ctx, "")
		require.NoError(t, err)
		assert.Empty(t, resp.Buckets)
	})

	t.Run("returns created buckets", func(t *testing.T) {
		// Create buckets
		_, err := be.CreateBucket(ctx, &backend.CreateBucketRequest{
			Bucket:  "bucket-a",
			Region:  "us-east-1",
			OwnerID: "AKIAEXAMPLE",
		})
		require.NoError(t, err)

		_, err = be.CreateBucket(ctx, &backend.CreateBucketRequest{
			Bucket:  "bucket-b",
			Region:  "eu-west-1",
			OwnerID: "AKIAEXAMPLE",
		})
		require.NoError(t, err)

		// List buckets
		resp, err := be.ListBuckets(ctx, "AKIAEXAMPLE")
		require.NoError(t, err)
		assert.Len(t, resp.Buckets, 2)

		// Verify buckets are in list
		bucketNames := make(map[string]bool)
		for _, b := range resp.Buckets {
			bucketNames[b.Name] = true
		}
		assert.True(t, bucketNames["bucket-a"])
		assert.True(t, bucketNames["bucket-b"])
	})
}

func TestBucketNameValidation(t *testing.T) {
	testCases := []struct {
		name      string
		bucket    string
		wantError bool
	}{
		{"valid simple", "my-bucket", false},
		{"valid with numbers", "bucket123", false},
		{"valid with periods", "my.bucket.name", false},
		{"too short", "ab", true},
		{"too long", "a-very-long-bucket-name-that-exceeds-the-maximum-length-of-63-chars", true},
		{"uppercase", "MyBucket", true},
		{"starts with hyphen", "-bucket", true},
		{"ends with hyphen", "bucket-", true},
		{"consecutive periods", "my..bucket", true},
		{"IP address", "192.168.1.1", true},
		{"xn-- prefix", "xn--bucket", true},
		{"sthree- prefix", "sthree-bucket", true},
		{"-s3alias suffix", "bucket-s3alias", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := backend.IsValidBucketName(tc.bucket)
			if tc.wantError {
				assert.Error(t, err, "expected error for bucket name: %s", tc.bucket)
			} else {
				assert.NoError(t, err, "unexpected error for bucket name: %s", tc.bucket)
			}
		})
	}
}
