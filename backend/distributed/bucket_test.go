package distributed

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/mulgadc/predastore/backend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupBucketTest(t *testing.T) (*Backend, func()) {
	t.Helper()

	tmpDir := t.TempDir()

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
	}

	b, ok := be.(*Backend)
	require.True(t, ok)
	return b, cleanup
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

	t.Run("returns created buckets for same account", func(t *testing.T) {
		_, err := be.CreateBucket(ctx, &backend.CreateBucketRequest{
			Bucket:    "bucket-a",
			Region:    "us-east-1",
			OwnerID:   "AKIAEXAMPLE",
			AccountID: "111111111111",
		})
		require.NoError(t, err)

		_, err = be.CreateBucket(ctx, &backend.CreateBucketRequest{
			Bucket:    "bucket-b",
			Region:    "eu-west-1",
			OwnerID:   "AKIAEXAMPLE",
			AccountID: "111111111111",
		})
		require.NoError(t, err)

		resp, err := be.ListBuckets(ctx, "111111111111")
		require.NoError(t, err)
		assert.Len(t, resp.Buckets, 2)

		bucketNames := make(map[string]bool)
		for _, b := range resp.Buckets {
			bucketNames[b.Name] = true
		}
		assert.True(t, bucketNames["bucket-a"])
		assert.True(t, bucketNames["bucket-b"])
	})

	t.Run("filters by account ID", func(t *testing.T) {
		_, err := be.CreateBucket(ctx, &backend.CreateBucketRequest{
			Bucket:    "acct-b-bucket",
			Region:    "us-east-1",
			OwnerID:   "AKIAOTHER",
			AccountID: "222222222222",
		})
		require.NoError(t, err)

		// Account A should only see its own buckets
		resp, err := be.ListBuckets(ctx, "111111111111")
		require.NoError(t, err)
		for _, b := range resp.Buckets {
			assert.NotEqual(t, "acct-b-bucket", b.Name,
				"Account A should not see Account B's bucket")
		}

		// Account B should only see its own bucket
		resp, err = be.ListBuckets(ctx, "222222222222")
		require.NoError(t, err)
		assert.Len(t, resp.Buckets, 1)
		assert.Equal(t, "acct-b-bucket", resp.Buckets[0].Name)
	})
}

func TestListBuckets_ConfigBucketsNotVisible(t *testing.T) {
	be, cleanup := setupBucketTest(t)
	defer cleanup()
	ctx := context.Background()

	// Simulate config-based buckets (like the internal "predastore" bucket)
	be.buckets = append(be.buckets, BucketConfig{
		Name:   "predastore",
		Region: "us-east-1",
		Type:   "distributed",
	})

	// Config buckets should NOT appear in ListBuckets
	resp, err := be.ListBuckets(ctx, "")
	require.NoError(t, err)
	for _, b := range resp.Buckets {
		assert.NotEqual(t, "predastore", b.Name, "config-based buckets should not appear in ListBuckets")
	}

	// Create a user bucket — it should appear
	_, err = be.CreateBucket(ctx, &backend.CreateBucketRequest{
		Bucket:  "user-bucket",
		Region:  "us-east-1",
		OwnerID: "AKIAEXAMPLE",
	})
	require.NoError(t, err)

	resp, err = be.ListBuckets(ctx, "")
	require.NoError(t, err)
	assert.Len(t, resp.Buckets, 1)
	assert.Equal(t, "user-bucket", resp.Buckets[0].Name)
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
