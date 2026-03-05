package filesystem

import (
	"context"
	"testing"

	"github.com/mulgadc/predastore/backend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateBucket_NotSupported(t *testing.T) {
	be, _, cleanup := testSetup(t)
	defer cleanup()

	resp, err := be.CreateBucket(context.Background(), &backend.CreateBucketRequest{
		Bucket: "new-bucket",
	})
	assert.Nil(t, resp)
	require.Error(t, err)

	var s3err *backend.S3Error
	require.ErrorAs(t, err, &s3err)
	assert.Equal(t, backend.ErrAccessDenied, s3err.Code)
}

func TestDeleteBucket_NotSupported(t *testing.T) {
	be, _, cleanup := testSetup(t)
	defer cleanup()

	err := be.DeleteBucket(context.Background(), &backend.DeleteBucketRequest{
		Bucket: "test-bucket",
	})
	require.Error(t, err)

	var s3err *backend.S3Error
	require.ErrorAs(t, err, &s3err)
	assert.Equal(t, backend.ErrAccessDenied, s3err.Code)
}

func TestHeadBucket_Exists(t *testing.T) {
	be, _, cleanup := testSetup(t)
	defer cleanup()

	resp, err := be.HeadBucket(context.Background(), &backend.HeadBucketRequest{
		Bucket: "test-bucket",
	})
	require.NoError(t, err)
	assert.Equal(t, "test-bucket", resp.Name)
	assert.Equal(t, "us-east-1", resp.Region)
}

func TestHeadBucket_NotFound(t *testing.T) {
	be, _, cleanup := testSetup(t)
	defer cleanup()

	_, err := be.HeadBucket(context.Background(), &backend.HeadBucketRequest{
		Bucket: "nonexistent-bucket",
	})
	require.Error(t, err)
}
