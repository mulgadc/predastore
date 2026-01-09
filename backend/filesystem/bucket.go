package filesystem

import (
	"context"

	"github.com/mulgadc/predastore/backend"
)

// CreateBucket creates a new bucket in the filesystem backend
// For filesystem backend, this is not supported - buckets must be defined in config
func (b *Backend) CreateBucket(ctx context.Context, req *backend.CreateBucketRequest) (*backend.CreateBucketResponse, error) {
	// Filesystem backend doesn't support dynamic bucket creation
	// Buckets must be pre-configured in the config file
	return nil, backend.NewS3Error(
		backend.ErrAccessDenied,
		"Filesystem backend does not support dynamic bucket creation. Please configure buckets in the config file.",
		403,
	)
}

// DeleteBucket deletes a bucket from the filesystem backend
// For filesystem backend, this is not supported - buckets must be managed via config
func (b *Backend) DeleteBucket(ctx context.Context, req *backend.DeleteBucketRequest) error {
	// Filesystem backend doesn't support dynamic bucket deletion
	// Buckets must be pre-configured in the config file
	return backend.NewS3Error(
		backend.ErrAccessDenied,
		"Filesystem backend does not support dynamic bucket deletion. Please manage buckets in the config file.",
		403,
	)
}

// HeadBucket checks if a bucket exists in the filesystem backend
func (b *Backend) HeadBucket(ctx context.Context, req *backend.HeadBucketRequest) (*backend.HeadBucketResponse, error) {
	bucket, err := b.getBucket(req.Bucket)
	if err != nil {
		return nil, err
	}

	return &backend.HeadBucketResponse{
		Region: bucket.Region,
		Name:   bucket.Name,
	}, nil
}
