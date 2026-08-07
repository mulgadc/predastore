package backend

import (
	"context"

	"github.com/mulgadc/predastore/internal/gateway/model"
)

// Backend defines the interface for storage backends.
// All methods accept context.Context for cancellation and timeouts.
// This interface is HTTP-layer agnostic - no framework-specific types.
type Backend interface {
	// Object operations
	GetObject(ctx context.Context, req *model.GetObjectRequest) (*model.GetObjectResponse, error)
	HeadObject(ctx context.Context, bucket, key string) (*model.HeadObjectResponse, error)
	PutObject(ctx context.Context, req *model.PutObjectRequest) (*model.PutObjectResponse, error)
	DeleteObject(ctx context.Context, req *model.DeleteObjectRequest) error

	// Bucket operations
	CreateBucket(ctx context.Context, req *model.CreateBucketRequest) (*model.CreateBucketResponse, error)
	DeleteBucket(ctx context.Context, req *model.DeleteBucketRequest) error
	HeadBucket(ctx context.Context, req *model.HeadBucketRequest) (*model.HeadBucketResponse, error)
	ListBuckets(ctx context.Context, accountID string) (*model.ListBucketsResponse, error)
	ListObjects(ctx context.Context, req *model.ListObjectsRequest) (*model.ListObjectsResponse, error)
	GetBucketMetadata(bucket string) (*model.BucketMetadata, error)

	// Multipart upload operations
	CreateMultipartUpload(ctx context.Context, req *model.CreateMultipartUploadRequest) (*model.CreateMultipartUploadResponse, error)
	UploadPart(ctx context.Context, req *model.UploadPartRequest) (*model.UploadPartResponse, error)
	CompleteMultipartUpload(ctx context.Context, req *model.CompleteMultipartUploadRequest) (*model.CompleteMultipartUploadResponse, error)
	AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error

	// Backend info
	Type() string
}
