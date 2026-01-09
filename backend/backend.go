package backend

import (
	"context"
)

// Backend defines the interface for storage backends.
// All methods accept context.Context for cancellation and timeouts.
// This interface is HTTP-layer agnostic - no framework-specific types.
type Backend interface {
	// Object operations
	GetObject(ctx context.Context, req *GetObjectRequest) (*GetObjectResponse, error)
	HeadObject(ctx context.Context, bucket, key string) (*HeadObjectResponse, error)
	PutObject(ctx context.Context, req *PutObjectRequest) (*PutObjectResponse, error)
	DeleteObject(ctx context.Context, req *DeleteObjectRequest) error

	// Bucket operations
	CreateBucket(ctx context.Context, req *CreateBucketRequest) (*CreateBucketResponse, error)
	DeleteBucket(ctx context.Context, req *DeleteBucketRequest) error
	HeadBucket(ctx context.Context, req *HeadBucketRequest) (*HeadBucketResponse, error)
	ListBuckets(ctx context.Context, ownerID string) (*ListBucketsResponse, error)
	ListObjects(ctx context.Context, req *ListObjectsRequest) (*ListObjectsResponse, error)

	// Multipart upload operations
	CreateMultipartUpload(ctx context.Context, req *CreateMultipartUploadRequest) (*CreateMultipartUploadResponse, error)
	UploadPart(ctx context.Context, req *UploadPartRequest) (*UploadPartResponse, error)
	CompleteMultipartUpload(ctx context.Context, req *CompleteMultipartUploadRequest) (*CompleteMultipartUploadResponse, error)
	AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error

	// Backend info
	Type() string
	Close() error
}
