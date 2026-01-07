package distributed

import (
	"context"

	"github.com/mulgadc/predastore/backend"
)

// CreateMultipartUpload initiates a multipart upload
func (b *Backend) CreateMultipartUpload(ctx context.Context, req *backend.CreateMultipartUploadRequest) (*backend.CreateMultipartUploadResponse, error) {
	// Multipart upload for distributed backend not yet implemented
	return nil, backend.NewS3Error(backend.ErrInternalError, "multipart upload not implemented for distributed backend", 501)
}

// UploadPart uploads a part in a multipart upload
func (b *Backend) UploadPart(ctx context.Context, req *backend.UploadPartRequest) (*backend.UploadPartResponse, error) {
	// Multipart upload for distributed backend not yet implemented
	return nil, backend.NewS3Error(backend.ErrInternalError, "multipart upload not implemented for distributed backend", 501)
}

// CompleteMultipartUpload completes a multipart upload
func (b *Backend) CompleteMultipartUpload(ctx context.Context, req *backend.CompleteMultipartUploadRequest) (*backend.CompleteMultipartUploadResponse, error) {
	// Multipart upload for distributed backend not yet implemented
	return nil, backend.NewS3Error(backend.ErrInternalError, "multipart upload not implemented for distributed backend", 501)
}

// AbortMultipartUpload aborts a multipart upload
func (b *Backend) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	// Multipart upload for distributed backend not yet implemented
	return backend.NewS3Error(backend.ErrInternalError, "multipart upload not implemented for distributed backend", 501)
}
