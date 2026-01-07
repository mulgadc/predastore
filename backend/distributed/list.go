package distributed

import (
	"context"

	"github.com/mulgadc/predastore/backend"
)

// ListBuckets returns a list of buckets
// Note: Distributed backend does not manage buckets directly - they are logical containers
func (b *Backend) ListBuckets(ctx context.Context) (*backend.ListBucketsResponse, error) {
	// Distributed backend doesn't maintain bucket list - buckets are virtual
	// Return empty list for now
	return &backend.ListBucketsResponse{
		Buckets: []backend.BucketInfo{},
	}, nil
}

// ListObjects returns a list of objects in a bucket
// Note: This requires scanning the Badger DB for objects with matching prefix
func (b *Backend) ListObjects(ctx context.Context, req *backend.ListObjectsRequest) (*backend.ListObjectsResponse, error) {
	if req.Bucket == "" {
		return nil, backend.ErrNoSuchBucketError.WithResource(req.Bucket)
	}

	// Distributed backend object listing is not yet implemented
	// Would need to scan Badger DB keys with bucket prefix
	return &backend.ListObjectsResponse{
		Name:        req.Bucket,
		Prefix:      req.Prefix,
		MaxKeys:     req.MaxKeys,
		IsTruncated: false,
		Contents:    []backend.ObjectInfo{},
	}, nil
}
