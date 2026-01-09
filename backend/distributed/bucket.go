package distributed

import (
	"bytes"
	"context"
	"encoding/gob"
	"strings"
	"time"

	"github.com/mulgadc/predastore/backend"
)

// CreateBucket creates a new bucket in the distributed store
func (b *Backend) CreateBucket(ctx context.Context, req *backend.CreateBucketRequest) (*backend.CreateBucketResponse, error) {
	// Validate bucket name
	if err := backend.IsValidBucketName(req.Bucket); err != nil {
		return nil, backend.ErrInvalidBucketNameError.WithResource(req.Bucket)
	}

	// Check if bucket already exists
	exists, ownerID, err := b.bucketExists(req.Bucket)
	if err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	}

	if exists {
		// If the bucket is owned by the same user, return BucketAlreadyOwnedByYou
		if ownerID == req.OwnerID {
			return nil, backend.ErrBucketAlreadyOwnedByYouError.WithResource(req.Bucket)
		}
		// If owned by someone else, return BucketAlreadyExists
		return nil, backend.ErrBucketAlreadyExistsError.WithResource(req.Bucket)
	}

	// Determine region
	region := req.Region
	if region == "" {
		region = "us-east-1"
	}

	// Create bucket metadata
	metadata := backend.BucketMetadata{
		Name:         req.Bucket,
		Region:       region,
		OwnerID:      req.OwnerID,
		OwnerDisplay: req.OwnerDisplayName,
		CreationDate: time.Now().UTC(),
		Public:       false,
		ObjectLock:   req.ObjectLockEnabled,
		Versioning:   "",
	}

	// Serialize metadata
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(&metadata); err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, "failed to encode bucket metadata: "+err.Error(), 500)
	}

	// Store in s3db
	if err := b.globalState.Set(TableBuckets, []byte(req.Bucket), buf.Bytes()); err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, "failed to store bucket: "+err.Error(), 500)
	}

	// Also add to local bucket cache for immediate availability
	b.addBucketToCache(req.Bucket, region, false)

	return &backend.CreateBucketResponse{
		Location: "/" + req.Bucket,
	}, nil
}

// DeleteBucket deletes a bucket from the distributed store
func (b *Backend) DeleteBucket(ctx context.Context, req *backend.DeleteBucketRequest) error {
	// Check if bucket exists and get owner
	exists, ownerID, err := b.bucketExists(req.Bucket)
	if err != nil {
		return backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	}

	if !exists {
		return backend.ErrNoSuchBucketError.WithResource(req.Bucket)
	}

	// Verify ownership
	if req.OwnerID != "" && ownerID != req.OwnerID {
		return backend.ErrAccessDeniedError.WithResource(req.Bucket)
	}

	// Check if bucket is empty (scan for any objects with this bucket prefix)
	arnPrefix := arnObjectPrefix + req.Bucket + "/"
	hasObjects := false
	err = b.globalState.Scan(TableObjects, []byte(arnPrefix), func(key, value []byte) error {
		hasObjects = true
		return nil // Stop scanning after finding first object
	})
	if err != nil {
		return backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	}

	if hasObjects {
		return backend.ErrBucketNotEmptyError.WithResource(req.Bucket)
	}

	// Delete bucket from s3db
	if err := b.globalState.Delete(TableBuckets, []byte(req.Bucket)); err != nil {
		return backend.NewS3Error(backend.ErrInternalError, "failed to delete bucket: "+err.Error(), 500)
	}

	// Remove from local cache
	b.removeBucketFromCache(req.Bucket)

	return nil
}

// HeadBucket checks if a bucket exists
func (b *Backend) HeadBucket(ctx context.Context, req *backend.HeadBucketRequest) (*backend.HeadBucketResponse, error) {
	// First check local config (for backward compatibility with configured buckets)
	for _, bucket := range b.buckets {
		if bucket.Name == req.Bucket {
			return &backend.HeadBucketResponse{
				Region: bucket.Region,
				Name:   bucket.Name,
			}, nil
		}
	}

	// Then check s3db for dynamically created buckets
	data, err := b.globalState.Get(TableBuckets, []byte(req.Bucket))
	if err != nil {
		// Check if it's a "not found" type error
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "KeyNotFound") {
			return nil, backend.ErrNoSuchBucketError.WithResource(req.Bucket)
		}
		return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	}

	if len(data) == 0 {
		return nil, backend.ErrNoSuchBucketError.WithResource(req.Bucket)
	}

	// Decode metadata
	var metadata backend.BucketMetadata
	r := bytes.NewReader(data)
	dec := gob.NewDecoder(r)
	if err := dec.Decode(&metadata); err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, "failed to decode bucket metadata: "+err.Error(), 500)
	}

	return &backend.HeadBucketResponse{
		Region: metadata.Region,
		Name:   metadata.Name,
	}, nil
}

// bucketExists checks if a bucket exists and returns the owner ID
func (b *Backend) bucketExists(bucket string) (exists bool, ownerID string, err error) {
	// Check s3db first (authoritative source with owner info)
	data, err := b.globalState.Get(TableBuckets, []byte(bucket))
	if err != nil {
		if !strings.Contains(err.Error(), "not found") && !strings.Contains(err.Error(), "KeyNotFound") {
			return false, "", err
		}
		// Not found in s3db, check local config as fallback
	} else if len(data) > 0 {
		// Found in s3db - decode to get owner
		var metadata backend.BucketMetadata
		r := bytes.NewReader(data)
		dec := gob.NewDecoder(r)
		if err := dec.Decode(&metadata); err != nil {
			return false, "", err
		}
		return true, metadata.OwnerID, nil
	}

	// Fallback: Check local config (for backward compatibility with config-defined buckets)
	for _, bc := range b.buckets {
		if bc.Name == bucket {
			return true, "", nil // Config buckets don't have an owner ID stored
		}
	}

	return false, "", nil
}

// addBucketToCache adds a bucket to the local cache for immediate availability
func (b *Backend) addBucketToCache(name, region string, public bool) {
	b.buckets = append(b.buckets, BucketConfig{
		Name:   name,
		Region: region,
		Type:   "distributed",
		Public: public,
	})
}

// removeBucketFromCache removes a bucket from the local cache
func (b *Backend) removeBucketFromCache(name string) {
	newBuckets := make([]BucketConfig, 0, len(b.buckets))
	for _, bc := range b.buckets {
		if bc.Name != name {
			newBuckets = append(newBuckets, bc)
		}
	}
	b.buckets = newBuckets
}

// GetBucketMetadata retrieves bucket metadata from s3db
func (b *Backend) GetBucketMetadata(bucket string) (*backend.BucketMetadata, error) {
	data, err := b.globalState.Get(TableBuckets, []byte(bucket))
	if err != nil {
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "KeyNotFound") {
			return nil, backend.ErrNoSuchBucketError.WithResource(bucket)
		}
		return nil, err
	}

	if len(data) == 0 {
		return nil, backend.ErrNoSuchBucketError.WithResource(bucket)
	}

	var metadata backend.BucketMetadata
	r := bytes.NewReader(data)
	dec := gob.NewDecoder(r)
	if err := dec.Decode(&metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}
