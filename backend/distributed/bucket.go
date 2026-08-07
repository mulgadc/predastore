package distributed

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"time"

	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/internal/gateway/model"
	"github.com/mulgadc/predastore/internal/state"
)

// Compile-time check that *Backend satisfies backend.Backend.
var _ backend.Backend = (*Backend)(nil)

// CreateBucket creates a new bucket in the distributed store.
func (b *Backend) CreateBucket(ctx context.Context, req *model.CreateBucketRequest) (*model.CreateBucketResponse, error) {
	// Validate bucket name
	if err := model.IsValidBucketName(req.Bucket); err != nil {
		return nil, model.ErrInvalidBucketNameError.WithResource(req.Bucket)
	}

	// Check if bucket already exists
	exists, ownerID, err := b.bucketExists(req.Bucket)
	if err != nil {
		return nil, model.NewS3Error(model.ErrInternalError, err.Error(), 500)
	}

	if exists {
		// If the bucket is owned by the same user, return BucketAlreadyOwnedByYou
		if ownerID == req.OwnerID {
			return nil, model.ErrBucketAlreadyOwnedByYouError.WithResource(req.Bucket)
		}
		// If owned by someone else, return BucketAlreadyExists
		return nil, model.ErrBucketAlreadyExistsError.WithResource(req.Bucket)
	}

	// Determine region
	region := req.Region
	if region == "" {
		region = "us-east-1"
	}

	// Create bucket metadata
	metadata := model.BucketMetadata{
		Name:         req.Bucket,
		Region:       region,
		OwnerID:      req.OwnerID,
		AccountID:    req.AccountID,
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
		return nil, model.NewS3Error(model.ErrInternalError, "failed to encode bucket metadata: "+err.Error(), 500)
	}

	// Store in global state
	if err := b.statePut(model.TableBuckets, req.Bucket, buf.Bytes()); err != nil {
		return nil, model.NewS3Error(model.ErrInternalError, "failed to store bucket: "+err.Error(), 500)
	}

	// Also add to local bucket cache for immediate availability
	b.addBucketToCache(req.Bucket, region, false)

	return &model.CreateBucketResponse{
		Location: "/" + req.Bucket,
	}, nil
}

// DeleteBucket deletes a bucket from the distributed store.
func (b *Backend) DeleteBucket(ctx context.Context, req *model.DeleteBucketRequest) error {
	// Check if bucket exists and get owner
	exists, ownerID, err := b.bucketExists(req.Bucket)
	if err != nil {
		return model.NewS3Error(model.ErrInternalError, err.Error(), 500)
	}

	if !exists {
		return model.ErrNoSuchBucketError.WithResource(req.Bucket)
	}

	// Verify ownership
	if req.OwnerID != "" && ownerID != req.OwnerID {
		return model.ErrAccessDeniedError.WithResource(req.Bucket)
	}

	// Check if bucket is empty; one object is enough to reject the delete.
	arnPrefix := arnObjectPrefix + req.Bucket + "/"
	objects, err := b.stateScan(model.TableObjects, arnPrefix, 1)
	if err != nil {
		return model.NewS3Error(model.ErrInternalError, err.Error(), 500)
	}

	if len(objects) > 0 {
		return model.ErrBucketNotEmptyError.WithResource(req.Bucket)
	}

	// Delete bucket from global state
	if err := b.stateDelete(model.TableBuckets, req.Bucket); err != nil {
		return model.NewS3Error(model.ErrInternalError, "failed to delete bucket: "+err.Error(), 500)
	}

	// Remove from local cache
	b.removeBucketFromCache(req.Bucket)

	return nil
}

// HeadBucket checks if a bucket exists.
func (b *Backend) HeadBucket(ctx context.Context, req *model.HeadBucketRequest) (*model.HeadBucketResponse, error) {
	// First check local config (for backward compatibility with configured buckets)
	for _, bucket := range b.buckets {
		if bucket.Name == req.Bucket {
			return &model.HeadBucketResponse{
				Region: bucket.Region,
				Name:   bucket.Name,
			}, nil
		}
	}

	// Then check global state for dynamically created buckets
	data, err := b.stateGet(model.TableBuckets, req.Bucket)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil, model.ErrNoSuchBucketError.WithResource(req.Bucket)
		}
		return nil, model.NewS3Error(model.ErrInternalError, err.Error(), 500)
	}

	if len(data) == 0 {
		return nil, model.ErrNoSuchBucketError.WithResource(req.Bucket)
	}

	// Decode metadata
	var metadata model.BucketMetadata
	r := bytes.NewReader(data)
	dec := gob.NewDecoder(r)
	if err := dec.Decode(&metadata); err != nil {
		return nil, model.NewS3Error(model.ErrInternalError, "failed to decode bucket metadata: "+err.Error(), 500)
	}

	return &model.HeadBucketResponse{
		Region: metadata.Region,
		Name:   metadata.Name,
	}, nil
}

// bucketExists checks if a bucket exists and returns the owner ID.
func (b *Backend) bucketExists(bucket string) (exists bool, ownerID string, err error) {
	// Check global state first (authoritative source with owner info)
	data, err := b.stateGet(model.TableBuckets, bucket)
	if err != nil {
		if !errors.Is(err, state.ErrNotFound) {
			return false, "", err
		}
		// Not found in global state, check local config as fallback
	} else if len(data) > 0 {
		// Found in global state - decode to get owner
		var metadata model.BucketMetadata
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

// addBucketToCache adds a bucket to the local cache for immediate availability.
func (b *Backend) addBucketToCache(name, region string, public bool) {
	b.buckets = append(b.buckets, BucketConfig{
		Name:   name,
		Region: region,
		Type:   "distributed",
		Public: public,
	})
}

// removeBucketFromCache removes a bucket from the local cache.
func (b *Backend) removeBucketFromCache(name string) {
	newBuckets := make([]BucketConfig, 0, len(b.buckets))
	for _, bc := range b.buckets {
		if bc.Name != name {
			newBuckets = append(newBuckets, bc)
		}
	}
	b.buckets = newBuckets
}

// GetBucketMetadata retrieves bucket metadata from global state.
func (b *Backend) GetBucketMetadata(bucket string) (*model.BucketMetadata, error) {
	data, err := b.stateGet(model.TableBuckets, bucket)
	if err != nil {
		// A typed sentinel rather than substring matching — a transient backend
		// error whose message coincidentally contained "not found" would
		// otherwise be silently converted into NoSuchBucket and short-circuit
		// the bucket-ownership check via the config fallback.
		if errors.Is(err, state.ErrNotFound) {
			return nil, model.ErrNoSuchBucketError.WithResource(bucket)
		}
		return nil, err
	}

	if len(data) == 0 {
		return nil, model.ErrNoSuchBucketError.WithResource(bucket)
	}

	var metadata model.BucketMetadata
	r := bytes.NewReader(data)
	dec := gob.NewDecoder(r)
	if err := dec.Decode(&metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}
