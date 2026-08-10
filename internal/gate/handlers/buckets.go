package handlers

import (
	"bytes"
	"encoding/gob"
	"errors"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/meta"
)

// BucketCache is the gate's view of which buckets exist without a state
// round-trip: the config-defined set at startup, plus buckets created since.
// The handlers and the auth middleware share one instance, so a bucket created
// by a request is visible to the next request's ownership check.
type BucketCache struct {
	entries []BucketConfig
}

// NewBucketCache seeds the cache from the config-defined buckets.
func NewBucketCache(configured []BucketConfig) *BucketCache {
	return &BucketCache{entries: append([]BucketConfig(nil), configured...)}
}

// find returns the cached entry for a bucket.
func (c *BucketCache) find(bucket string) (BucketConfig, bool) {
	for _, b := range c.entries {
		if b.Name == bucket {
			return b, true
		}
	}
	return BucketConfig{}, false
}

// add makes a freshly created bucket available without waiting for a state read.
func (c *BucketCache) add(name, region, accountID string, public bool) {
	c.entries = append(c.entries, BucketConfig{
		Name:      name,
		Region:    region,
		Public:    public,
		AccountID: accountID,
	})
}

// remove drops a deleted bucket from the cache.
func (c *BucketCache) remove(name string) {
	kept := make([]BucketConfig, 0, len(c.entries))
	for _, b := range c.entries {
		if b.Name != name {
			kept = append(kept, b)
		}
	}
	c.entries = kept
}

// lookupBucket resolves a bucket the way HeadBucket does: the cache first
// (config-defined buckets are static and known at startup), then global state
// for buckets created at runtime. Returns NoSuchBucket when neither knows it.
func lookupBucket(st Meta, cache *BucketCache, bucket string) (*model.BucketMetadata, error) {
	if b, ok := cache.find(bucket); ok {
		return &model.BucketMetadata{Name: b.Name, Region: b.Region, AccountID: b.AccountID, Public: b.Public}, nil
	}

	data, err := metaGet(st, model.TableBuckets, bucket)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			return nil, model.ErrNoSuchBucketError.WithResource(bucket)
		}
		return nil, model.NewS3Error(model.ErrInternalError, err.Error(), 500)
	}
	if len(data) == 0 {
		return nil, model.ErrNoSuchBucketError.WithResource(bucket)
	}

	var metadata model.BucketMetadata
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&metadata); err != nil { //nolint:gosec // G709: the input is state this gate wrote, not client data.
		return nil, model.NewS3Error(model.ErrInternalError, "failed to decode bucket metadata: "+err.Error(), 500)
	}
	return &metadata, nil
}

// requireBucket rejects an object operation against a bucket that does not
// exist, so a write never lands under a bucket no listing would show.
func requireBucket(st Meta, cache *BucketCache, bucket string) error {
	_, err := lookupBucket(st, cache, bucket)
	return err
}

// bucketExists reports whether a bucket exists and who owns it. Global state is
// authoritative because it is the only source carrying an owner; config-defined
// buckets exist but have no owner recorded.
func bucketExists(st Meta, cache *BucketCache, bucket string) (exists bool, ownerID string, err error) {
	data, err := metaGet(st, model.TableBuckets, bucket)
	if err != nil {
		if !errors.Is(err, meta.ErrNotFound) {
			return false, "", err
		}
		// Not found in global state, check the cached buckets below.
	} else if len(data) > 0 {
		var metadata model.BucketMetadata
		if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&metadata); err != nil { //nolint:gosec // G709: the input is state this gate wrote, not client data.
			return false, "", err
		}
		return true, metadata.OwnerID, nil
	}

	if _, ok := cache.find(bucket); ok {
		return true, "", nil
	}

	return false, "", nil
}

// getBucketMetadata reads a bucket's metadata straight from global state,
// bypassing the config cache. A typed sentinel rather than substring matching:
// a transient state error whose message coincidentally contained "not found"
// would otherwise be converted into NoSuchBucket and short-circuit the
// bucket-ownership check via the config fallback.
func getBucketMetadata(st Meta, bucket string) (*model.BucketMetadata, error) {
	data, err := metaGet(st, model.TableBuckets, bucket)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			return nil, model.ErrNoSuchBucketError.WithResource(bucket)
		}
		return nil, err
	}

	if len(data) == 0 {
		return nil, model.ErrNoSuchBucketError.WithResource(bucket)
	}

	var metadata model.BucketMetadata
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}

// ResolveBucketMetadata returns metadata for the named bucket. Config-defined
// buckets (static, known at startup) are checked first to avoid a synchronous
// state round-trip on every authenticated request. Returns nil with no error
// when the bucket is unknown anywhere — the route handler is responsible for
// returning NoSuchBucket so existence is reported consistently.
//
// The gate's auth middleware calls this for its cross-account ownership
// check, which is why bucket resolution lives with the handlers rather than
// inside one of them.
func ResolveBucketMetadata(st Meta, cfg Config, bucket string) (*model.BucketMetadata, error) {
	if b, ok := cfg.Find(bucket); ok {
		return &model.BucketMetadata{
			Name:      b.Name,
			Region:    b.Region,
			AccountID: b.AccountID,
			Public:    b.Public,
		}, nil
	}
	if st == nil {
		return nil, nil
	}
	meta, err := getBucketMetadata(st, bucket)
	if err == nil {
		return meta, nil
	}
	if stateErr, ok := model.IsS3Error(err); ok && stateErr.Code == model.ErrNoSuchBucket {
		return nil, nil
	}
	return nil, err
}
