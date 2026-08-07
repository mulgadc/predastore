package s3

import (
	"bytes"
	"encoding/gob"
	"errors"

	"github.com/mulgadc/predastore/internal/gateway/model"
	"github.com/mulgadc/predastore/internal/state"
)

// lookupBucket resolves a bucket the way HeadBucket does: the local cache of
// config-defined buckets first (static, known at startup), then global state
// for buckets created at runtime. Returns NoSuchBucket when neither knows it.
func (s *HTTP2Server) lookupBucket(bucket string) (*model.BucketMetadata, error) {
	for _, b := range s.buckets {
		if b.Name == bucket {
			return &model.BucketMetadata{Name: b.Name, Region: b.Region, AccountID: b.AccountID, Public: b.Public}, nil
		}
	}

	data, err := s.stateGet(model.TableBuckets, bucket)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil, model.ErrNoSuchBucketError.WithResource(bucket)
		}
		return nil, model.NewS3Error(model.ErrInternalError, err.Error(), 500)
	}
	if len(data) == 0 {
		return nil, model.ErrNoSuchBucketError.WithResource(bucket)
	}

	var metadata model.BucketMetadata
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&metadata); err != nil { //nolint:gosec // G709: the input is state this gateway wrote, not client data.
		return nil, model.NewS3Error(model.ErrInternalError, "failed to decode bucket metadata: "+err.Error(), 500)
	}
	return &metadata, nil
}

// requireBucket rejects an object operation against a bucket that does not
// exist, so a write never lands under a bucket no listing would show.
func (s *HTTP2Server) requireBucket(bucket string) error {
	_, err := s.lookupBucket(bucket)
	return err
}

// bucketExists reports whether a bucket exists and who owns it. Global state is
// authoritative because it is the only source carrying an owner; config-defined
// buckets exist but have no owner recorded.
func (s *HTTP2Server) bucketExists(bucket string) (exists bool, ownerID string, err error) {
	data, err := s.stateGet(model.TableBuckets, bucket)
	if err != nil {
		if !errors.Is(err, state.ErrNotFound) {
			return false, "", err
		}
		// Not found in global state, check the config-defined buckets below.
	} else if len(data) > 0 {
		var metadata model.BucketMetadata
		if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&metadata); err != nil { //nolint:gosec // G709: the input is state this gateway wrote, not client data.
			return false, "", err
		}
		return true, metadata.OwnerID, nil
	}

	for _, b := range s.buckets {
		if b.Name == bucket {
			return true, "", nil
		}
	}

	return false, "", nil
}

// getBucketMetadata reads a bucket's metadata straight from global state,
// bypassing the config cache. A typed sentinel rather than substring matching:
// a transient state error whose message coincidentally contained "not found"
// would otherwise be converted into NoSuchBucket and short-circuit the
// bucket-ownership check via the config fallback.
func (s *HTTP2Server) getBucketMetadata(bucket string) (*model.BucketMetadata, error) {
	data, err := s.stateGet(model.TableBuckets, bucket)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil, model.ErrNoSuchBucketError.WithResource(bucket)
		}
		return nil, err
	}

	if len(data) == 0 {
		return nil, model.ErrNoSuchBucketError.WithResource(bucket)
	}

	var metadata model.BucketMetadata
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&metadata); err != nil { //nolint:gosec // G709: the input is state this gateway wrote, not client data.
		return nil, err
	}

	return &metadata, nil
}

// resolveBucketMetadata returns metadata for the named bucket. Config-defined
// buckets (static, known at startup) are checked first to avoid a synchronous
// state round-trip on every authenticated request. Returns nil with no error
// when the bucket is unknown anywhere — the route handler is responsible for
// returning NoSuchBucket so existence is reported consistently.
func (s *HTTP2Server) resolveBucketMetadata(bucket string) (*model.BucketMetadata, error) {
	if b, err := s.config.BucketConfig(bucket); err == nil {
		return &model.BucketMetadata{
			Name:      b.Name,
			Region:    b.Region,
			AccountID: b.AccountID,
			Public:    b.Public,
		}, nil
	}
	if s.globalState == nil {
		return nil, nil
	}
	meta, err := s.getBucketMetadata(bucket)
	if err == nil {
		return meta, nil
	}
	if stateErr, ok := model.IsS3Error(err); ok && stateErr.Code == model.ErrNoSuchBucket {
		return nil, nil
	}
	return nil, err
}

// addBucketToCache makes a freshly created bucket available without waiting
// for a state read.
func (s *HTTP2Server) addBucketToCache(name, region, accountID string, public bool) {
	s.buckets = append(s.buckets, S3_Buckets{
		Name:      name,
		Region:    region,
		Type:      "distributed",
		Public:    public,
		AccountID: accountID,
	})
}

// removeBucketFromCache drops a deleted bucket from the local cache.
func (s *HTTP2Server) removeBucketFromCache(name string) {
	kept := make([]S3_Buckets, 0, len(s.buckets))
	for _, b := range s.buckets {
		if b.Name != name {
			kept = append(kept, b)
		}
	}
	s.buckets = kept
}
