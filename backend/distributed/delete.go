package distributed

import (
	"context"

	"github.com/mulgadc/predastore/backend"
	s3db "github.com/mulgadc/predastore/s3db"
)

// arnObjectPrefixDel is the ARN prefix for object keys in Badger
const arnObjectPrefixDel = "arn:aws:s3:::"

// DeleteObject removes an object from the distributed storage
func (b *Backend) DeleteObject(ctx context.Context, req *backend.DeleteObjectRequest) error {
	if req.Bucket == "" {
		return backend.ErrNoSuchBucketError.WithResource(req.Bucket)
	}
	if req.Key == "" {
		return backend.ErrNoSuchKeyError.WithResource(req.Key)
	}

	objectHash := s3db.GenObjectHash(req.Bucket, req.Key)

	// Check if object exists
	_, err := b.db.Get(objectHash[:])
	if err != nil {
		return backend.ErrNoSuchKeyError.WithResource(req.Key)
	}

	// Delete the object hash metadata from Badger
	// Note: The actual WAL shard data will be cleaned up by WAL compaction
	err = b.db.Delete(objectHash[:])
	if err != nil {
		return backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	}

	// Delete the ARN key from Badger (for listing)
	arnKey := []byte(arnObjectPrefixDel + req.Bucket + "/" + req.Key)
	err = b.db.Delete(arnKey)
	if err != nil {
		// Don't fail if ARN key doesn't exist (backwards compatibility)
		return nil
	}

	return nil
}
