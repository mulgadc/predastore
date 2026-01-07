package filesystem

import (
	"context"
	"log/slog"
	"os"

	"github.com/mulgadc/predastore/backend"
)

// DeleteObject removes an object from the filesystem
func (b *Backend) DeleteObject(ctx context.Context, req *backend.DeleteObjectRequest) error {
	bucket, err := b.getBucket(req.Bucket)
	if err != nil {
		return err
	}

	if err := validateKey(req.Key); err != nil {
		return err
	}

	pathname, err := b.resolvePath(bucket, req.Key)
	if err != nil {
		return err
	}

	// Check if object exists
	if _, err := os.Stat(pathname); os.IsNotExist(err) {
		return backend.ErrNoSuchKeyError.WithResource(req.Key)
	}

	// Remove the file
	if err := os.Remove(pathname); err != nil {
		slog.Error("Error deleting object", "path", pathname, "error", err)
		return backend.NewS3Error(backend.ErrInternalError, "Failed to delete object", 500)
	}

	// Clean up empty parent directories
	if err := deleteEmptyParentDirs(pathname, bucket.Pathname); err != nil {
		slog.Warn("Error cleaning up empty directories", "error", err)
		// Non-fatal error, object was deleted successfully
	}

	slog.Info("Object deleted", "bucket", req.Bucket, "key", req.Key)
	return nil
}
