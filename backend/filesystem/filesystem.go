package filesystem

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/mulgadc/predastore/backend"
)

// Config holds filesystem backend configuration
type Config struct {
	Buckets   []BucketConfig
	TempDir   string // Directory for multipart uploads (defaults to os.TempDir())
	OwnerID   string
	OwnerName string
}

// BucketConfig defines a bucket configuration
type BucketConfig struct {
	Name     string
	Pathname string
	Region   string
	Type     string
	Public   bool
}

// Backend implements the filesystem storage backend
type Backend struct {
	config    *Config
	bucketMap map[string]*BucketConfig
}

// New creates a new filesystem backend
func New(config any) (backend.Backend, error) {
	cfg, ok := config.(*Config)
	if !ok {
		return nil, errors.New("invalid configuration type for filesystem backend")
	}

	if cfg.TempDir == "" {
		cfg.TempDir = os.TempDir()
	}

	if cfg.OwnerID == "" {
		cfg.OwnerID = "predastore-owner-id"
	}

	if cfg.OwnerName == "" {
		cfg.OwnerName = "predastore"
	}

	b := &Backend{
		config:    cfg,
		bucketMap: make(map[string]*BucketConfig),
	}

	// Build bucket lookup map
	for i := range cfg.Buckets {
		bucket := &cfg.Buckets[i]
		b.bucketMap[bucket.Name] = bucket
	}

	return b, nil
}

// Type returns the backend type identifier
func (b *Backend) Type() string {
	return "filesystem"
}

// Close cleans up any resources
func (b *Backend) Close() error {
	return nil
}

// getBucket returns the bucket configuration or an error
func (b *Backend) getBucket(name string) (*BucketConfig, error) {
	bucket, exists := b.bucketMap[name]
	if !exists {
		return nil, backend.ErrNoSuchBucketError.WithResource(name)
	}
	return bucket, nil
}

// resolvePath safely resolves a key within a bucket path
func (b *Backend) resolvePath(bucket *BucketConfig, key string) (string, error) {
	// Clean the key to prevent path traversal
	cleanKey := filepath.Clean(key)
	if strings.HasPrefix(cleanKey, "..") || filepath.IsAbs(cleanKey) {
		return "", backend.NewS3Error(backend.ErrInvalidKey, "Invalid key path", 400).WithResource(key)
	}

	fullPath := filepath.Join(bucket.Pathname, cleanKey)

	// Ensure the resolved path is still within the bucket
	if !strings.HasPrefix(fullPath, bucket.Pathname) {
		return "", backend.NewS3Error(backend.ErrInvalidKey, "Key resolves outside bucket", 400).WithResource(key)
	}

	return fullPath, nil
}

// validateKey validates an object key
func validateKey(key string) error {
	if key == "" {
		return backend.NewS3Error(backend.ErrInvalidKey, "Key cannot be empty", 400)
	}
	if !utf8.ValidString(key) {
		return backend.NewS3Error(backend.ErrInvalidKey, "Key must be valid UTF-8", 400)
	}
	return nil
}

// validateBucketName validates a bucket name according to S3 rules
func validateBucketName(name string) error {
	if len(name) < 3 {
		return errors.New("bucket name must be at least 3 characters")
	}
	if len(name) > 63 {
		return errors.New("bucket name must be at most 63 characters")
	}

	validBucket := regexp.MustCompile(`^[a-z0-9][a-z0-9.-]*[a-z0-9]$`)
	if !validBucket.MatchString(name) {
		return errors.New("bucket name must consist of lowercase letters, numbers, periods, and hyphens")
	}

	if strings.Contains(name, "..") {
		return errors.New("bucket name cannot contain consecutive periods")
	}

	if regexp.MustCompile(`^\d+\.\d+\.\d+\.\d+$`).MatchString(name) {
		return errors.New("bucket name cannot be formatted as an IP address")
	}

	reservedPrefixes := []string{"xn--", "sthree-", "amzn-s3-demo-"}
	for _, prefix := range reservedPrefixes {
		if strings.HasPrefix(name, prefix) {
			return fmt.Errorf("bucket name cannot start with reserved prefix: %s", prefix)
		}
	}

	reservedSuffixes := []string{"-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3"}
	for _, suffix := range reservedSuffixes {
		if strings.HasSuffix(name, suffix) {
			return fmt.Errorf("bucket name cannot end with reserved suffix: %s", suffix)
		}
	}

	return nil
}

// deleteEmptyParentDirs removes empty parent directories up to stopAt
func deleteEmptyParentDirs(path, stopAt string) error {
	path = filepath.Clean(path)
	stopAt = filepath.Clean(stopAt)
	dir := filepath.Dir(path)

	for dir != stopAt && strings.HasPrefix(dir, stopAt) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}

		if len(entries) > 0 {
			break // Directory not empty
		}

		if err := os.Remove(dir); err != nil {
			return err
		}

		slog.Debug("Removed empty directory", "dir", dir)
		dir = filepath.Dir(dir)
	}

	return nil
}

// AbortMultipartUpload cancels a multipart upload and cleans up parts
func (b *Backend) AbortMultipartUpload(ctx context.Context, bucketName, key, uploadID string) error {
	bucket, err := b.getBucket(bucketName)
	if err != nil {
		return err
	}

	if err := validateKey(key); err != nil {
		return err
	}

	uploadDir := filepath.Join(b.config.TempDir, uploadID)

	// Check if upload exists
	if _, err := os.Stat(uploadDir); os.IsNotExist(err) {
		return backend.ErrNoSuchUploadError.WithResource(uploadID)
	}

	// Remove the upload directory and all parts
	if err := os.RemoveAll(uploadDir); err != nil {
		slog.Error("Failed to abort multipart upload", "uploadID", uploadID, "error", err)
		return backend.NewS3Error(backend.ErrInternalError, "Failed to abort upload", 500)
	}

	slog.Info("Aborted multipart upload", "bucket", bucket.Name, "key", key, "uploadID", uploadID)
	return nil
}
