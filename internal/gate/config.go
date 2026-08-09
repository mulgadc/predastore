package gate

import (
	"errors"
	"path/filepath"

	"github.com/mulgadc/predastore/internal/gate/handlers"
)

// validatePublicBucketPermission checks if the request is allowed for a public bucket
// Returns nil if the request is allowed, otherwise returns an error.
func (s3 *Config) validatePublicBucketPermission(method, path string) error {
	// Extract bucket name from path
	parts := filepath.SplitList(path)
	if len(parts) == 0 {
		return errors.New("invalid path")
	}

	// Remove leading slash and get first component (bucket name)
	cleanPath := path
	if len(path) > 0 && path[0] == '/' {
		cleanPath = path[1:]
	}

	pathParts := filepath.SplitList(cleanPath)
	if len(pathParts) == 0 {
		// Root path (list buckets) - not public
		return errors.New("listing buckets requires authentication")
	}

	// Get bucket name by splitting on /
	bucketName := cleanPath
	if idx := findSlash(cleanPath); idx != -1 {
		bucketName = cleanPath[:idx]
	}

	// Find bucket configuration
	var bucket *handlers.BucketConfig
	for _, b := range s3.Buckets {
		if b.Name == bucketName {
			bucket = &b
			break
		}
	}

	if bucket == nil {
		return errors.New("bucket not found")
	}

	// Check if bucket is public
	if !bucket.Public {
		return errors.New("bucket is not public")
	}

	// For public buckets:
	// - Allow GET operations (read)
	// - Allow HEAD operations (metadata)
	// - Deny PUT, POST, DELETE without auth
	switch method {
	case "GET", "HEAD":
		return nil // Allow public read
	default:
		return errors.New("public buckets only allow read operations")
	}
}

// findSlash finds the first / in a string.
func findSlash(s string) int {
	for i, c := range s {
		if c == '/' {
			return i
		}
	}
	return -1
}
