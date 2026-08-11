package gate

import (
	"errors"
	"net/http"
)

// validatePublicBucketPermission checks if the request is allowed for a public bucket
// Returns nil if the request is allowed, otherwise returns an error.
func (s3 *Config) validatePublicBucketPermission(method, bucket string) error {
	if bucket == "" {
		// Root path (list buckets) - not public
		return errors.New("listing buckets requires authentication")
	}

	cfg, ok := s3.handlerConfig().Find(bucket)
	if !ok {
		return errors.New("bucket not found")
	}

	if !cfg.Public {
		return errors.New("bucket is not public")
	}

	// For public buckets:
	// - Allow GET operations (read)
	// - Allow HEAD operations (metadata)
	// - Deny PUT, POST, DELETE without auth
	switch method {
	case http.MethodGet, http.MethodHead:
		return nil // Allow public read
	default:
		return errors.New("public buckets only allow read operations")
	}
}
