package s3

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/backend/filesystem"
	"github.com/pelletier/go-toml/v2"
)

func (s3 *Config) ReadConfig() (err error) {

	if !filepath.IsAbs(s3.BasePath) {
		dir, err := os.Getwd()
		if err != nil {
			slog.Warn("Error getting working directory", "error", err)
			return err
		}
		s3.BasePath = filepath.Join(dir, s3.BasePath)
	}

	config, err := os.ReadFile(s3.ConfigPath)

	if err != nil {
		errorMsg := fmt.Sprintf("Error reading %s %s", s3.ConfigPath, err)
		slog.Warn("Error reading config file", "error", errorMsg)
		return errors.New(errorMsg)
	}

	err = toml.Unmarshal(config, &s3)

	var validBuckets = []S3_Buckets{}

	// Loop through the buckets, if a directory is relative, add the base path
	for k, b := range s3.Buckets {

		// Check if the bucket name is valid
		err := IsValidBucketName(b.Name)
		if err != nil {
			slog.Warn("Invalid bucket name", "bucket", b.Name, "error", err)
			continue
			//return fmt.Errorf("invalid bucket name: %s", err)
		}

		// Check if the directory is relative
		if b.Type == "fs" && !filepath.IsAbs(b.Pathname) {
			s3.Buckets[k].Pathname = filepath.Join(s3.BasePath, b.Pathname)
		}

		// Check if the directory exists, otherwise create it
		if _, err := os.Stat(s3.Buckets[k].Pathname); os.IsNotExist(err) {
			os.MkdirAll(s3.Buckets[k].Pathname, 0750)
		}

		// Add to our valid buckets
		validBuckets = append(validBuckets, s3.Buckets[k])

	}

	if err != nil {
		errorMsg := fmt.Sprintf("Error parsing %s %s", s3.ConfigPath, err)
		slog.Warn("Error parsing config file", "error", errorMsg)
		return errors.New(errorMsg)
	}

	// Replace config with our valid buckets
	s3.Buckets = validBuckets

	return nil
}

func (s3 *Config) BucketConfig(bucket string) (S3_Buckets, error) {

	for _, b := range s3.Buckets {
		if b.Name == bucket {
			return b, nil
		}
	}

	return S3_Buckets{}, errors.New("bucket not found")
}

// ToFilesystemConfig converts s3.Config to filesystem backend config
func (s3 *Config) ToFilesystemConfig() any {
	// Import dynamically to avoid circular imports
	// The actual conversion happens in the routes setup
	return s3
}

// validatePublicBucketPermission checks if the request is allowed for a public bucket
// Returns nil if the request is allowed, otherwise returns an error
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
	var bucket *S3_Buckets
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

// findSlash finds the first / in a string
func findSlash(s string) int {
	for i, c := range s {
		if c == '/' {
			return i
		}
	}
	return -1
}

// createFilesystemBackend creates a filesystem backend from the configuration.
// This is primarily used for testing.
func (s3 *Config) createFilesystemBackend() backend.Backend {
	buckets := make([]filesystem.BucketConfig, len(s3.Buckets))
	for i, b := range s3.Buckets {
		buckets[i] = filesystem.BucketConfig{
			Name:     b.Name,
			Pathname: b.Pathname,
			Region:   b.Region,
			Type:     b.Type,
			Public:   b.Public,
		}
	}

	fsConfig := &filesystem.Config{
		Buckets:   buckets,
		OwnerID:   "predastore",
		OwnerName: "predastore",
	}

	be, err := filesystem.New(fsConfig)
	if err != nil {
		slog.Error("Failed to create filesystem backend", "error", err)
		return nil
	}

	return be
}
