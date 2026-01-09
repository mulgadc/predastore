package distributed

import (
	"bytes"
	"context"
	"encoding/gob"
	"strings"
	"time"

	"github.com/mulgadc/predastore/backend"
)

// ARN key format constants
const (
	// Bucket ARN format: arn:aws:s3::<account_id>:<bucket_name>
	// Object ARN format: arn:aws:s3:::<bucket_name>/<key>
	arnBucketPrefix = "arn:aws:s3::"
	arnObjectPrefix = "arn:aws:s3:::"
)

// ListBuckets returns a list of buckets from both config and s3db
// ownerID filters to only show buckets owned by the specified user (empty = all buckets)
func (b *Backend) ListBuckets(ctx context.Context, ownerID string) (*backend.ListBucketsResponse, error) {
	bucketMap := make(map[string]backend.BucketInfo)

	// First add buckets from config (for backward compatibility)
	for _, bucket := range b.buckets {
		// Only include distributed type buckets
		if bucket.Type == "distributed" {
			bucketMap[bucket.Name] = backend.BucketInfo{
				Name:         bucket.Name,
				Region:       bucket.Region,
				CreationDate: time.Now(), // Config buckets don't have stored creation date
			}
		}
	}

	// Then scan s3db for dynamically created buckets
	err := b.globalState.Scan(TableBuckets, nil, func(key, value []byte) error {
		var metadata backend.BucketMetadata
		r := bytes.NewReader(value)
		dec := gob.NewDecoder(r)
		if err := dec.Decode(&metadata); err != nil {
			return nil // Skip invalid entries
		}

		// Filter by owner if ownerID is provided
		if ownerID != "" && metadata.OwnerID != ownerID {
			// Check if bucket is public or if we should show it anyway
			// For now, show all buckets but mark ownership
			// The auth middleware should handle access control
		}

		// Add or update (s3db takes precedence for metadata)
		bucketMap[metadata.Name] = backend.BucketInfo{
			Name:         metadata.Name,
			Region:       metadata.Region,
			CreationDate: metadata.CreationDate,
		}

		return nil
	})

	if err != nil {
		// Log error but don't fail - return what we have from config
	}

	// Convert map to slice
	buckets := make([]backend.BucketInfo, 0, len(bucketMap))
	for _, info := range bucketMap {
		buckets = append(buckets, info)
	}

	displayName := "Predastore"
	if ownerID != "" {
		displayName = ownerID
	}

	return &backend.ListBucketsResponse{
		Owner: backend.OwnerInfo{
			ID:          ownerID,
			DisplayName: displayName,
		},
		Buckets: buckets,
	}, nil
}

// ListObjects returns a list of objects in a bucket by scanning global state
// Objects are stored with ARN key format: arn:aws:s3:::<bucket>/<key>
func (b *Backend) ListObjects(ctx context.Context, req *backend.ListObjectsRequest) (*backend.ListObjectsResponse, error) {
	if req.Bucket == "" {
		return nil, backend.ErrNoSuchBucketError.WithResource(req.Bucket)
	}

	// Check if bucket exists (in config or s3db)
	_, err := b.HeadBucket(ctx, &backend.HeadBucketRequest{Bucket: req.Bucket})
	if err != nil {
		return nil, err
	}

	// Build the ARN prefix for scanning
	// Format: arn:aws:s3:::<bucket>/<prefix>
	scanPrefix := arnObjectPrefix + req.Bucket + "/"
	if req.Prefix != "" {
		scanPrefix += req.Prefix
	}

	// Scan global state for matching keys
	contents := make([]backend.ObjectInfo, 0)
	commonPrefixes := make([]string, 0)
	prefixSet := make(map[string]bool) // To dedupe common prefixes

	err = b.globalState.Scan(TableObjects, []byte(scanPrefix), func(key, value []byte) error {
		keyStr := string(key)

		// Extract the object key from ARN
		// arn:aws:s3:::<bucket>/<key> -> <key>
		arnBucketPrefix := arnObjectPrefix + req.Bucket + "/"
		if !strings.HasPrefix(keyStr, arnBucketPrefix) {
			return nil
		}
		objectKey := strings.TrimPrefix(keyStr, arnBucketPrefix)

		// Handle delimiter for common prefixes (directory-like listing)
		if req.Delimiter != "" {
			// Check if there's a delimiter after the prefix
			afterPrefix := objectKey
			if req.Prefix != "" {
				afterPrefix = strings.TrimPrefix(objectKey, req.Prefix)
			}

			if idx := strings.Index(afterPrefix, req.Delimiter); idx >= 0 {
				// This is a "directory" - add to common prefixes
				prefix := objectKey[:len(req.Prefix)+idx+len(req.Delimiter)]
				if !prefixSet[prefix] {
					prefixSet[prefix] = true
					commonPrefixes = append(commonPrefixes, prefix)
				}
				return nil // Don't add as content
			}
		}

		// Look up object metadata using the objectHash (value) to get size
		var objectSize int64

		if len(value) == 32 {
			// value is the objectHash, look up the full metadata
			metaData, err := b.globalState.Get(TableObjects, value)
			if err == nil && len(metaData) > 0 {
				var objMeta ObjectToShardNodes
				r := bytes.NewReader(metaData)
				dec := gob.NewDecoder(r)
				if err := dec.Decode(&objMeta); err == nil {
					objectSize = objMeta.Size
				}
			}
		}

		// Add as content
		contents = append(contents, backend.ObjectInfo{
			Key:          objectKey,
			LastModified: time.Now(), // TODO: Store actual modification time
			Size:         objectSize,
			StorageClass: "STANDARD",
		})

		return nil
	})

	if err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	}

	maxKeys := req.MaxKeys
	if maxKeys == 0 {
		maxKeys = 1000
	}

	return &backend.ListObjectsResponse{
		Name:           req.Bucket,
		Prefix:         req.Prefix,
		MaxKeys:        maxKeys,
		KeyCount:       len(contents),
		IsTruncated:    false, // TODO: Implement pagination
		Contents:       contents,
		CommonPrefixes: commonPrefixes,
	}, nil
}

// getBucketConfig returns the bucket configuration for a given bucket name
func (b *Backend) getBucketConfig(name string) *BucketConfig {
	for _, bucket := range b.buckets {
		if bucket.Name == name {
			return &bucket
		}
	}
	return nil
}
