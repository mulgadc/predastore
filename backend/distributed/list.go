package distributed

import (
	"bytes"
	"context"
	"encoding/gob"
	"log/slog"
	"strings"
	"time"

	"github.com/mulgadc/predastore/internal/gateway/model"
)

// ARN key format constants.
const (
	// Bucket ARN format: arn:aws:s3::<account_id>:<bucket_name>
	// Object ARN format: arn:aws:s3:::<bucket_name>/<key>.
	arnBucketPrefix = "arn:aws:s3::"
	arnObjectPrefix = "arn:aws:s3:::"
)

// ListBuckets returns a list of buckets from global state filtered by account.
func (b *Backend) ListBuckets(ctx context.Context, accountID string) (*model.ListBucketsResponse, error) {
	bucketMap := make(map[string]model.BucketInfo)

	// Scan global state for dynamically created buckets
	items, err := b.stateScan(model.TableBuckets, "", 0)
	if err != nil {
		return nil, model.NewS3Error(model.ErrInternalError, "failed to list buckets: "+err.Error(), 500)
	}

	for _, item := range items {
		var metadata model.BucketMetadata
		dec := gob.NewDecoder(bytes.NewReader(item.Value))
		if err := dec.Decode(&metadata); err != nil {
			slog.Warn("Skipping corrupt bucket entry during scan", "key", item.Key, "error", err)
			continue
		}

		// Filter by account
		if accountID != "" && metadata.AccountID != accountID {
			continue
		}

		bucketMap[metadata.Name] = model.BucketInfo{
			Name:         metadata.Name,
			Region:       metadata.Region,
			CreationDate: metadata.CreationDate,
		}
	}

	// Convert to slice
	buckets := make([]model.BucketInfo, 0, len(bucketMap))
	for _, info := range bucketMap {
		buckets = append(buckets, info)
	}

	displayName := "Predastore"
	if accountID != "" {
		displayName = accountID
	}

	return &model.ListBucketsResponse{
		Owner: model.OwnerInfo{
			ID:          accountID,
			DisplayName: displayName,
		},
		Buckets: buckets,
	}, nil
}

// ListObjects returns a list of objects in a bucket by scanning global state
// Objects are stored with ARN key format: arn:aws:s3:::<bucket>/<key>.
func (b *Backend) ListObjects(ctx context.Context, req *model.ListObjectsRequest) (*model.ListObjectsResponse, error) {
	if req.Bucket == "" {
		return nil, model.ErrNoSuchBucketError.WithResource(req.Bucket)
	}

	// Check if bucket exists (in config or global state)
	_, err := b.HeadBucket(ctx, &model.HeadBucketRequest{Bucket: req.Bucket})
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
	contents := make([]model.ObjectInfo, 0)
	commonPrefixes := make([]string, 0)
	prefixSet := make(map[string]bool) // To dedupe common prefixes

	items, err := b.stateScan(model.TableObjects, scanPrefix, 0)
	if err != nil {
		return nil, model.NewS3Error(model.ErrInternalError, err.Error(), 500)
	}

	for _, item := range items {
		// Extract the object key from ARN
		// arn:aws:s3:::<bucket>/<key> -> <key>
		arnBucketPrefix := arnObjectPrefix + req.Bucket + "/"
		if !strings.HasPrefix(item.Key, arnBucketPrefix) {
			continue
		}
		objectKey := strings.TrimPrefix(item.Key, arnBucketPrefix)

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
				continue // Don't add as content
			}
		}

		// Look up object metadata using the objectHash (value) to get size
		var objectSize int64

		if len(item.Value) == 32 {
			// value is the objectHash, look up the full metadata
			metaData, err := b.stateGet(model.TableObjects, string(item.Value))
			if err == nil && len(metaData) > 0 {
				var objMeta ObjectToShardNodes
				dec := gob.NewDecoder(bytes.NewReader(metaData))
				if err := dec.Decode(&objMeta); err == nil {
					objectSize = objMeta.Size
				}
			}
		}

		// Add as content
		contents = append(contents, model.ObjectInfo{
			Key:          objectKey,
			LastModified: time.Now(), // TODO: Store actual modification time
			Size:         objectSize,
			StorageClass: "STANDARD",
		})
	}

	maxKeys := req.MaxKeys
	if maxKeys == 0 {
		maxKeys = 1000
	}

	return &model.ListObjectsResponse{
		Name:           req.Bucket,
		Prefix:         req.Prefix,
		MaxKeys:        maxKeys,
		KeyCount:       len(contents),
		IsTruncated:    false, // TODO: Implement pagination
		Contents:       contents,
		CommonPrefixes: commonPrefixes,
	}, nil
}
