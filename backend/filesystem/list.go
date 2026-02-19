package filesystem

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/mulgadc/predastore/backend"
)

// ListBuckets returns a list of all configured buckets
// ownerID is accepted for interface compatibility but ignored for filesystem backend
func (b *Backend) ListBuckets(ctx context.Context, ownerID string) (*backend.ListBucketsResponse, error) {
	buckets := make([]backend.BucketInfo, 0, len(b.config.Buckets))

	for _, bucket := range b.config.Buckets {
		info, err := os.Stat(bucket.Pathname)
		if err != nil {
			slog.Warn("Could not stat bucket directory", "bucket", bucket.Name, "path", bucket.Pathname, "error", err)
			continue
		}

		buckets = append(buckets, backend.BucketInfo{
			Name:         bucket.Name,
			CreationDate: info.ModTime(),
			Region:       bucket.Region,
		})
	}

	return &backend.ListBucketsResponse{
		Buckets: buckets,
		Owner: backend.OwnerInfo{
			ID:          b.config.OwnerID,
			DisplayName: b.config.OwnerName,
		},
	}, nil
}

// ListObjects returns objects in a bucket with optional prefix filtering
func (b *Backend) ListObjects(ctx context.Context, req *backend.ListObjectsRequest) (*backend.ListObjectsResponse, error) {
	bucket, err := b.getBucket(req.Bucket)
	if err != nil {
		return nil, err
	}

	maxKeys := req.MaxKeys
	if maxKeys <= 0 {
		maxKeys = 1000 // Default S3 limit
	}

	// Determine the directory to list
	pathname := bucket.Pathname
	prefix := req.Prefix

	if prefix != "" {
		if strings.HasSuffix(prefix, "/") {
			// Prefix is a directory
			pathname = filepath.Join(bucket.Pathname, prefix)
		} else {
			// Prefix includes partial filename
			pathname = filepath.Join(bucket.Pathname, filepath.Dir(prefix))
			prefix = filepath.Base(prefix)
		}
	} else {
		prefix = ""
	}

	entries, err := os.ReadDir(pathname)
	if err != nil {
		if os.IsNotExist(err) {
			// Return empty list for non-existent prefix directory
			return &backend.ListObjectsResponse{
				Name:           req.Bucket,
				Prefix:         req.Prefix,
				Delimiter:      req.Delimiter,
				MaxKeys:        maxKeys,
				KeyCount:       0,
				IsTruncated:    false,
				Contents:       []backend.ObjectInfo{},
				CommonPrefixes: []string{},
			}, nil
		}
		return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	}

	objects := make([]backend.ObjectInfo, 0)
	prefixes := make([]string, 0)

	for _, entry := range entries {
		name := entry.Name()

		// Skip hidden files
		if name == "." || name == ".." {
			continue
		}

		// Apply prefix filter
		if prefix != "" && !strings.HasSuffix(req.Prefix, "/") {
			if !strings.HasPrefix(name, prefix) {
				continue
			}
		}

		if entry.IsDir() {
			// Add as common prefix
			prefixes = append(prefixes, name+"/")
		} else {
			info, err := entry.Info()
			if err != nil {
				slog.Warn("Error getting file info", "file", name, "error", err)
				continue
			}

			// URL decode the key
			key, _ := url.PathUnescape(name)

			// Generate ETag
			hash := md5.New()
			hash.Write(fmt.Appendf(nil, "%s/%s:(%s)", req.Bucket, name, info.ModTime().String()))
			etag := hex.EncodeToString(hash.Sum(nil))

			objects = append(objects, backend.ObjectInfo{
				Key:          key,
				Size:         info.Size(),
				LastModified: info.ModTime(),
				ETag:         etag,
				StorageClass: "STANDARD",
			})
		}

		// Check max keys limit
		if len(objects)+len(prefixes) >= maxKeys {
			break
		}
	}

	return &backend.ListObjectsResponse{
		Name:           req.Bucket,
		Prefix:         req.Prefix,
		Delimiter:      req.Delimiter,
		MaxKeys:        maxKeys,
		KeyCount:       len(objects),
		IsTruncated:    len(objects)+len(prefixes) >= maxKeys,
		Contents:       objects,
		CommonPrefixes: prefixes,
	}, nil
}
