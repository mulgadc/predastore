package handlers

import (
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/mulgadc/predastore/internal/gate/model"
)

// defaultMaxKeys is the page size S3 reports when the client asks for none.
const defaultMaxKeys = 1000

// ListObjects serves GET /{bucket} (ListObjectsV2). Objects are listed by
// scanning their ARN keys in global state.
func ListObjects(mc MetaClient, cache *BucketCache) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		resource, ok := routedBucket(w, r)
		if !ok {
			return
		}
		bucket := resource.Name
		query := r.URL.Query()

		// Return proper errors for unsupported bucket sub-resource operations
		// that Terraform and other tools may call.
		slog.DebugContext(ctx, "listObjects called", "bucket", bucket, "query", r.URL.RawQuery)
		if query.Has("policy") {
			slog.DebugContext(ctx, "returning NoSuchBucketPolicy for ?policy request", "bucket", bucket)
			WriteS3Error(w, r, http.StatusNotFound, "NoSuchBucketPolicy", "The bucket policy does not exist")
			return
		}
		if query.Has("acl") {
			WriteS3Error(w, r, http.StatusNotImplemented, "NotImplemented", "ACL is not implemented")
			return
		}
		if query.Has("versioning") {
			WriteS3Error(w, r, http.StatusNotImplemented, "NotImplemented", "Versioning is not implemented")
			return
		}

		if bucket == "" {
			HandleError(w, r, model.ErrNoSuchBucketError.WithResource(bucket))
			return
		}
		if err := requireBucket(ctx, mc, cache, bucket); err != nil {
			HandleError(w, r, err)
			return
		}

		prefix := query.Get("prefix")
		delimiter := query.Get("delimiter")

		items, err := metaScan(ctx, mc, model.TableObjects, objectARN(bucket, prefix), 0)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}

		contents := make([]ListObjectsV2_Contents, 0, len(items))
		prefixes := make([]ListObjectsV2_Dir, 0)
		seenPrefix := make(map[string]bool)

		keyPrefix := objectARN(bucket, "")
		for _, item := range items {
			if !strings.HasPrefix(item.Key, keyPrefix) {
				continue
			}
			objectKey := strings.TrimPrefix(item.Key, keyPrefix)

			// A delimiter collapses everything below it into a common prefix, which
			// is how S3 presents a flat keyspace as directories.
			if delimiter != "" {
				afterPrefix := strings.TrimPrefix(objectKey, prefix)
				if idx := strings.Index(afterPrefix, delimiter); idx >= 0 {
					dir := objectKey[:len(prefix)+idx+len(delimiter)]
					if !seenPrefix[dir] {
						seenPrefix[dir] = true
						prefixes = append(prefixes, ListObjectsV2_Dir{Prefix: dir})
					}
					continue
				}
			}

			// The listing row holds the object hash; the size and the write time
			// live with the shard placement it keys.
			var objectSize int64
			var modified time.Time
			if len(item.Value) == 32 {
				if meta, err := metaGet(ctx, mc, model.TableObjects, string(item.Value)); err == nil && len(meta) > 0 {
					if placement, err := DecodePlacement(meta); err == nil {
						objectSize = placement.Size
						modified, _ = placement.ModifiedAt()
					}
				}
			}

			contents = append(contents, ListObjectsV2_Contents{
				Key:          objectKey,
				LastModified: modified,
				Size:         objectSize,
				StorageClass: "STANDARD",
			})
		}

		result := ListObjectsV2{
			Name:           bucket,
			Prefix:         prefix,
			KeyCount:       len(contents),
			MaxKeys:        defaultMaxKeys,
			IsTruncated:    false, // TODO: Implement pagination
			Contents:       &contents,
			CommonPrefixes: &prefixes,
		}

		if err := writeXML(w, http.StatusOK, result); err != nil {
			slog.DebugContext(ctx, "failed to write XML response", "error", err)
		}
	})
}
