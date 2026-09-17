package handlers

import (
	"context"
	"log/slog"
	"net/http"
	"sort"
	"strings"

	"github.com/mulgadc/predastore/internal/gate/model"
)

// ListObjectVersions serves GET /{bucket}?versions.
//
// Versions and delete markers are separate elements in the response even though
// they interleave in key order, which is how S3 reports them. IsLatest is
// computed from the merged order rather than stored, so it cannot disagree with
// the listing it appears in.
func ListObjectVersions(mc MetaClient, cache *BucketCache) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		resource, ok := routedBucket(w, r)
		if !ok {
			return
		}
		bucket := resource.Name
		query := r.URL.Query()

		if err := requireBucket(ctx, mc, cache, bucket); err != nil {
			HandleError(w, r, err)
			return
		}

		maxKeys, ok := parseMaxKeys(query.Get("max-keys"))
		if !ok {
			WriteS3Error(w, r, http.StatusBadRequest, string(model.ErrInvalidArgument),
				"max-keys must be a non-negative integer")
			return
		}

		prefix := query.Get("prefix")
		keyMarker := query.Get("key-marker")

		keys, err := versionedKeys(ctx, mc, bucket, prefix)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}

		result := ListVersionsResult{
			Name:            bucket,
			Prefix:          prefix,
			KeyMarker:       keyMarker,
			VersionIdMarker: query.Get("version-id-marker"),
			MaxKeys:         maxKeys,
		}

		// Paging is by key, not by version: a key's versions are reported
		// together so IsLatest is decided against the whole set rather than
		// against whatever part of it landed on this page.
		count := 0
		for _, key := range keys {
			if keyMarker != "" && key <= keyMarker {
				continue
			}
			versions, err := keyVersions(ctx, mc, bucket, key)
			if err != nil {
				HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
				return
			}
			if len(versions) == 0 {
				continue
			}
			if count+len(versions) > maxKeys && count > 0 {
				result.IsTruncated = true
				result.NextKeyMarker = result.Versions[len(result.Versions)-1].Key
				break
			}

			appendVersions(&result, key, versions)
			count += len(versions)
		}

		if err := writeXML(w, http.StatusOK, result); err != nil {
			slog.DebugContext(ctx, "failed to write XML response", "error", err)
		}
	})
}

// appendVersions adds one key's versions to a listing, newest first.
func appendVersions(result *ListVersionsResult, key string, versions []VersionRecord) {
	for i, rec := range versions {
		latest := i == 0
		if rec.DeleteMarker {
			result.DeleteMarkers = append(result.DeleteMarkers, DeleteMarkerEntry{
				Key:          key,
				VersionId:    rec.VersionID,
				IsLatest:     latest,
				LastModified: rec.LastModified,
			})
			continue
		}
		result.Versions = append(result.Versions, ObjectVersionEntry{
			Key:          key,
			VersionId:    rec.VersionID,
			IsLatest:     latest,
			LastModified: rec.LastModified,
			ETag:         rec.ETag,
			Size:         rec.Size,
			StorageClass: "STANDARD",
		})
	}
}

// versionedKeys names every key in a bucket that has any version at all, in key
// order.
//
// Two sources have to be merged, because neither is complete on its own: the
// version index misses a key that only ever had its null version, and the
// listing keys miss a key hidden behind a delete marker.
func versionedKeys(ctx context.Context, mc MetaClient, bucket, prefix string) ([]string, error) {
	seen := map[string]struct{}{}

	indexed, err := metaScan(ctx, mc, model.TableObjectVersions, versionBucketPrefix(bucket), 0)
	if err != nil {
		return nil, err
	}
	for _, item := range indexed {
		key, ok := splitVersionIndexKey(bucket, item.Key)
		if ok && strings.HasPrefix(key, prefix) {
			seen[key] = struct{}{}
		}
	}

	listed, err := metaScan(ctx, mc, model.TableObjects, objectARN(bucket, prefix), 0)
	if err != nil {
		return nil, err
	}
	for _, item := range listed {
		key, found := strings.CutPrefix(item.Key, objectARN(bucket, ""))
		if found {
			seen[key] = struct{}{}
		}
	}

	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}
