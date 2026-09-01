package handlers

import (
	"encoding/base64"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/meta"
)

// defaultMaxKeys is the page size S3 reports when the client asks for none, and
// the ceiling it clamps a larger request down to.
const defaultMaxKeys = 1000

// listEntry is one row of a listing: an object, or a common prefix standing in
// for every key collapsed beneath it. Paging works on entries rather than on
// raw keys because a delimiter turns many keys into one row.
type listEntry struct {
	// sortKey orders the listing and is what a continuation token names. It is
	// the object key, or the common prefix including its trailing delimiter.
	sortKey string
	dir     bool
	// hash keys the shard placement holding the object's size. Empty for a
	// common prefix.
	hash []byte
}

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
		startAfter := query.Get("start-after")
		token := query.Get("continuation-token")

		maxKeys, ok := parseMaxKeys(query.Get("max-keys"))
		if !ok {
			WriteS3Error(w, r, http.StatusBadRequest, string(model.ErrInvalidArgument),
				"max-keys must be a non-negative integer")
			return
		}

		// A continuation token supersedes start-after, and both resolve to the
		// same thing: the last entry the client has already seen.
		cursor := startAfter
		if token != "" {
			decoded, err := base64.StdEncoding.DecodeString(token)
			if err != nil {
				WriteS3Error(w, r, http.StatusBadRequest, string(model.ErrInvalidArgument),
					"The continuation token provided is incorrect")
				return
			}
			cursor = string(decoded)
		}

		items, err := metaScan(ctx, mc, model.TableObjects, objectARN(bucket, prefix), 0)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}

		entries := collapse(items, objectARN(bucket, ""), prefix, delimiter)

		// The scan arrives in key order, but MetaClient promises nothing about
		// ordering, and a cursor into an unordered listing drops and repeats
		// keys without saying so.
		sort.Slice(entries, func(i, j int) bool { return entries[i].sortKey < entries[j].sortKey })

		if cursor != "" {
			entries = entries[sort.Search(len(entries), func(i int) bool {
				return entries[i].sortKey > cursor
			}):]
		}

		// max-keys of zero is an empty listing, not a truncated one: there is no
		// entry to name in a token, so a client told it was truncated would
		// either stall or re-request the same empty page forever.
		truncated := maxKeys > 0 && len(entries) > maxKeys
		if len(entries) > maxKeys {
			entries = entries[:maxKeys]
		}

		contents := make([]ListObjectsV2_Contents, 0, len(entries))
		prefixes := make([]ListObjectsV2_Dir, 0)
		for _, entry := range entries {
			if entry.dir {
				prefixes = append(prefixes, ListObjectsV2_Dir{Prefix: entry.sortKey})
				continue
			}

			// The listing row holds the object hash; the size, the write time and
			// the ETag live with the shard placement it keys. Only the page is
			// resolved, so a listing costs the page size, not the bucket size.
			var objectSize int64
			var modified time.Time
			var etag string
			if len(entry.hash) == 32 {
				if row, err := metaGet(ctx, mc, model.TableObjects, string(entry.hash)); err == nil && len(row) > 0 {
					if placement, err := DecodePlacement(row); err == nil {
						objectSize = placement.Size
						modified = placement.ModifiedAt()
						etag, _ = placement.ETag()
					}
				}
			}

			contents = append(contents, ListObjectsV2_Contents{
				Key:          entry.sortKey,
				LastModified: modified,
				ETag:         etag,
				Size:         objectSize,
				StorageClass: "STANDARD",
			})
		}

		result := ListObjectsV2{
			Name:              bucket,
			Prefix:            prefix,
			Delimiter:         delimiter,
			KeyCount:          len(contents) + len(prefixes),
			MaxKeys:           maxKeys,
			IsTruncated:       truncated,
			ContinuationToken: token,
			StartAfter:        startAfter,
			Contents:          &contents,
			CommonPrefixes:    &prefixes,
		}
		if truncated {
			last := entries[len(entries)-1].sortKey
			result.NextContinuationToken = base64.StdEncoding.EncodeToString([]byte(last))
		}

		if err := writeXML(w, http.StatusOK, result); err != nil {
			slog.DebugContext(ctx, "failed to write XML response", "error", err)
		}
	})
}

// collapse turns scanned rows into listing entries, folding everything below a
// delimiter into a single common prefix the way S3 presents a flat keyspace as
// directories.
func collapse(items []meta.Item, keyPrefix, prefix, delimiter string) []listEntry {
	entries := make([]listEntry, 0, len(items))
	seenPrefix := make(map[string]bool)

	for _, item := range items {
		if !strings.HasPrefix(item.Key, keyPrefix) {
			continue
		}
		objectKey := strings.TrimPrefix(item.Key, keyPrefix)

		if delimiter != "" {
			afterPrefix := strings.TrimPrefix(objectKey, prefix)
			if idx := strings.Index(afterPrefix, delimiter); idx >= 0 {
				dir := objectKey[:len(prefix)+idx+len(delimiter)]
				if !seenPrefix[dir] {
					seenPrefix[dir] = true
					entries = append(entries, listEntry{sortKey: dir, dir: true})
				}
				continue
			}
		}

		entries = append(entries, listEntry{sortKey: objectKey, hash: item.Value})
	}
	return entries
}

// parseMaxKeys reads the client's page size. S3 clamps a request above 1000
// rather than rejecting it, but a negative or non-numeric value is an error:
// treating it as unlimited is how an unpaginated listing gets served to a
// client that asked for a page.
func parseMaxKeys(raw string) (int, bool) {
	if raw == "" {
		return defaultMaxKeys, true
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0, false
	}
	return min(n, defaultMaxKeys), true
}
