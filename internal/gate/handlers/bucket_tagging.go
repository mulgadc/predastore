package handlers

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/xml"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"

	"github.com/mulgadc/bluebottle/pkg/sigv4"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/meta"
)

// maxTagSetBody caps the tagging document read into memory. S3 allows 50 tags
// of 128/256 characters, so this is several times the largest legitimate body
// and leaves no basis for an allocation a client chooses.
const maxTagSetBody = 64 * 1024

// s3MaxBucketTags is the number of tags S3 accepts on a bucket.
const s3MaxBucketTags = 50

// PutBucketTagging serves PUT /{bucket}?tagging, replacing the bucket's whole
// tag set as S3 does — this is not a merge.
func PutBucketTagging(mc MetaClient, cache *BucketCache) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		resource, ok := routedBucket(w, r)
		if !ok {
			return
		}
		bucket := resource.Name

		if err := requireBucket(ctx, mc, cache, bucket); err != nil {
			HandleError(w, r, err)
			return
		}

		body, err := io.ReadAll(io.LimitReader(r.Body, maxTagSetBody+1))
		if err != nil {
			// A rewritten body keeps its own error rather than being reported as
			// a document the handler could not parse.
			if errors.Is(err, sigv4.ErrContentSHA256Mismatch) {
				HandleError(w, r, err)
				return
			}
			HandleError(w, r, model.NewS3Error(model.ErrMalformedXML,
				"The tagging configuration could not be read", http.StatusBadRequest))
			return
		}
		if len(body) > maxTagSetBody {
			WriteS3Error(w, r, http.StatusBadRequest, string(model.ErrInvalidArgument),
				"The tagging configuration is too large")
			return
		}

		var doc Tagging
		if err := xml.Unmarshal(body, &doc); err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrMalformedXML,
				"The tagging configuration is not well-formed XML", http.StatusBadRequest))
			return
		}

		tags, invalid := tagSetToMap(doc.TagSet)
		if invalid != "" {
			WriteS3Error(w, r, http.StatusBadRequest, "InvalidTag", invalid)
			return
		}

		if err := putBucketTags(ctx, mc, bucket, tags); err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError,
				"failed to store bucket tags: "+err.Error(), 500))
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})
}

// GetBucketTagging serves GET /{bucket}?tagging. An untagged bucket is
// NoSuchTagSet, not an empty set, so a caller can tell "none" from "not read".
func GetBucketTagging(mc MetaClient, cache *BucketCache) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		resource, ok := routedBucket(w, r)
		if !ok {
			return
		}
		bucket := resource.Name

		if err := requireBucket(ctx, mc, cache, bucket); err != nil {
			HandleError(w, r, err)
			return
		}

		tags, err := getBucketTags(ctx, mc, bucket)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError,
				"failed to read bucket tags: "+err.Error(), 500))
			return
		}
		if len(tags) == 0 {
			WriteS3Error(w, r, http.StatusNotFound, "NoSuchTagSet", "The TagSet does not exist")
			return
		}

		if err := writeXML(w, http.StatusOK, Tagging{TagSet: mapToTagSet(tags)}); err != nil {
			slog.DebugContext(ctx, "failed to write XML response", "error", err)
		}
	})
}

// DeleteBucketTagging serves DELETE /{bucket}?tagging. Idempotent: removing the
// tags of an untagged bucket succeeds, as it does on S3.
func DeleteBucketTagging(mc MetaClient, cache *BucketCache) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		resource, ok := routedBucket(w, r)
		if !ok {
			return
		}
		bucket := resource.Name

		if err := requireBucket(ctx, mc, cache, bucket); err != nil {
			HandleError(w, r, err)
			return
		}

		if err := deleteBucketTags(ctx, mc, bucket); err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError,
				"failed to delete bucket tags: "+err.Error(), 500))
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})
}

// tagSetToMap validates a tag set and collapses it to the stored form, naming
// the first offending tag rather than the count so the caller can fix it.
func tagSetToMap(set []Tag) (map[string]string, string) {
	if len(set) > s3MaxBucketTags {
		return nil, "A bucket may carry at most 50 tags"
	}

	tags := make(map[string]string, len(set))
	for _, tag := range set {
		switch {
		case tag.Key == "":
			return nil, "A tag key may not be empty"
		case len(tag.Key) > 128:
			return nil, "Tag key " + tag.Key + " is longer than 128 characters"
		case len(tag.Value) > 256:
			return nil, "The value of tag key " + tag.Key + " is longer than 256 characters"
		case strings.HasPrefix(strings.ToLower(tag.Key), "aws:"):
			return nil, "Tag key " + tag.Key + " uses the reserved aws: prefix"
		}
		if _, dup := tags[tag.Key]; dup {
			return nil, "Tag key " + tag.Key + " appears more than once"
		}
		tags[tag.Key] = tag.Value
	}
	return tags, ""
}

// mapToTagSet renders the stored form back as a tag set. Go randomises map
// iteration, so the order is fixed here rather than left to vary between two
// reads of an unchanged bucket.
func mapToTagSet(tags map[string]string) []Tag {
	keys := make([]string, 0, len(tags))
	for key := range tags {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	set := make([]Tag, 0, len(keys))
	for _, key := range keys {
		set = append(set, Tag{Key: key, Value: tags[key]})
	}
	return set
}

// putBucketTags replaces a bucket's stored tag set. An empty set removes the
// entry, so an untagged bucket is one state rather than two.
func putBucketTags(ctx context.Context, mc MetaClient, bucket string, tags map[string]string) error {
	if len(tags) == 0 {
		return deleteBucketTags(ctx, mc, bucket)
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&tags); err != nil {
		return err
	}
	return metaPut(ctx, mc, model.TableBucketTags, bucket, buf.Bytes())
}

// getBucketTags reads a bucket's stored tag set, reporting an absent entry as
// an empty set rather than an error.
func getBucketTags(ctx context.Context, mc MetaClient, bucket string) (map[string]string, error) {
	data, err := metaGet(ctx, mc, model.TableBucketTags, bucket)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}

	var tags map[string]string
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&tags); err != nil {
		return nil, err
	}
	return tags, nil
}

// deleteBucketTags drops a bucket's tag entry. A missing entry is not an error:
// DeleteBucketTagging is idempotent, and DeleteBucket calls this for every
// bucket whether it was tagged or not.
func deleteBucketTags(ctx context.Context, mc MetaClient, bucket string) error {
	if err := metaDelete(ctx, mc, model.TableBucketTags, bucket); err != nil && !errors.Is(err, meta.ErrNotFound) {
		return err
	}
	return nil
}
