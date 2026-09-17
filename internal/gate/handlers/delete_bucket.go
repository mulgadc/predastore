package handlers

import (
	"log/slog"
	"net/http"

	"github.com/mulgadc/predastore/internal/gate/model"
)

// DeleteBucket serves DELETE /{bucket}.
func DeleteBucket(mc MetaClient, cache *BucketCache) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		resource, ok := routedBucket(w, r)
		if !ok {
			return
		}
		bucket := resource.Name

		// DELETE /{bucket}?policy — no-op, bucket policies are not supported
		if r.URL.Query().Has("policy") {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// A config-declared bucket is part of the deployment, not tenant data,
		// and is shared by every account. Nothing may delete one — not even a
		// service credential, which the ownership check waves through.
		if cache.isDeclared(bucket) {
			HandleError(w, r, model.ErrAccessDeniedError.WithResource(bucket))
			return
		}

		exists, _, err := bucketExists(ctx, mc, cache, bucket)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}
		if !exists {
			HandleError(w, r, model.ErrNoSuchBucketError.WithResource(bucket))
			return
		}

		// Who may delete this bucket was already decided by the auth
		// middleware, which compares accounts and honours a service
		// credential. Re-deciding it here on the creating access key would
		// mean only one user in an account could ever remove a bucket.

		// One object is enough to reject the delete, so the scan stops at the first.
		objects, err := metaScan(ctx, mc, model.TableObjects, objectARN(bucket, ""), 1)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}
		if len(objects) > 0 {
			HandleError(w, r, model.ErrBucketNotEmptyError.WithResource(bucket))
			return
		}

		// A versioned bucket whose every key is hidden behind a delete marker has
		// no listing keys at all, so the scan above reports it empty while it
		// still holds every byte ever written to it.
		versions, err := metaScan(ctx, mc, model.TableObjectVersions, versionBucketPrefix(bucket), 1)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}
		if len(versions) > 0 {
			HandleError(w, r, model.ErrBucketNotEmptyError.WithResource(bucket))
			return
		}

		if err := metaDelete(ctx, mc, model.TableBuckets, bucket); err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "failed to delete bucket: "+err.Error(), 500))
			return
		}

		// Drop the tags after the bucket record, so a failed delete leaves a
		// live bucket with its tags rather than a live bucket that has lost
		// them. A recycled name would otherwise inherit the old set.
		if err := deleteBucketTags(ctx, mc, bucket); err != nil {
			slog.ErrorContext(ctx, "failed to delete bucket tags", "bucket", bucket, "error", err)
		}

		// Same ordering, same reason: a recycled name must not inherit the
		// versioning state of the bucket that held it before.
		if err := metaDelete(ctx, mc, model.TableBucketVersioning, bucket); err != nil {
			slog.ErrorContext(ctx, "failed to delete bucket versioning state", "bucket", bucket, "error", err)
		}

		cache.remove(bucket)

		w.WriteHeader(http.StatusNoContent)
	})
}
