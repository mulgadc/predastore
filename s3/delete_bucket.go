package s3

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/mulgadc/predastore/internal/gateway/model"
)

// deleteBucket serves DELETE /{bucket}.
func (s *HTTP2Server) deleteBucket(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucket := chi.URLParam(r, "bucket")

	// DELETE /{bucket}?policy — no-op, bucket policies are not supported
	if r.URL.Query().Has("policy") {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	ownerID := ""
	if v := ctx.Value(ContextKeyAccessKeyID); v != nil {
		ownerID, _ = v.(string)
	}

	exists, bucketOwner, err := s.bucketExists(bucket)
	if err != nil {
		s.handleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
		return
	}
	if !exists {
		s.handleError(w, r, model.ErrNoSuchBucketError.WithResource(bucket))
		return
	}
	if ownerID != "" && bucketOwner != ownerID {
		s.handleError(w, r, model.ErrAccessDeniedError.WithResource(bucket))
		return
	}

	// One object is enough to reject the delete, so the scan stops at the first.
	objects, err := s.stateScan(model.TableObjects, objectARN(bucket, ""), 1)
	if err != nil {
		s.handleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
		return
	}
	if len(objects) > 0 {
		s.handleError(w, r, model.ErrBucketNotEmptyError.WithResource(bucket))
		return
	}

	if err := s.stateDelete(model.TableBuckets, bucket); err != nil {
		s.handleError(w, r, model.NewS3Error(model.ErrInternalError, "failed to delete bucket: "+err.Error(), 500))
		return
	}

	s.removeBucketFromCache(bucket)

	w.WriteHeader(http.StatusNoContent)
}
