package s3

import (
	"bytes"
	"encoding/gob"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/mulgadc/predastore/internal/gateway/model"
)

// createBucket serves PUT /{bucket}.
func (s *HTTP2Server) createBucket(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucket := chi.URLParam(r, "bucket")

	// PUT /{bucket}?policy — bucket policies are not supported
	if r.URL.Query().Has("policy") {
		s.writeS3Error(w, r, http.StatusNotImplemented, "NotImplemented", "Bucket policy is not implemented")
		return
	}

	ownerID := ""
	if v := ctx.Value(ContextKeyAccessKeyID); v != nil {
		ownerID, _ = v.(string)
	}
	accountID := ""
	if v := ctx.Value(ContextKeyAccountID); v != nil {
		accountID, _ = v.(string)
	}

	region := s.config.Region
	if r.ContentLength > 0 {
		var config CreateBucketConfiguration
		body, _ := io.ReadAll(r.Body)
		if xml.Unmarshal(body, &config) == nil && config.LocationConstraint != "" {
			region = config.LocationConstraint
		}
	}
	if region == "" {
		region = "us-east-1"
	}

	if err := model.IsValidBucketName(bucket); err != nil {
		s.handleError(w, r, model.ErrInvalidBucketNameError.WithResource(bucket))
		return
	}

	// An existing bucket is reported differently depending on who owns it, so
	// the caller can tell "already yours" from "taken by someone else".
	exists, existingOwner, err := s.bucketExists(bucket)
	if err != nil {
		s.handleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
		return
	}
	if exists {
		if existingOwner == ownerID {
			s.handleError(w, r, model.ErrBucketAlreadyOwnedByYouError.WithResource(bucket))
			return
		}
		s.handleError(w, r, model.ErrBucketAlreadyExistsError.WithResource(bucket))
		return
	}

	metadata := model.BucketMetadata{
		Name:         bucket,
		Region:       region,
		OwnerID:      ownerID,
		AccountID:    accountID,
		OwnerDisplay: ownerID,
		CreationDate: time.Now().UTC(),
		Public:       false,
		Versioning:   "",
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&metadata); err != nil {
		s.handleError(w, r, model.NewS3Error(model.ErrInternalError, "failed to encode bucket metadata: "+err.Error(), 500))
		return
	}

	if err := s.statePut(model.TableBuckets, bucket, buf.Bytes()); err != nil {
		s.handleError(w, r, model.NewS3Error(model.ErrInternalError, "failed to store bucket: "+err.Error(), 500))
		return
	}

	s.addBucketToCache(bucket, region, accountID, false)

	w.Header().Set("Location", fmt.Sprintf("http://%s.s3.%s.amazonaws.com/", bucket, region))
	w.WriteHeader(http.StatusOK)
}
