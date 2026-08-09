package handlers

import (
	"bytes"
	"encoding/gob"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/mulgadc/predastore/internal/gate/auth"
	"github.com/mulgadc/predastore/internal/gate/model"
)

// CreateBucket serves PUT /{bucket}.
func CreateBucket(st Meta, cache *BucketCache, cfg Config) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		bucket := chi.URLParam(r, "bucket")

		// PUT /{bucket}?policy — bucket policies are not supported
		if r.URL.Query().Has("policy") {
			WriteS3Error(w, r, http.StatusNotImplemented, "NotImplemented", "Bucket policy is not implemented")
			return
		}

		ownerID := auth.AccessKeyID(ctx)
		accountID := auth.AccountID(ctx)

		region := cfg.Region
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
			HandleError(w, r, model.ErrInvalidBucketNameError.WithResource(bucket))
			return
		}

		// An existing bucket is reported differently depending on who owns it, so
		// the caller can tell "already yours" from "taken by someone else".
		exists, existingOwner, err := bucketExists(st, cache, bucket)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}
		if exists {
			if existingOwner == ownerID {
				HandleError(w, r, model.ErrBucketAlreadyOwnedByYouError.WithResource(bucket))
				return
			}
			HandleError(w, r, model.ErrBucketAlreadyExistsError.WithResource(bucket))
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
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "failed to encode bucket metadata: "+err.Error(), 500))
			return
		}

		if err := metaPut(st, model.TableBuckets, bucket, buf.Bytes()); err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "failed to store bucket: "+err.Error(), 500))
			return
		}

		cache.add(bucket, region, accountID, false)

		w.Header().Set("Location", fmt.Sprintf("http://%s.s3.%s.amazonaws.com/", bucket, region))
		w.WriteHeader(http.StatusOK)
	})
}
