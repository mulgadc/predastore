package handlers

import (
	"bytes"
	"encoding/gob"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/mulgadc/bluebottle/pkg/sigv4"
	"github.com/mulgadc/predastore/internal/gate/auth"
	"github.com/mulgadc/predastore/internal/gate/model"
)

// unsupportedBucketWrites names every bucket sub-resource whose PUT predastore
// does not serve, and what a client is told instead. Each is refused rather
// than accepted and echoed back: storing a setting nothing applies would report
// a bucket as encrypted, locked down or expiring when none of that is true.
//
// Matching is by exact parameter name. The AWS SDKs append x-id to ordinary
// requests, so refusing anything unrecognised would break CreateBucket itself.
var unsupportedBucketWrites = []struct {
	param   string
	message string
}{
	{"policy", "Bucket policy is not implemented"},
	{"acl", "Bucket ACLs are not implemented"},
	{"versioning", "Versioning is not implemented"},
	{"encryption", "Bucket encryption configuration is not implemented"},
	{"lifecycle", "Lifecycle configuration is not implemented"},
	{"publicAccessBlock", "Public access block configuration is not implemented"},
	{"ownershipControls", "Bucket ownership controls are not implemented"},
	{"cors", "CORS configuration is not implemented"},
	{"object-lock", "Object Lock configuration is not implemented"},
	{"notification", "Bucket notification configuration is not implemented"},
	{"logging", "Bucket logging is not implemented"},
	{"replication", "Bucket replication is not implemented"},
	{"website", "Bucket website configuration is not implemented"},
	{"accelerate", "Transfer acceleration is not implemented"},
	{"requestPayment", "Requester pays configuration is not implemented"},
	{"analytics", "Bucket analytics configuration is not implemented"},
	{"intelligent-tiering", "Intelligent tiering configuration is not implemented"},
	{"inventory", "Bucket inventory configuration is not implemented"},
	{"metrics", "Bucket metrics configuration is not implemented"},
}

// unsupportedBucketWrite reports whether a bucket PUT names a sub-resource
// predastore does not serve, and what to say about it.
func unsupportedBucketWrite(query url.Values) (string, bool) {
	for _, sub := range unsupportedBucketWrites {
		if query.Has(sub.param) {
			return sub.message, true
		}
	}
	return "", false
}

// CreateBucket serves PUT /{bucket}.
func CreateBucket(mc MetaClient, cache *BucketCache, cfg Config) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		resource, ok := routedBucket(w, r)
		if !ok {
			return
		}
		bucket := resource.Name

		// A sub-resource write is not CreateBucket. Falling through would answer
		// "already yours", which reads as a name clash rather than as the
		// truthful "this setting is not supported".
		if message, unsupported := unsupportedBucketWrite(r.URL.Query()); unsupported {
			WriteS3Error(w, r, http.StatusNotImplemented, "NotImplemented", message)
			return
		}

		ownerID := auth.AccessKeyID(ctx)
		accountID := auth.AccountID(ctx)

		region := cfg.Region
		if r.ContentLength > 0 {
			var config CreateBucketConfiguration
			// The read carries the SigV4 payload check on a streamed body, so discarding
			// its error would apply a location the client never signed for.
			body, err := io.ReadAll(r.Body)
			if err != nil {
				// A rewritten body keeps its own error rather than being reported as a
				// configuration the handler could not parse.
				if errors.Is(err, sigv4.ErrContentSHA256Mismatch) {
					HandleError(w, r, err)
					return
				}

				HandleError(w, r, model.NewS3Error(model.ErrMalformedXML,
					"The bucket configuration could not be read", http.StatusBadRequest))
				return
			}
			if xml.Unmarshal(body, &config) == nil && config.LocationConstraint != "" {
				region = config.LocationConstraint
			}
		}
		if region == "" {
			region = "us-east-1"
		}

		// An existing bucket is reported differently depending on who owns it, so
		// the caller can tell "already yours" from "taken by someone else".
		exists, existingOwner, err := bucketExists(ctx, mc, cache, bucket)
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

		if err := metaPut(ctx, mc, model.TableBuckets, bucket, buf.Bytes()); err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "failed to store bucket: "+err.Error(), 500))
			return
		}

		cache.add(bucket, region, accountID, false)

		w.Header().Set("Location", fmt.Sprintf("http://%s.s3.%s.amazonaws.com/", bucket, region))
		w.WriteHeader(http.StatusOK)
	})
}
