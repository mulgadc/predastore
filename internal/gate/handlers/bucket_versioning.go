package handlers

import (
	"encoding/xml"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/mulgadc/bluebottle/pkg/sigv4"
	"github.com/mulgadc/predastore/internal/gate/model"
)

// maxVersioningBody caps the versioning document read into memory. The document
// carries one status word, so this is generous rather than considered.
const maxVersioningBody = 8 * 1024

// PutBucketVersioning serves PUT /{bucket}?versioning.
//
// Enabling is one way. S3 has no route back to unversioned, and inventing one
// would mean either destroying the versions already held or reporting a bucket
// as unversioned while it still serves them.
func PutBucketVersioning(mc MetaClient, cache *BucketCache) http.Handler {
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

		body, err := io.ReadAll(io.LimitReader(r.Body, maxVersioningBody+1))
		if err != nil {
			// A rewritten body keeps its own error rather than being reported as
			// a document the handler could not parse.
			if errors.Is(err, sigv4.ErrContentSHA256Mismatch) {
				HandleError(w, r, err)
				return
			}
			HandleError(w, r, model.NewS3Error(model.ErrMalformedXML,
				"The versioning configuration could not be read", http.StatusBadRequest))
			return
		}
		if len(body) > maxVersioningBody {
			WriteS3Error(w, r, http.StatusBadRequest, string(model.ErrInvalidArgument),
				"The versioning configuration is too large")
			return
		}

		var doc VersioningConfiguration
		if err := xml.Unmarshal(body, &doc); err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrMalformedXML,
				"The versioning configuration is not well-formed XML", http.StatusBadRequest))
			return
		}

		// An unrecognised status is refused rather than ignored: accepting it and
		// storing nothing would answer 200 to a request to enable versioning and
		// leave the bucket unversioned.
		if doc.Status != VersioningEnabled && doc.Status != VersioningSuspended {
			WriteS3Error(w, r, http.StatusBadRequest, "IllegalVersioningConfigurationException",
				"The versioning status must be Enabled or Suspended")
			return
		}

		if doc.MFADelete != "" {
			WriteS3Error(w, r, http.StatusNotImplemented, "NotImplemented",
				"MFA delete is not implemented")
			return
		}

		if err := setBucketVersioning(ctx, mc, cache, bucket, doc.Status); err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError,
				"failed to store the versioning state: "+err.Error(), 500))
			return
		}

		w.WriteHeader(http.StatusOK)
	})
}

// GetBucketVersioning serves GET /{bucket}?versioning. A bucket that has never
// been versioned reports no status at all, which is distinct from Suspended:
// a suspended bucket still holds and serves the versions it accumulated.
func GetBucketVersioning(mc MetaClient, cache *BucketCache) http.Handler {
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

		status, err := bucketVersioning(ctx, mc, bucket)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError,
				"failed to read the versioning state: "+err.Error(), 500))
			return
		}

		if err := writeXML(w, http.StatusOK, VersioningConfiguration{Status: status}); err != nil {
			slog.DebugContext(ctx, "failed to write XML response", "error", err)
		}
	})
}
