package handlers

import (
	"bytes"
	"encoding/gob"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/mulgadc/predastore/internal/gate/chunked"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/gate/placement"
)

// PutObject serves PUT /{bucket}/{key}: the body is erasure coded across the
// blob nodes as it is read, and its placement recorded in global state under
// both the object hash and the listing key.
func PutObject(mc MetaClient, bc BlobClient, ring *placement.Ring, cache *BucketCache, cfg Config) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		resource, ok := routedObject(w, r)
		if !ok {
			return
		}
		bucket, key := resource.Bucket.Name, resource.Key

		if err := requireBucket(ctx, mc, cache, bucket); err != nil {
			HandleError(w, r, err)
			return
		}

		objectHash := model.ObjectHash(bucket, key)

		body, size := decodeBody(r)
		if size < 0 {
			HandleError(w, r, model.ErrMissingContentLengthError)
			return
		}

		poolNearFull, err := writeObject(ctx, bc, ring, cfg, body, size, objectHash)
		if err != nil {
			slog.ErrorContext(ctx, "putObject: shard distribution failed", "error", err)
			HandleError(w, r, mapPutErr(err))
			return
		}

		place, err := placeShards(ring, cfg, objectHash, size)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}

		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(place); err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}

		// Object hash -> shard placement, for retrieval.
		if err := metaPut(ctx, mc, model.TableObjects, string(objectHash[:]), buf.Bytes()); err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}

		// Listing key -> object hash, for ListObjects.
		if err := metaPut(ctx, mc, model.TableObjects, objectARN(bucket, key), objectHash[:]); err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}

		// Nearfull writes still succeed; the header lets clients back off before
		// hitting the hard 507 rejection.
		if poolNearFull {
			w.Header().Set("X-Predastore-Pool-Pressure", "nearfull")
		}
		w.Header().Set("ETag", model.ObjectETag(bucket, key))
		w.WriteHeader(http.StatusOK)
	})
}

// decodeBody unwraps aws-chunked framing when the client used it, so the rest
// of the write path only ever sees object bytes, and reports how many of those
// bytes to expect. The count is negative when the request declared no length.
func decodeBody(r *http.Request) (io.Reader, int64) {
	if r.Body == nil {
		return http.NoBody, 0
	}
	if r.Header.Get("Content-Encoding") != "aws-chunked" {
		return r.Body, r.ContentLength
	}
	// Content-Length on a chunked request measures the framing, not the object,
	// so the decoded length is the only size the splitter can use. An absent or
	// unparseable header leaves the object size undeclared.
	decodedLen, err := strconv.ParseInt(r.Header.Get("X-Amz-Decoded-Content-Length"), 10, 64)
	if err != nil || decodedLen < 0 {
		return chunked.NewDecoder(r.Body, 0), -1
	}
	return chunked.NewDecoder(r.Body, decodedLen), decodedLen
}
