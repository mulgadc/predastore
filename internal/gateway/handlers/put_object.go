package handlers

import (
	"bytes"
	"encoding/gob"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/mulgadc/predastore/internal/gateway/chunked"
	"github.com/mulgadc/predastore/internal/gateway/model"
	"github.com/mulgadc/predastore/internal/gateway/placement"
	"github.com/mulgadc/predastore/internal/storage"
)

// PutObject serves PUT /{bucket}/{key}: the body is staged to a temporary file,
// erasure coded across the storage nodes, and its placement recorded in global
// state under both the object hash and the listing key.
func PutObject(st Store, shards *storage.Client, ring *placement.Ring, cache *BucketCache, cfg Config) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		bucket := chi.URLParam(r, "bucket")
		key := chi.URLParam(r, "*")

		if bucket == "" {
			HandleError(w, r, model.ErrNoSuchBucketError.WithResource(bucket))
			return
		}
		if key == "" {
			HandleError(w, r, model.ErrNoSuchKeyError.WithResource(key))
			return
		}
		if err := requireBucket(st, cache, bucket); err != nil {
			HandleError(w, r, err)
			return
		}

		objectHash := model.ObjectHash(bucket, key)

		// The Reed-Solomon splitter needs a seekable, sized input, so the body is
		// staged to a temporary file before any shard is written.
		tmpFile, err := os.CreateTemp("", "distributed-put-*")
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}
		defer os.Remove(tmpFile.Name())
		defer tmpFile.Close()

		if r.Body != nil {
			if _, err := io.Copy(tmpFile, decodeBody(r)); err != nil {
				slog.ErrorContext(ctx, "putObject: copy to temp file failed", "error", err)
				HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
				return
			}
		}
		if closeErr := tmpFile.Close(); closeErr != nil {
			slog.DebugContext(ctx, "Failed to close temp file", "path", tmpFile.Name(), "error", closeErr)
		}

		size, poolNearFull, err := putObjectViaQUIC(ctx, shards, ring, cfg, tmpFile.Name(), objectHash)
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
		if err := statePut(st, model.TableObjects, string(objectHash[:]), buf.Bytes()); err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}

		// Listing key -> object hash, for ListObjects.
		if err := statePut(st, model.TableObjects, objectARN(bucket, key), objectHash[:]); err != nil {
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
// of the write path only ever sees object bytes.
func decodeBody(r *http.Request) io.Reader {
	if r.Header.Get("Content-Encoding") != "aws-chunked" {
		return r.Body
	}
	decodedLen, _ := strconv.ParseInt(r.Header.Get("X-Amz-Decoded-Content-Length"), 10, 64)
	return chunked.NewDecoder(r.Body, decodedLen)
}
