package s3

import (
	"bytes"
	"encoding/gob"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/mulgadc/predastore/internal/gateway/model"
	"github.com/mulgadc/predastore/s3/chunked"
)

// putObject serves PUT /{bucket}/{key}: the body is staged to a temporary file,
// erasure coded across the storage nodes, and its placement recorded in global
// state under both the object hash and the listing key.
func (s *HTTP2Server) putObject(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucket := chi.URLParam(r, "bucket")
	key := chi.URLParam(r, "*")

	if bucket == "" {
		s.handleError(w, r, model.ErrNoSuchBucketError.WithResource(bucket))
		return
	}
	if key == "" {
		s.handleError(w, r, model.ErrNoSuchKeyError.WithResource(key))
		return
	}
	if err := s.requireBucket(bucket); err != nil {
		s.handleError(w, r, err)
		return
	}

	objectHash := model.ObjectHash(bucket, key)

	// The Reed-Solomon splitter needs a seekable, sized input, so the body is
	// staged to a temporary file before any shard is written.
	tmpFile, err := os.CreateTemp("", "distributed-put-*")
	if err != nil {
		s.handleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
		return
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if r.Body != nil {
		if _, err := io.Copy(tmpFile, s.decodeBody(r)); err != nil {
			slog.ErrorContext(ctx, "putObject: copy to temp file failed", "error", err)
			s.handleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}
	}
	if closeErr := tmpFile.Close(); closeErr != nil {
		slog.DebugContext(ctx, "Failed to close temp file", "path", tmpFile.Name(), "error", closeErr)
	}

	size, poolNearFull, err := s.putObjectViaQUIC(ctx, tmpFile.Name(), objectHash)
	if err != nil {
		slog.ErrorContext(ctx, "putObject: shard distribution failed", "error", err)
		s.handleError(w, r, mapPutErr(err))
		return
	}

	placement, err := s.placeShards(objectHash, size)
	if err != nil {
		s.handleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
		return
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(placement); err != nil {
		s.handleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
		return
	}

	// Object hash -> shard placement, for retrieval.
	if err := s.statePut(model.TableObjects, string(objectHash[:]), buf.Bytes()); err != nil {
		s.handleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
		return
	}

	// Listing key -> object hash, for ListObjects.
	if err := s.statePut(model.TableObjects, objectARN(bucket, key), objectHash[:]); err != nil {
		s.handleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
		return
	}

	// Nearfull writes still succeed; the header lets clients back off before
	// hitting the hard 507 rejection.
	if poolNearFull {
		w.Header().Set("X-Predastore-Pool-Pressure", "nearfull")
	}
	w.Header().Set("ETag", model.ObjectETag(bucket, key))
	w.WriteHeader(http.StatusOK)
}

// decodeBody unwraps aws-chunked framing when the client used it, so the rest
// of the write path only ever sees object bytes.
func (s *HTTP2Server) decodeBody(r *http.Request) io.Reader {
	if r.Header.Get("Content-Encoding") != "aws-chunked" {
		return r.Body
	}
	decodedLen, _ := strconv.ParseInt(r.Header.Get("X-Amz-Decoded-Content-Length"), 10, 64)
	return chunked.NewDecoder(r.Body, decodedLen)
}
