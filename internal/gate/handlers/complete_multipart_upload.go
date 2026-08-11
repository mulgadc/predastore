package handlers

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync"

	"github.com/klauspost/reedsolomon"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/gate/placement"
)

// maxParallelPartFetches bounds the read-back fan-out while assembling an
// upload, so completing a many-part object cannot saturate the blob nodes.
const maxParallelPartFetches = 10

// CompleteMultipartUpload serves POST /{bucket}/{key}?uploadId=X: it reads the
// parts back, concatenates them, and stores the result as one object.
func CompleteMultipartUpload(mc MetaClient, bc BlobClient, ring *placement.Ring, cache *BucketCache, cfg Config) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		resource, ok := routedObject(w, r)
		if !ok {
			return
		}
		bucket, key := resource.Bucket.Name, resource.Key
		uploadID := r.URL.Query().Get("uploadId")

		body, err := io.ReadAll(r.Body)
		if err != nil {
			HandleError(w, r, err)
			return
		}
		var completeReq CompleteMultipartUploadRequest
		if err := xml.Unmarshal(body, &completeReq); err != nil {
			HandleError(w, r, err)
			return
		}
		parts := make([]model.CompletedPart, len(completeReq.Parts))
		for i, p := range completeReq.Parts {
			parts[i] = model.CompletedPart{PartNumber: p.PartNumber, ETag: p.ETag}
		}

		if err := requireBucket(ctx, mc, cache, bucket); err != nil {
			HandleError(w, r, err)
			return
		}
		if err := requireUpload(ctx, mc, bucket, key, uploadID); err != nil {
			HandleError(w, r, err)
			return
		}

		storedParts, err := getStoredParts(ctx, mc, uploadID)
		if err != nil {
			slog.ErrorContext(ctx, "Failed to get stored parts", "uploadID", uploadID, "error", err)
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to retrieve parts", 500))
			return
		}
		if err := model.ValidatePartsForCompletion(parts, storedParts); err != nil {
			HandleError(w, r, err)
			return
		}

		storedMap := make(map[int]model.PartMetadata, len(storedParts))
		for _, p := range storedParts {
			storedMap[p.PartNumber] = p
		}

		// Assemble the parts into a temp file, which is then written exactly as a
		// single-shot PutObject would write it.
		tmpFile, err := os.CreateTemp("", "multipart-complete-*")
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to create temp file", 500))
			return
		}
		defer os.Remove(tmpFile.Name())
		defer tmpFile.Close()

		partETags := make([]string, len(parts))
		partData := make([][]byte, len(parts))

		type partResult struct {
			index int
			data  []byte
			err   error
		}
		resultChan := make(chan partResult, len(parts))
		semaphore := make(chan struct{}, maxParallelPartFetches)

		var wg sync.WaitGroup
		for i, part := range parts {
			partETags[i] = model.NormalizeETag(storedMap[part.PartNumber].ETag)

			wg.Add(1)
			go func(idx int, partNum int) {
				defer wg.Done()
				semaphore <- struct{}{}
				defer func() { <-semaphore }()

				data, err := getPartData(ctx, mc, bc, cfg, bucket, key, uploadID, partNum)
				resultChan <- partResult{index: idx, data: data, err: err}
			}(i, part.PartNumber)
		}

		go func() {
			wg.Wait()
			close(resultChan)
		}()

		for result := range resultChan {
			if result.err != nil {
				slog.ErrorContext(ctx, "Failed to retrieve part data", "uploadID", uploadID, "index", result.index, "error", result.err)
				HandleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to retrieve part data", 500))
				return
			}
			partData[result.index] = result.data
		}

		for _, data := range partData {
			if _, err := tmpFile.Write(data); err != nil {
				HandleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to write assembled data", 500))
				return
			}
		}
		if closeErr := tmpFile.Close(); closeErr != nil {
			slog.DebugContext(ctx, "Failed to close temp file", "path", tmpFile.Name(), "error", closeErr)
		}

		objectHash := model.ObjectHash(bucket, key)
		if _, _, err := writeObject(ctx, bc, ring, cfg, tmpFile.Name(), objectHash); err != nil {
			slog.ErrorContext(ctx, "Failed to store final object", "uploadID", uploadID, "error", err)
			HandleError(w, r, mapPutErr(err))
			return
		}

		finalInfo, err := os.Stat(tmpFile.Name())
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to get final object size", 500))
			return
		}

		place, err := placeShards(ring, cfg, objectHash, finalInfo.Size())
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to get shard placement", 500))
			return
		}
		var shardBuf bytes.Buffer
		if err := gob.NewEncoder(&shardBuf).Encode(place); err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to encode shard metadata", 500))
			return
		}
		if err := metaPut(ctx, mc, model.TableObjects, string(objectHash[:]), shardBuf.Bytes()); err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to store object metadata", 500))
			return
		}
		if err := metaPut(ctx, mc, model.TableObjects, objectARN(bucket, key), objectHash[:]); err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to store ARN mapping", 500))
			return
		}

		// Cleanup is best-effort: the object is already durable, so a failed part
		// delete must not fail the request.
		if err := cleanupMultipartUpload(ctx, mc, bc, bucket, key, uploadID, parts); err != nil {
			slog.WarnContext(ctx, "Failed to cleanup multipart upload", "uploadID", uploadID, "error", err)
		}

		slog.DebugContext(ctx, "Multipart upload completed", "bucket", bucket, "key", key, "uploadID", uploadID, "parts", len(parts))

		if err := writeXML(w, http.StatusOK, CompleteMultipartUploadResult{
			Location: fmt.Sprintf("https://%s/%s/%s", r.Host, bucket, key),
			Bucket:   bucket,
			Key:      key,
			ETag:     model.CalculateMultipartETag(partETags, len(parts)),
		}); err != nil {
			slog.DebugContext(ctx, "failed to write XML response", "error", err)
		}
	})
}

// getPartData reads one part back from its shards.
func getPartData(ctx context.Context, mc MetaClient, bc BlobClient, cfg Config, bucket, key, uploadID string, partNumber int) ([]byte, error) {
	data, err := metaGet(ctx, mc, model.TableObjects, partShardKey(uploadID, partNumber))
	if err != nil {
		return nil, fmt.Errorf("part not found: uploadID=%s part=%d", uploadID, partNumber)
	}

	var place ObjectToShardNodes
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&place); err != nil {
		return nil, err
	}

	enc, err := reedsolomon.NewStream(cfg.DataShards, cfg.ParityShards)
	if err != nil {
		return nil, fmt.Errorf("failed to create Reed-Solomon decoder: %w", err)
	}

	partKey := partObjectKey(key, uploadID, partNumber)
	buf, err := reconstructObject(ctx, bc, model.ObjectHash(bucket, partKey), place, enc, place.Size)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
