package handlers

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/gate/placement"
	"github.com/mulgadc/predastore/internal/telemetry"
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
		// A completion naming no parts means "use everything uploaded". AWS
		// rejects that, but MinIO accepts it and clients written against MinIO
		// rely on it. Honouring it costs nothing: a client that does send a
		// list is still held to that list exactly.
		if len(parts) == 0 && len(storedParts) > 0 {
			parts = make([]model.CompletedPart, len(storedParts))
			for i, p := range storedParts {
				parts[i] = model.CompletedPart{PartNumber: p.PartNumber, ETag: p.ETag}
			}
		}

		if err := model.ValidatePartsForCompletion(parts, storedParts); err != nil {
			// A rejected completion is a client-visible 400 and nothing else,
			// so without this the upload simply disappears from the logs.
			telemetry.RecordMultipartUpload(ctx, telemetry.UploadRejected)
			slog.WarnContext(ctx, "Multipart completion rejected",
				"uploadID", uploadID, "requested", len(parts), "stored", len(storedParts), "error", err)
			HandleError(w, r, err)
			return
		}

		storedMap := make(map[int]model.PartMetadata, len(storedParts))
		for _, p := range storedParts {
			storedMap[p.PartNumber] = p
		}

		// Validation has already established that every requested part exists,
		// so the final size is known without assembling anything to measure.
		partETags := make([]string, len(parts))
		var finalSize int64
		for i, part := range parts {
			partETags[i] = model.NormalizeETag(storedMap[part.PartNumber].ETag)
			finalSize += storedMap[part.PartNumber].Size
		}

		// The parts are streamed into the write path in order, so the assembled
		// object is erasure coded as it is read back rather than staged whole.
		assembled := streamParts(ctx, mc, bc, ring, cfg, bucket, key, uploadID, parts)
		defer assembled.Close()

		objectHash := model.ObjectHash(bucket, key)
		place, err := placeShards(ring, cfg, objectHash, finalSize)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to get shard placement", 500))
			return
		}

		written, err := writeObject(ctx, bc, cfg, ring, assembled, finalSize, objectHash, place)
		if err != nil {
			slog.ErrorContext(ctx, "Failed to store final object", "uploadID", uploadID, "error", err)
			abortShards(ctx, bc, objectHash, place, written)
			HandleError(w, r, mapPutErr(err))
			return
		}

		shardRecord, err := EncodePlacement(place)
		if err != nil {
			abortShards(ctx, bc, objectHash, place, written)
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to encode shard metadata", 500))
			return
		}
		if err := metaPut(ctx, mc, model.TableObjects, string(objectHash[:]), shardRecord); err != nil {
			abortShards(ctx, bc, objectHash, place, written)
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to store object metadata", 500))
			return
		}
		commitShards(ctx, bc, objectHash, place, written)

		if err := metaPut(ctx, mc, model.TableObjects, objectARN(bucket, key), objectHash[:]); err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to store ARN mapping", 500))
			return
		}

		// Cleanup is best-effort: the object is already durable, so a failed part
		// delete must not fail the request.
		if err := cleanupMultipartUpload(ctx, mc, bc, bucket, key, uploadID, parts); err != nil {
			slog.WarnContext(ctx, "Failed to cleanup multipart upload", "uploadID", uploadID, "error", err)
		}

		telemetry.RecordMultipartUpload(ctx, telemetry.UploadCompleted)
		slog.DebugContext(ctx, "Multipart upload completed", "bucket", bucket, "key", key, "uploadID", uploadID, "parts", len(parts))

		if written.degraded() {
			w.Header().Set(degradedWriteHeader, strconv.Itoa(len(written.missing)))
		}
		if len(written.handoff) > 0 {
			w.Header().Set(handoffHeader, strconv.Itoa(len(written.handoff)))
		}

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

// streamParts reads the parts back and writes them, in order, to the returned
// reader. Parts are fetched concurrently but only a bounded number are held at
// once: a fetch cannot start until the writer has consumed an earlier part, so
// completion stays parallel without ever holding the whole object.
//
// The caller must Close the reader. Closing it unblocks the writer and cancels
// any fetch still in flight, which is what stops an abandoned completion from
// leaking goroutines.
func streamParts(
	ctx context.Context, mc MetaClient, bc BlobClient, ring *placement.Ring, cfg Config,
	bucket, key, uploadID string, parts []model.CompletedPart,
) *io.PipeReader {
	type fetched struct {
		data []byte
		err  error
	}

	pr, pw := io.Pipe()

	go func() {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		// One slot per part in flight. The writer returns a slot after it has
		// consumed a part, which is what bounds the window.
		slots := make(chan struct{}, maxParallelPartFetches)
		results := make([]chan fetched, len(parts))
		for i := range results {
			results[i] = make(chan fetched, 1)
		}

		go func() {
			for i, part := range parts {
				select {
				case slots <- struct{}{}:
				case <-ctx.Done():
					return
				}
				go func(idx, partNumber int) {
					data, err := getPartData(ctx, mc, bc, ring, cfg, bucket, key, uploadID, partNumber)
					results[idx] <- fetched{data: data, err: err}
				}(i, part.PartNumber)
			}
		}()

		for i := range parts {
			var res fetched
			select {
			case res = <-results[i]:
			case <-ctx.Done():
				_ = pw.CloseWithError(ctx.Err())
				return
			}
			if res.err != nil {
				_ = pw.CloseWithError(res.err)
				return
			}
			if _, err := pw.Write(res.data); err != nil {
				_ = pw.CloseWithError(err)
				return
			}
			<-slots
		}
		_ = pw.Close()
	}()

	return pr
}

// getPartData reads one part back from its shards. A part is an object under a
// hidden key, so it is read exactly as GET reads one: the data shards are
// joined directly, and parity is only pulled in if that join fails. Going
// straight to reconstruction would read every shard twice for every part.
func getPartData(ctx context.Context, mc MetaClient, bc BlobClient, ring *placement.Ring, cfg Config, bucket, key, uploadID string, partNumber int) ([]byte, error) {
	data, err := metaGet(ctx, mc, model.TableObjects, partShardKey(uploadID, partNumber))
	if err != nil {
		telemetry.RecordMultipartPartFetch(ctx, telemetry.FetchReasonMetaMissing)
		return nil, fmt.Errorf("part not found: uploadID=%s part=%d", uploadID, partNumber)
	}

	place, err := DecodePlacement(data)
	if err != nil {
		telemetry.RecordMultipartPartFetch(ctx, telemetry.FetchReasonPlacementDecode)
		return nil, err
	}

	partKey := partObjectKey(key, uploadID, partNumber)
	part, _, err := readObject(ctx, bc, cfg, bucket, partKey, place, place.Size,
		handoffNode(ring, cfg, model.ObjectHash(bucket, partKey)))
	if err != nil {
		telemetry.RecordMultipartPartFetch(ctx, telemetry.FetchReasonShardRead)
		return nil, err
	}
	telemetry.RecordMultipartPartFetch(ctx, "")
	return part, nil
}
