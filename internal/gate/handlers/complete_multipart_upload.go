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

// multipartReadAhead bounds how far ahead of the writer the parts are resolved.
//
// It counts *placements*, not parts of data and not open streams. A placement
// is a KV get and a small struct, so the window costs nothing and is the same
// size whether the client chose 5 MiB parts or 5 GiB ones. Holding parts here
// instead is what made completion cost ten times whatever part size the client
// picked; holding open streams instead would risk the blob client's idle
// timeout, because a 5 GiB part takes half a minute to drain and a stream
// waiting its turn is an idle stream.
const multipartReadAhead = 8

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
			telemetry.RecordObjectWrite(ctx, telemetry.WriteOutcomeFailed, writeFailureReason(err))
			abortShards(ctx, bc, objectHash, place, written)
			HandleError(w, r, mapPutErr(err))
			return
		}

		// The assembled object's own record must carry the same digest the
		// response reports below, so a later HEAD, GET or listing can return
		// it without recomputing anything.
		digest := model.CalculateMultipartDigest(partETags)
		place.Digest = digest[:]
		place.PartCount = len(parts)

		shardRecord, err := EncodePlacement(place)
		if err != nil {
			abortShards(ctx, bc, objectHash, place, written)
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to encode shard metadata", 500))
			return
		}
		if err := metaPut(ctx, mc, model.TableObjects, string(objectHash[:]), shardRecord); err != nil {
			telemetry.RecordObjectWrite(ctx, telemetry.WriteOutcomeFailed, telemetry.WriteReasonMeta)
			abortShards(ctx, bc, objectHash, place, written)
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to store object metadata", 500))
			return
		}
		commitShards(ctx, bc, objectHash, place, written)

		if err := metaPut(ctx, mc, model.TableObjects, objectARN(bucket, key), objectHash[:]); err != nil {
			telemetry.RecordObjectWrite(ctx, telemetry.WriteOutcomeFailed, telemetry.WriteReasonMeta)
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "Failed to store ARN mapping", 500))
			return
		}

		// An object assembled from parts is an object write like any other, so
		// the counter means objects created rather than single-shot PUTs only.
		telemetry.RecordObjectWrite(ctx, telemetry.WriteOutcomeSuccess, "")

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
// reader. Each part is streamed from its shards straight into the pipe rather
// than assembled first, so completion holds a block per shard however large the
// client made its parts.
//
// The caller must Close the reader. Closing it unblocks the writer and cancels
// any fetch still in flight, which is what stops an abandoned completion from
// leaking goroutines.
func streamParts(
	ctx context.Context, mc MetaClient, bc BlobClient, ring *placement.Ring, cfg Config,
	bucket, key, uploadID string, parts []model.CompletedPart,
) *io.PipeReader {
	pr, pw := io.Pipe()

	go func() {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		// Placements are resolved ahead of the writer so the meta lookup of the
		// next part overlaps the streaming of the current one. Only the lookup
		// is ahead; the shards are opened when the part's turn comes.
		located := make(chan locatedPart, multipartReadAhead)
		go func() {
			defer close(located)
			for _, part := range parts {
				found := locatePart(ctx, mc, bucket, key, uploadID, part.PartNumber)
				select {
				case located <- found:
				case <-ctx.Done():
					return
				}
				if found.err != nil {
					return
				}
			}
		}()

		for found := range located {
			if found.err != nil {
				_ = pw.CloseWithError(found.err)

				return
			}
			if err := pipePart(ctx, bc, ring, cfg, bucket, found, pw); err != nil {
				_ = pw.CloseWithError(err)

				return
			}
		}
		_ = pw.Close()
	}()

	return pr
}

// locatedPart is a part's placement record, or the reason it could not be read.
type locatedPart struct {
	key   string
	place ObjectToShardNodes
	err   error
}

// locatePart resolves where one part's shards are. A part is an object under a
// hidden key, so this is the same lookup a GET does.
func locatePart(
	ctx context.Context, mc MetaClient, bucket, key, uploadID string, partNumber int,
) locatedPart {
	data, err := metaGet(ctx, mc, model.TableObjects, partShardKey(uploadID, partNumber))
	if err != nil {
		telemetry.RecordMultipartPartFetch(ctx, telemetry.FetchReasonMetaMissing)

		return locatedPart{err: fmt.Errorf("part not found: uploadID=%s part=%d", uploadID, partNumber)}
	}

	place, err := DecodePlacement(data)
	if err != nil {
		telemetry.RecordMultipartPartFetch(ctx, telemetry.FetchReasonPlacementDecode)

		return locatedPart{err: err}
	}

	return locatedPart{key: partObjectKey(key, uploadID, partNumber), place: place}
}

// pipePart streams one part's shards into the assembled object. The reader is
// opened here rather than with the placement, so a part waiting its turn holds
// no stream: the blob client aborts one left idle, and the largest parts are
// exactly the ones that would wait longest.
func pipePart(
	ctx context.Context, bc BlobClient, ring *placement.Ring, cfg Config,
	bucket string, found locatedPart, dst io.Writer,
) error {
	objectHash := model.ObjectHash(bucket, found.key)
	reader, err := newStripeReader(ctx, bc, cfg, objectHash, found.place,
		handoffNode(ring, cfg, objectHash))
	if err != nil {
		telemetry.RecordMultipartPartFetch(ctx, telemetry.FetchReasonShardRead)

		return err
	}
	defer reader.close(ctx)

	if err := pipeObject(ctx, reader, dst, found.place.Size); err != nil {
		telemetry.RecordMultipartPartFetch(ctx, telemetry.FetchReasonShardRead)

		return fmt.Errorf("read part %s: %w", found.key, err)
	}
	telemetry.RecordMultipartPartFetch(ctx, "")

	return nil
}
