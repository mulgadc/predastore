package handlers

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/gate/placement"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/mulgadc/predastore/internal/telemetry"
)

// multipartPartKey generates the key for storing part metadata.
func multipartPartKey(uploadID string, partNumber int) string {
	return fmt.Sprintf("%s:%05d", uploadID, partNumber)
}

// multipartPartsPrefix returns the prefix for all parts of an upload.
func multipartPartsPrefix(uploadID string) string {
	return uploadID + ":"
}

// partKeyPrefix starts the key a part's shard placement is recorded under.
const partKeyPrefix = "part:"

// partShardKey is where a part's shard placement is recorded, in the object
// table alongside whole objects.
func partShardKey(uploadID string, partNumber int) string {
	return fmt.Sprintf("%s%s:%05d", partKeyPrefix, uploadID, partNumber)
}

// partObjectKey is the object name a part's shards are hashed under. Parts live
// in a hidden prefix so an in-flight upload never shows up in a listing.
func partObjectKey(key, uploadID string, partNumber int) string {
	return fmt.Sprintf(".multipart/%s/%s/%05d", uploadID, key, partNumber)
}

// getUploadMetadata retrieves and validates upload metadata.
func getUploadMetadata(ctx context.Context, mc MetaClient, uploadID string) (*model.UploadMetadata, error) {
	data, err := metaGet(ctx, mc, model.TableMultipart, uploadID)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			return nil, model.ErrNoSuchUploadError.WithResource(uploadID)
		}
		return nil, model.NewS3Error(model.ErrInternalError, "Failed to retrieve upload metadata", 500)
	}

	var metadata model.UploadMetadata
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&metadata); err != nil { //nolint:gosec // G709: the input is state this gate wrote, not client data.
		return nil, model.NewS3Error(model.ErrInternalError, "Failed to decode upload metadata", 500)
	}

	return &metadata, nil
}

// requireUpload resolves an upload and checks it belongs to the bucket and key
// the request addressed, so one upload's parts can never be committed under
// another object's name.
func requireUpload(ctx context.Context, mc MetaClient, bucket, key, uploadID string) error {
	metadata, err := getUploadMetadata(ctx, mc, uploadID)
	if err != nil {
		return err
	}
	if metadata.Bucket != bucket || metadata.Key != key {
		return model.NewS3Error(model.ErrInvalidPart, "Bucket or key does not match upload", 400)
	}
	return nil
}

// storePart writes one part of an upload and records it. A part is stored as an
// object in its own right under a hidden key, so completion is a read-back and
// concatenation; body must deliver exactly size bytes.
//
// verify runs once the payload has landed but before the part is reachable, so
// a failed check leaves nothing CompleteMultipartUpload can assemble. It is
// where UploadPart validates the chunked encoding it decoded; a copied part has
// no client payload to check and passes nil.
func storePart(
	ctx context.Context, mc MetaClient, bc BlobClient, ring *placement.Ring, cfg Config,
	bucket, key, uploadID string, partNumber int, body io.Reader, size int64,
	verify func() error,
) (model.PartMetadata, writeResult, error) {
	partKey := partObjectKey(key, uploadID, partNumber)
	objectHash := model.ObjectHash(bucket, partKey)

	// The ETag is MD5 over the part, and the write path already reads the part
	// end to end, so the hash is teed off that read instead of costing a second
	// pass over a buffered copy.
	digest := model.NewPartETagHasher()

	// Placement comes from the part's own object hash, so a retried part lands
	// on the same nodes without anything deterministic on disk. Its epoch is its
	// own: a part is an object and follows the same rules.
	place, err := placeShards(ring, cfg, objectHash, size)
	if err != nil {
		return model.PartMetadata{}, writeResult{}, model.NewS3Error(model.ErrInternalError, "Failed to get shard placement", 500)
	}

	written, err := writeObject(ctx, bc, cfg, ring, io.TeeReader(body, digest), size, objectHash, place)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to store part", "uploadID", uploadID, "part", partNumber, "error", err)
		abortShards(ctx, bc, objectHash, place, written)
		return model.PartMetadata{}, written, mapPutErr(err)
	}
	if verify != nil {
		if err := verify(); err != nil {
			abortShards(ctx, bc, objectHash, place, written)
			return model.PartMetadata{}, written, err
		}
	}

	// The part is dated from its own epoch, so ListParts and the placement
	// record cannot disagree about when it was written.
	modified, _ := place.ModifiedAt()
	partMeta := model.PartMetadata{
		PartNumber:   partNumber,
		Size:         size,
		ETag:         model.PartETagFrom(digest),
		LastModified: modified,
	}

	var partBuf bytes.Buffer
	if err := gob.NewEncoder(&partBuf).Encode(partMeta); err != nil {
		abortShards(ctx, bc, objectHash, place, written)
		return model.PartMetadata{}, written, model.NewS3Error(model.ErrInternalError, "Failed to encode part metadata", 500)
	}
	if err := metaPut(ctx, mc, model.TableParts, multipartPartKey(uploadID, partNumber), partBuf.Bytes()); err != nil {
		slog.ErrorContext(ctx, "Failed to store part metadata", "uploadID", uploadID, "part", partNumber, "error", err)
		abortShards(ctx, bc, objectHash, place, written)
		return model.PartMetadata{}, written, model.NewS3Error(model.ErrInternalError, "Failed to store part metadata", 500)
	}

	shardRecord, err := EncodePlacement(place)
	if err != nil {
		abortShards(ctx, bc, objectHash, place, written)
		return model.PartMetadata{}, written, model.NewS3Error(model.ErrInternalError, "Failed to encode shard metadata", 500)
	}
	if err := metaPut(ctx, mc, model.TableObjects, partShardKey(uploadID, partNumber), shardRecord); err != nil {
		slog.ErrorContext(ctx, "Failed to store part shard metadata", "uploadID", uploadID, "part", partNumber, "error", err)
		abortShards(ctx, bc, objectHash, place, written)
		return model.PartMetadata{}, written, model.NewS3Error(model.ErrInternalError, "Failed to store part shard metadata", 500)
	}

	commitShards(ctx, bc, objectHash, place, written)
	telemetry.RecordMultipartPart(ctx, size)
	slog.DebugContext(ctx, "Part uploaded", "uploadID", uploadID, "partNumber", partNumber,
		"size", size, "etag", partMeta.ETag)

	return partMeta, written, nil
}

// writePartHeaders reports what the write cost the part's durability. Neither
// is an error: the part is stored either way.
func writePartHeaders(w http.ResponseWriter, written writeResult) {
	if written.degraded() {
		w.Header().Set(degradedWriteHeader, strconv.Itoa(len(written.missing)))
	}
	if len(written.handoff) > 0 {
		w.Header().Set(handoffHeader, strconv.Itoa(len(written.handoff)))
	}
}

// getStoredParts retrieves all stored parts for an upload, in part order.
func getStoredParts(ctx context.Context, mc MetaClient, uploadID string) ([]model.PartMetadata, error) {
	items, err := metaScan(ctx, mc, model.TableParts, multipartPartsPrefix(uploadID), 0)
	if err != nil {
		return nil, err
	}

	parts := make([]model.PartMetadata, 0, len(items))
	for _, item := range items {
		var part model.PartMetadata
		if err := gob.NewDecoder(bytes.NewReader(item.Value)).Decode(&part); err != nil {
			return nil, err
		}
		parts = append(parts, part)
	}

	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	return parts, nil
}

// cleanupMultipartUpload removes all part shards, part/upload metadata, and the
// shard-location map for an upload. Shard deletes are best-effort: a per-node
// failure is logged and skipped, never failing the complete/abort request.
func cleanupMultipartUpload(ctx context.Context, mc MetaClient, bc BlobClient, bucket, key, uploadID string, parts []model.CompletedPart) error {
	for _, part := range parts {
		shardKey := partShardKey(uploadID, part.PartNumber)

		// Drop the physical part shards before removing the shard-location map. A missing
		// or corrupt map, or a per-node delete failure, is logged and skipped — cleanup is
		// best-effort and must not fail the complete/abort request.
		if data, err := metaGet(ctx, mc, model.TableObjects, shardKey); err != nil {
			slog.WarnContext(ctx, "cleanup: part shard map missing, skipping shard delete", "uploadID", uploadID, "part", part.PartNumber)
		} else {
			nodes, err := DecodePlacement(data)
			if err != nil {
				slog.WarnContext(ctx, "cleanup: part shard map corrupt, skipping shard delete", "uploadID", uploadID, "part", part.PartNumber, "error", err)
			} else {
				partObjKey := partObjectKey(key, uploadID, part.PartNumber)
				if err := deleteObject(ctx, bc, bucket, partObjKey, model.ObjectHash(bucket, partObjKey), nodes); err != nil {
					slog.ErrorContext(ctx, "cleanup: shard delete failed, continuing", "uploadID", uploadID, "part", part.PartNumber, "error", err)
				}
			}
		}

		if err := metaDelete(ctx, mc, model.TableParts, multipartPartKey(uploadID, part.PartNumber)); err != nil {
			slog.WarnContext(ctx, "Failed to delete part metadata", "uploadID", uploadID, "part", part.PartNumber, "error", err)
		}

		if err := metaDelete(ctx, mc, model.TableObjects, shardKey); err != nil {
			slog.WarnContext(ctx, "Failed to delete part shard metadata", "uploadID", uploadID, "part", part.PartNumber, "error", err)
		}
	}

	return metaDelete(ctx, mc, model.TableMultipart, uploadID)
}
