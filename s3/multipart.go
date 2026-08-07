package s3

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"log/slog"
	"sort"

	"github.com/mulgadc/predastore/internal/gateway/model"
	"github.com/mulgadc/predastore/internal/state"
)

// multipartPartKey generates the key for storing part metadata.
func multipartPartKey(uploadID string, partNumber int) string {
	return fmt.Sprintf("%s:%05d", uploadID, partNumber)
}

// multipartPartsPrefix returns the prefix for all parts of an upload.
func multipartPartsPrefix(uploadID string) string {
	return uploadID + ":"
}

// partShardKey is where a part's shard placement is recorded, in the object
// table alongside whole objects.
func partShardKey(uploadID string, partNumber int) string {
	return fmt.Sprintf("part:%s:%05d", uploadID, partNumber)
}

// partObjectKey is the object name a part's shards are hashed under. Parts live
// in a hidden prefix so an in-flight upload never shows up in a listing.
func partObjectKey(key, uploadID string, partNumber int) string {
	return fmt.Sprintf(".multipart/%s/%s/%05d", uploadID, key, partNumber)
}

// getUploadMetadata retrieves and validates upload metadata.
func (s *HTTP2Server) getUploadMetadata(uploadID string) (*model.UploadMetadata, error) {
	data, err := s.stateGet(model.TableMultipart, uploadID)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil, model.ErrNoSuchUploadError.WithResource(uploadID)
		}
		return nil, model.NewS3Error(model.ErrInternalError, "Failed to retrieve upload metadata", 500)
	}

	var metadata model.UploadMetadata
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&metadata); err != nil { //nolint:gosec // G709: the input is state this gateway wrote, not client data.
		return nil, model.NewS3Error(model.ErrInternalError, "Failed to decode upload metadata", 500)
	}

	return &metadata, nil
}

// requireUpload resolves an upload and checks it belongs to the bucket and key
// the request addressed, so one upload's parts can never be committed under
// another object's name.
func (s *HTTP2Server) requireUpload(bucket, key, uploadID string) error {
	metadata, err := s.getUploadMetadata(uploadID)
	if err != nil {
		return err
	}
	if metadata.Bucket != bucket || metadata.Key != key {
		return model.NewS3Error(model.ErrInvalidPart, "Bucket or key does not match upload", 400)
	}
	return nil
}

// getStoredParts retrieves all stored parts for an upload, in part order.
func (s *HTTP2Server) getStoredParts(uploadID string) ([]model.PartMetadata, error) {
	items, err := s.stateScan(model.TableParts, multipartPartsPrefix(uploadID), 0)
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
func (s *HTTP2Server) cleanupMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []model.CompletedPart) error {
	for _, part := range parts {
		shardKey := partShardKey(uploadID, part.PartNumber)

		// Drop the physical part shards before removing the shard-location map. A missing
		// or corrupt map, or a per-node delete failure, is logged and skipped — cleanup is
		// best-effort and must not fail the complete/abort request.
		if data, err := s.stateGet(model.TableObjects, shardKey); err != nil {
			slog.WarnContext(ctx, "cleanup: part shard map missing, skipping shard delete", "uploadID", uploadID, "part", part.PartNumber)
		} else {
			var nodes ObjectToShardNodes
			if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&nodes); err != nil {
				slog.WarnContext(ctx, "cleanup: part shard map corrupt, skipping shard delete", "uploadID", uploadID, "part", part.PartNumber, "error", err)
			} else {
				partObjKey := partObjectKey(key, uploadID, part.PartNumber)
				if err := s.deleteObjectViaQUIC(ctx, bucket, partObjKey, nodes.Object, nodes); err != nil {
					slog.ErrorContext(ctx, "cleanup: shard delete failed, continuing", "uploadID", uploadID, "part", part.PartNumber, "error", err)
				}
			}
		}

		if err := s.stateDelete(model.TableParts, multipartPartKey(uploadID, part.PartNumber)); err != nil {
			slog.WarnContext(ctx, "Failed to delete part metadata", "uploadID", uploadID, "part", part.PartNumber, "error", err)
		}

		if err := s.stateDelete(model.TableObjects, shardKey); err != nil {
			slog.WarnContext(ctx, "Failed to delete part shard metadata", "uploadID", uploadID, "part", part.PartNumber, "error", err)
		}
	}

	return s.stateDelete(model.TableMultipart, uploadID)
}
