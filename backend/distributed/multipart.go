package distributed

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/klauspost/reedsolomon"
	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/backend/multipart"
	"github.com/mulgadc/predastore/quic/quicserver"
	s3db "github.com/mulgadc/predastore/s3db"
	"golang.org/x/sync/errgroup"
)

// multipartUploadKey generates the key for storing upload metadata.
func multipartUploadKey(uploadID string) []byte {
	return []byte(uploadID)
}

// multipartPartKey generates the key for storing part metadata.
func multipartPartKey(uploadID string, partNumber int) []byte {
	return fmt.Appendf(nil, "%s:%05d", uploadID, partNumber)
}

// multipartPartsPrefix returns the prefix for all parts of an upload.
func multipartPartsPrefix(uploadID string) []byte {
	return []byte(uploadID + ":")
}

// partObjectKey generates the S3 object key for storing a part.
func partObjectKey(bucket, key, uploadID string, partNumber int) string {
	// Store parts in a hidden directory structure
	return fmt.Sprintf(".multipart/%s/%s/%05d", uploadID, key, partNumber)
}

// CreateMultipartUpload initiates a multipart upload.
func (b *Backend) CreateMultipartUpload(ctx context.Context, req *backend.CreateMultipartUploadRequest) (*backend.CreateMultipartUploadResponse, error) {
	if req.Bucket == "" {
		return nil, backend.ErrNoSuchBucketError.WithResource(req.Bucket)
	}
	if req.Key == "" {
		return nil, backend.ErrNoSuchKeyError.WithResource(req.Key)
	}

	if _, err := b.HeadBucket(ctx, &backend.HeadBucketRequest{Bucket: req.Bucket}); err != nil {
		return nil, err
	}

	// Generate unique upload ID
	uploadID := uuid.New().String()

	// Create upload metadata
	metadata := multipart.UploadMetadata{
		UploadID:    uploadID,
		Bucket:      req.Bucket,
		Key:         req.Key,
		ContentType: req.ContentType,
		CreatedAt:   time.Now(),
		Parts:       []multipart.PartMetadata{},
	}

	// Encode and store metadata
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(metadata); err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, "Failed to encode upload metadata", 500)
	}

	if err := b.globalState.Set(TableMultipart, multipartUploadKey(uploadID), buf.Bytes()); err != nil {
		slog.Error("Failed to store multipart upload metadata", "uploadID", uploadID, "error", err)
		return nil, backend.NewS3Error(backend.ErrInternalError, "Failed to create multipart upload", 500)
	}

	slog.Debug("Multipart upload created", "bucket", req.Bucket, "key", req.Key, "uploadID", uploadID)

	return &backend.CreateMultipartUploadResponse{
		Bucket:   req.Bucket,
		Key:      req.Key,
		UploadID: uploadID,
	}, nil
}

// getUploadMetadata retrieves and validates upload metadata.
func (b *Backend) getUploadMetadata(uploadID string) (*multipart.UploadMetadata, error) {
	data, err := b.globalState.Get(TableMultipart, multipartUploadKey(uploadID))
	if err != nil {
		if errors.Is(err, s3db.ErrKeyNotFound) {
			return nil, backend.ErrNoSuchUploadError.WithResource(uploadID)
		}
		return nil, backend.NewS3Error(backend.ErrInternalError, "Failed to retrieve upload metadata", 500)
	}

	var metadata multipart.UploadMetadata
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&metadata); err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, "Failed to decode upload metadata", 500)
	}

	return &metadata, nil
}

// UploadPart uploads a part in a multipart upload.
func (b *Backend) UploadPart(ctx context.Context, req *backend.UploadPartRequest) (*backend.UploadPartResponse, error) {
	if req.Bucket == "" {
		return nil, backend.ErrNoSuchBucketError.WithResource(req.Bucket)
	}
	if req.Key == "" {
		return nil, backend.ErrNoSuchKeyError.WithResource(req.Key)
	}

	if _, err := b.HeadBucket(ctx, &backend.HeadBucketRequest{Bucket: req.Bucket}); err != nil {
		return nil, err
	}

	// Validate part number
	if err := multipart.ValidatePartNumber(req.PartNumber); err != nil {
		return nil, err
	}

	// Verify upload exists
	uploadMetadata, err := b.getUploadMetadata(req.UploadID)
	if err != nil {
		return nil, err
	}

	// Verify bucket and key match
	if uploadMetadata.Bucket != req.Bucket || uploadMetadata.Key != req.Key {
		return nil, backend.NewS3Error(backend.ErrInvalidPart, "Bucket or key does not match upload", 400)
	}

	// Validate part size before reading or allocating. The S3 ingress passes the
	// exact decoded length, so ETag hashing and storage share one read.
	if req.ContentLength < 0 {
		return nil, backend.NewS3Error(backend.ErrInvalidRequest, "Part content length must be non-negative", 400)
	}
	if req.ContentLength > multipart.MaxPartSize {
		return nil, backend.NewS3Error(backend.ErrEntityTooLarge, "Part exceeds maximum size", 400)
	}

	// Store the part data using the existing PutObject mechanism
	// Use a deterministic path based on uploadID and partNumber for consistent hash ring placement
	partKey := partObjectKey(req.Bucket, req.Key, req.UploadID, req.PartNumber)
	objectHash := s3db.GenObjectHash(req.Bucket, partKey)

	hash := md5.New() // S3 multipart ETags require MD5 for compatibility.
	reader := io.TeeReader(req.Body, hash)
	if _, err = b.putObjectViaQUIC(ctx, req.Bucket, partKey, reader, req.ContentLength, objectHash); err != nil {
		slog.Error("Failed to store part", "uploadID", req.UploadID, "part", req.PartNumber, "error", err)
		return nil, mapPutErr(err)
	}
	etag := fmt.Sprintf("\"%x\"", hash.Sum(nil))
	partSize := req.ContentLength

	// Create part metadata
	partMeta := multipart.PartMetadata{
		PartNumber:   req.PartNumber,
		Size:         partSize,
		ETag:         etag,
		LastModified: time.Now(),
	}

	// Store part metadata
	var partBuf bytes.Buffer
	partEnc := gob.NewEncoder(&partBuf)
	if err := partEnc.Encode(partMeta); err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, "Failed to encode part metadata", 500)
	}

	if err := b.globalState.Set(TableParts, multipartPartKey(req.UploadID, req.PartNumber), partBuf.Bytes()); err != nil {
		slog.Error("Failed to store part metadata", "uploadID", req.UploadID, "part", req.PartNumber, "error", err)
		return nil, backend.NewS3Error(backend.ErrInternalError, "Failed to store part metadata", 500)
	}

	// Store the shard location mapping for this part
	partShardKey := fmt.Sprintf("part:%s:%05d", req.UploadID, req.PartNumber)
	partObjectToShardNodes := ObjectToShardNodes{
		Object: objectHash,
		Size:   partSize,
	}

	// Get hash ring placement for this part using the partKey for consistency
	// This ensures the same nodes are used for storage and retrieval
	hashRingShards, err := b.hashRing.GetClosestN(objectHash[:], b.rsDataShard+b.rsParityShard)
	if err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, "Failed to get shard placement", 500)
	}

	partObjectToShardNodes.DataShardNodes = make([]uint32, b.rsDataShard)
	partObjectToShardNodes.ParityShardNodes = make([]uint32, b.rsParityShard)

	for i := 0; i < b.rsDataShard; i++ {
		partObjectToShardNodes.DataShardNodes[i], _ = NodeToUint32(hashRingShards[i].String())
	}
	for i := 0; i < b.rsParityShard; i++ {
		partObjectToShardNodes.ParityShardNodes[i], _ = NodeToUint32(hashRingShards[b.rsDataShard+i].String())
	}

	var shardBuf bytes.Buffer
	shardEnc := gob.NewEncoder(&shardBuf)
	if err := shardEnc.Encode(partObjectToShardNodes); err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, "Failed to encode shard metadata", 500)
	}

	if err := b.globalState.Set(TableObjects, []byte(partShardKey), shardBuf.Bytes()); err != nil {
		slog.Error("Failed to store part shard metadata", "uploadID", req.UploadID, "part", req.PartNumber, "error", err)
		return nil, backend.NewS3Error(backend.ErrInternalError, "Failed to store part shard metadata", 500)
	}

	slog.Debug("Part uploaded", "uploadID", req.UploadID, "partNumber", req.PartNumber, "size", partSize, "etag", etag)

	return &backend.UploadPartResponse{
		ETag:       etag,
		PartNumber: req.PartNumber,
	}, nil
}

// getStoredParts retrieves all stored parts for an upload.
func (b *Backend) getStoredParts(uploadID string) ([]multipart.PartMetadata, error) {
	var parts []multipart.PartMetadata

	err := b.globalState.Scan(TableParts, multipartPartsPrefix(uploadID), func(key, value []byte) error {
		var part multipart.PartMetadata
		dec := gob.NewDecoder(bytes.NewReader(value))
		if err := dec.Decode(&part); err != nil {
			return err
		}
		parts = append(parts, part)
		return nil
	})

	if err != nil {
		return nil, err
	}

	// Sort by part number
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	return parts, nil
}

// CompleteMultipartUpload completes a multipart upload by assembling all parts.
func (b *Backend) CompleteMultipartUpload(ctx context.Context, req *backend.CompleteMultipartUploadRequest) (*backend.CompleteMultipartUploadResponse, error) {
	if req.Bucket == "" {
		return nil, backend.ErrNoSuchBucketError.WithResource(req.Bucket)
	}
	if req.Key == "" {
		return nil, backend.ErrNoSuchKeyError.WithResource(req.Key)
	}

	if _, err := b.HeadBucket(ctx, &backend.HeadBucketRequest{Bucket: req.Bucket}); err != nil {
		return nil, err
	}

	// Verify upload exists
	uploadMetadata, err := b.getUploadMetadata(req.UploadID)
	if err != nil {
		return nil, err
	}

	// Verify bucket and key match
	if uploadMetadata.Bucket != req.Bucket || uploadMetadata.Key != req.Key {
		return nil, backend.NewS3Error(backend.ErrInvalidPart, "Bucket or key does not match upload", 400)
	}

	// Get all stored parts
	storedParts, err := b.getStoredParts(req.UploadID)
	if err != nil {
		slog.Error("Failed to get stored parts", "uploadID", req.UploadID, "error", err)
		return nil, backend.NewS3Error(backend.ErrInternalError, "Failed to retrieve parts", 500)
	}

	// Validate parts
	if err := multipart.ValidatePartsForCompletion(req.Parts, storedParts); err != nil {
		return nil, err
	}

	// Create a map for quick part lookup
	storedMap := make(map[int]multipart.PartMetadata, len(storedParts))
	for _, p := range storedParts {
		storedMap[p.PartNumber] = p
	}

	partETags := make([]string, len(req.Parts))
	var finalSize int64
	for i, part := range req.Parts {
		stored := storedMap[part.PartNumber]
		partETags[i] = multipart.NormalizeETag(stored.ETag)
		if stored.Size < 0 || stored.Size > multipart.MaxObjectSize-finalSize {
			return nil, backend.NewS3Error(backend.ErrEntityTooLarge, "Completed object exceeds maximum size", 400)
		}
		finalSize += stored.Size
	}

	objectHash := s3db.GenObjectHash(req.Bucket, req.Key)
	pipeReader, pipeWriter := io.Pipe()
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() (producerErr error) {
		defer func() { _ = pipeWriter.CloseWithError(producerErr) }()
		for _, part := range req.Parts {
			data, err := b.getPartData(groupCtx, req.Bucket, req.Key, req.UploadID, part.PartNumber)
			if err != nil {
				return fmt.Errorf("retrieve multipart part %d: %w", part.PartNumber, err)
			}
			if _, err := pipeWriter.Write(data); err != nil {
				return fmt.Errorf("stream multipart part %d: %w", part.PartNumber, err)
			}
		}
		return nil
	})
	group.Go(func() (storeErr error) {
		defer func() { _ = pipeReader.CloseWithError(storeErr) }()
		_, storeErr = b.putObjectViaQUIC(groupCtx, req.Bucket, req.Key, pipeReader, finalSize, objectHash)
		if storeErr != nil {
			return fmt.Errorf("store completed multipart object: %w", storeErr)
		}
		return nil
	})
	if err := group.Wait(); err != nil {
		slog.Error("Failed to store final object", "uploadID", req.UploadID, "error", err)
		return nil, mapPutErr(err)
	}

	// Store object metadata (same as regular PutObject)
	objectToShardNodes := ObjectToShardNodes{
		Object: objectHash,
		Size:   finalSize,
	}

	// Use objectHash for hash ring placement - must match what putObjectViaQUIC uses
	hashRingShards, err := b.hashRing.GetClosestN(objectHash[:], b.rsDataShard+b.rsParityShard)
	if err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, "Failed to get shard placement", 500)
	}

	objectToShardNodes.DataShardNodes = make([]uint32, b.rsDataShard)
	objectToShardNodes.ParityShardNodes = make([]uint32, b.rsParityShard)

	for i := 0; i < b.rsDataShard; i++ {
		objectToShardNodes.DataShardNodes[i], _ = NodeToUint32(hashRingShards[i].String())
	}
	for i := 0; i < b.rsParityShard; i++ {
		objectToShardNodes.ParityShardNodes[i], _ = NodeToUint32(hashRingShards[b.rsDataShard+i].String())
	}

	var shardBuf bytes.Buffer
	shardEnc := gob.NewEncoder(&shardBuf)
	if err := shardEnc.Encode(objectToShardNodes); err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, "Failed to encode shard metadata", 500)
	}

	if err := b.globalState.Set(TableObjects, objectHash[:], shardBuf.Bytes()); err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, "Failed to store object metadata", 500)
	}

	// Store ARN key -> object hash mapping
	arnKey := []byte(arnObjectPrefixPut + req.Bucket + "/" + req.Key)
	if err := b.globalState.Set(TableObjects, arnKey, objectHash[:]); err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, "Failed to store ARN mapping", 500)
	}

	// Clean up: delete parts and upload metadata
	if err := b.cleanupMultipartUpload(ctx, req.Bucket, req.Key, req.UploadID, req.Parts); err != nil {
		slog.Warn("Failed to cleanup multipart upload", "uploadID", req.UploadID, "error", err)
		// Don't fail the request, cleanup is best-effort
	}

	// Calculate multipart ETag
	finalETag := multipart.CalculateMultipartETag(partETags, len(req.Parts))

	slog.Debug("Multipart upload completed", "bucket", req.Bucket, "key", req.Key, "uploadID", req.UploadID, "parts", len(req.Parts))

	return &backend.CompleteMultipartUploadResponse{
		Location: fmt.Sprintf("/%s/%s", req.Bucket, req.Key),
		Bucket:   req.Bucket,
		Key:      req.Key,
		ETag:     finalETag,
	}, nil
}

// getPartData retrieves the data for a specific part.
func (b *Backend) getPartData(ctx context.Context, bucket, key, uploadID string, partNumber int) ([]byte, error) {
	// Look up shard location using the key format from UploadPart
	partShardKey := fmt.Sprintf("part:%s:%05d", uploadID, partNumber)

	data, err := b.globalState.Get(TableObjects, []byte(partShardKey))
	if err != nil {
		return nil, fmt.Errorf("part not found: uploadID=%s part=%d", uploadID, partNumber)
	}

	var shardNodes ObjectToShardNodes
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&shardNodes); err != nil {
		return nil, err
	}

	// Create RS decoder
	enc, err := reedsolomon.NewStream(b.rsDataShard, b.rsParityShard)
	if err != nil {
		return nil, fmt.Errorf("failed to create Reed-Solomon decoder: %w", err)
	}

	// Construct the part object key for shard retrieval
	partKey := partObjectKey(bucket, key, uploadID, partNumber)
	if shardNodes.Size == 0 {
		return []byte{}, nil
	}
	if b.rsDataShard == 1 && b.rsParityShard == 0 {
		if len(shardNodes.DataShardNodes) != 1 {
			return nil, fmt.Errorf("part %d has %d data nodes, want 1", partNumber, len(shardNodes.DataShardNodes))
		}
		reader, err := b.shards.GetShard(ctx, int(shardNodes.DataShardNodes[0]), quicserver.ObjectRequest{
			Bucket:     bucket,
			Object:     partKey,
			RangeStart: -1,
			RangeEnd:   -1,
			ShardIndex: 0,
		})
		if err != nil {
			return nil, fmt.Errorf("get part %d shard: %w", partNumber, err)
		}
		defer reader.Close()
		partData, err := io.ReadAll(io.LimitReader(reader, shardNodes.Size))
		if err != nil {
			return nil, fmt.Errorf("read part %d shard: %w", partNumber, err)
		}
		if int64(len(partData)) != shardNodes.Size {
			return nil, fmt.Errorf("read part %d shard: got %d bytes, want %d", partNumber, len(partData), shardNodes.Size)
		}
		return partData, nil
	}

	// Use the existing reconstructObject from get.go
	buf, err := b.reconstructObject(ctx, bucket, partKey, shardNodes, enc, shardNodes.Size)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// cleanupMultipartUpload removes all part shards (via QUIC), part/upload metadata,
// and the shard-location map for an upload. Shard deletes are best-effort: a per-node
// failure is logged and skipped, never failing the complete/abort request.
func (b *Backend) cleanupMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []backend.CompletedPart) error {
	for _, part := range parts {
		partShardKey := fmt.Sprintf("part:%s:%05d", uploadID, part.PartNumber)

		// Drop the physical part shards before removing the shard-location map. A missing
		// or corrupt map, or a per-node delete failure, is logged and skipped — cleanup is
		// best-effort and must not fail the complete/abort request.
		if data, err := b.globalState.Get(TableObjects, []byte(partShardKey)); err != nil {
			slog.Warn("cleanup: part shard map missing, skipping shard delete", "uploadID", uploadID, "part", part.PartNumber)
		} else {
			var nodes ObjectToShardNodes
			if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&nodes); err != nil {
				slog.Warn("cleanup: part shard map corrupt, skipping shard delete", "uploadID", uploadID, "part", part.PartNumber, "error", err)
			} else {
				partObjKey := partObjectKey(bucket, key, uploadID, part.PartNumber)
				if err := b.deleteObjectViaQUIC(ctx, bucket, partObjKey, nodes.Object, nodes); err != nil {
					slog.Error("cleanup: QUIC shard delete failed, continuing", "uploadID", uploadID, "part", part.PartNumber, "error", err)
				}
			}
		}

		if err := b.globalState.Delete(TableParts, multipartPartKey(uploadID, part.PartNumber)); err != nil {
			slog.Warn("Failed to delete part metadata", "uploadID", uploadID, "part", part.PartNumber, "error", err)
		}

		if err := b.globalState.Delete(TableObjects, []byte(partShardKey)); err != nil {
			slog.Warn("Failed to delete part shard metadata", "uploadID", uploadID, "part", part.PartNumber, "error", err)
		}
	}

	// Delete upload metadata
	if err := b.globalState.Delete(TableMultipart, multipartUploadKey(uploadID)); err != nil {
		return err
	}

	return nil
}

// AbortMultipartUpload aborts a multipart upload and cleans up all parts.
func (b *Backend) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	if bucket == "" {
		return backend.ErrNoSuchBucketError.WithResource(bucket)
	}
	if key == "" {
		return backend.ErrNoSuchKeyError.WithResource(key)
	}

	if _, err := b.HeadBucket(ctx, &backend.HeadBucketRequest{Bucket: bucket}); err != nil {
		return err
	}

	// Verify upload exists
	uploadMetadata, err := b.getUploadMetadata(uploadID)
	if err != nil {
		return err
	}

	// Verify bucket and key match
	if uploadMetadata.Bucket != bucket || uploadMetadata.Key != key {
		return backend.NewS3Error(backend.ErrInvalidPart, "Bucket or key does not match upload", 400)
	}

	// Get all stored parts
	storedParts, err := b.getStoredParts(uploadID)
	if err != nil {
		slog.Warn("Failed to get stored parts for cleanup", "uploadID", uploadID, "error", err)
		storedParts = []multipart.PartMetadata{}
	}

	// Convert to CompletedPart for cleanup
	parts := make([]backend.CompletedPart, len(storedParts))
	for i, p := range storedParts {
		parts[i] = backend.CompletedPart{
			PartNumber: p.PartNumber,
			ETag:       p.ETag,
		}
	}

	// Clean up
	if err := b.cleanupMultipartUpload(ctx, bucket, key, uploadID, parts); err != nil {
		slog.Warn("Failed to cleanup multipart upload", "uploadID", uploadID, "error", err)
	}

	slog.Debug("Multipart upload aborted", "bucket", bucket, "key", key, "uploadID", uploadID)

	return nil
}
