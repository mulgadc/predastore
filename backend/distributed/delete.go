package distributed

import (
	"bytes"
	"context"
	"encoding/gob"
	"log/slog"
	"sync"
	"time"

	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/quic/quicclient"
	"github.com/mulgadc/predastore/quic/quicserver"
	s3db "github.com/mulgadc/predastore/s3db"
)

// arnObjectPrefixDel is the ARN prefix for object keys
const arnObjectPrefixDel = "arn:aws:s3:::"

// deletedObjectPrefix is the key prefix for tracking deleted objects
// Format: deleted:<bucket>/<key> -> DeletedObjectInfo (gob encoded)
// This allows querying all deleted objects for a future compaction coordinator
const deletedObjectPrefix = "deleted:"

// DeletedObjectInfo tracks a deleted object for compaction coordination
type DeletedObjectInfo struct {
	Bucket         string   `json:"bucket"`
	Key            string   `json:"key"`
	ObjectHash     [32]byte `json:"object_hash"`
	DeletedAt      int64    `json:"deleted_at"`       // Unix timestamp
	DataShardNodes []uint32 `json:"data_shard_nodes"` // Which nodes had data shards
	ParityNodes    []uint32 `json:"parity_nodes"`     // Which nodes had parity shards
}

// DeleteObject removes an object from the distributed storage
func (b *Backend) DeleteObject(ctx context.Context, req *backend.DeleteObjectRequest) error {
	if req.Bucket == "" {
		return backend.ErrNoSuchBucketError.WithResource(req.Bucket)
	}
	if req.Key == "" {
		return backend.ErrNoSuchKeyError.WithResource(req.Key)
	}

	objectHash := s3db.GenObjectHash(req.Bucket, req.Key)

	// Check if object exists and get shard node info
	data, err := b.globalState.Get(TableObjects, objectHash[:])
	if err != nil {
		return backend.ErrNoSuchKeyError.WithResource(req.Key)
	}

	// Decode existing metadata to get shard locations
	var objectToShardNodes ObjectToShardNodes
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&objectToShardNodes); err != nil {
		return backend.NewS3Error(backend.ErrInternalError, "corrupt metadata", 500)
	}

	// Delete shards from nodes via QUIC if enabled
	if b.useQUIC {
		if err := b.deleteObjectViaQUIC(ctx, req.Bucket, req.Key, objectHash, objectToShardNodes); err != nil {
			slog.Error("deleteObjectViaQUIC failed", "error", err)
			// Continue with local cleanup even if QUIC delete fails
		}
	}

	// Track deleted object for future compaction coordination
	// TODO: A future compaction coordinator can scan these entries to know which
	// shard servers have deleted data that needs WAL compaction
	deletedInfo := DeletedObjectInfo{
		Bucket:         req.Bucket,
		Key:            req.Key,
		ObjectHash:     objectHash,
		DeletedAt:      time.Now().Unix(),
		DataShardNodes: objectToShardNodes.DataShardNodes,
		ParityNodes:    objectToShardNodes.ParityShardNodes,
	}

	var deletedBuf bytes.Buffer
	if err := gob.NewEncoder(&deletedBuf).Encode(deletedInfo); err == nil {
		deletedKey := []byte(deletedObjectPrefix + req.Bucket + "/" + req.Key)
		_ = b.globalState.Set(TableObjects, deletedKey, deletedBuf.Bytes()) // Best effort
	}

	// Delete the object hash metadata from global state
	err = b.globalState.Delete(TableObjects, objectHash[:])
	if err != nil {
		return backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	}

	// Delete the ARN key from global state (for listing)
	arnKey := []byte(arnObjectPrefixDel + req.Bucket + "/" + req.Key)
	_ = b.globalState.Delete(TableObjects, arnKey) // Best effort

	return nil
}

// deleteObjectViaQUIC sends DELETE requests to all shard nodes
func (b *Backend) deleteObjectViaQUIC(ctx context.Context, bucket, key string, objectHash [32]byte, shards ObjectToShardNodes) error {
	// Collect all node IDs (data + parity)
	allNodes := make([]uint32, 0, len(shards.DataShardNodes)+len(shards.ParityShardNodes))
	allNodes = append(allNodes, shards.DataShardNodes...)
	allNodes = append(allNodes, shards.ParityShardNodes...)

	// Send DELETE to all nodes concurrently
	var wg sync.WaitGroup
	errCh := make(chan error, len(allNodes))

	for _, nodeNum := range allNodes {
		wg.Add(1)
		go func(node uint32) {
			defer wg.Done()

			addr := b.getNodeAddr(int(node))
			client, err := quicclient.Dial(ctx, addr)
			if err != nil {
				slog.Error("deleteObjectViaQUIC: dial failed", "node", node, "addr", addr, "error", err)
				errCh <- err
				return
			}
			defer client.Close()

			delReq := quicserver.DeleteRequest{
				Bucket:     bucket,
				Object:     key,
				ObjectHash: objectHash,
			}

			resp, err := client.Delete(ctx, delReq)
			if err != nil {
				slog.Error("deleteObjectViaQUIC: delete failed", "node", node, "error", err)
				errCh <- err
				return
			}

			if !resp.Deleted {
				slog.Warn("deleteObjectViaQUIC: shard not found on node", "node", node)
			} else {
				slog.Info("deleteObjectViaQUIC: deleted shard", "node", node, "bucket", bucket, "key", key)
			}
		}(nodeNum)
	}

	wg.Wait()
	close(errCh)

	// Return first error if any (but all deletes are attempted)
	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return nil
}
