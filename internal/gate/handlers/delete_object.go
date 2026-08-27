package handlers

import (
	"bytes"
	"encoding/gob"
	"log/slog"
	"net/http"
	"time"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
)

// deletedObjectPrefix keys the tombstone left behind by a delete, so a future
// compaction coordinator can find which nodes hold dead shards.
const deletedObjectPrefix = "deleted:"

// DeletedObjectInfo tracks a deleted object for compaction coordination.
type DeletedObjectInfo struct {
	Bucket         string          `json:"bucket"`
	Key            string          `json:"key"`
	ObjectHash     [32]byte        `json:"object_hash"`
	DeletedAt      int64           `json:"deleted_at"`       // Unix timestamp
	DataShardNodes []config.NodeID `json:"data_shard_nodes"` // Which nodes had data shards
	ParityNodes    []config.NodeID `json:"parity_nodes"`     // Which nodes had parity shards
}

// DeleteObject serves DELETE /{bucket}/{key} with no uploadId.
func DeleteObject(mc MetaClient, bc BlobClient, cache *BucketCache) http.Handler {
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

		data, err := metaGet(ctx, mc, model.TableObjects, string(objectHash[:]))
		if err != nil {
			HandleError(w, r, model.ErrNoSuchKeyError.WithResource(key))
			return
		}

		place, err := decodePlacement(data)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "corrupt metadata", 500))
			return
		}

		// A node that refuses its shard delete leaves garbage the compactor will
		// reclaim; the object must still disappear from state.
		if err := deleteObject(ctx, bc, bucket, key, objectHash, place); err != nil {
			slog.ErrorContext(ctx, "deleteObject failed", "error", err)
		}

		// TODO: A future compaction coordinator can scan these entries to know which
		// shard servers have deleted data that needs WAL compaction.
		deletedInfo := DeletedObjectInfo{
			Bucket:         bucket,
			Key:            key,
			ObjectHash:     objectHash,
			DeletedAt:      time.Now().Unix(),
			DataShardNodes: place.DataShardNodes,
			ParityNodes:    place.ParityShardNodes,
		}
		var deletedBuf bytes.Buffer
		if err := gob.NewEncoder(&deletedBuf).Encode(deletedInfo); err == nil {
			_ = metaPut(ctx, mc, model.TableObjects, deletedObjectPrefix+bucket+"/"+key, deletedBuf.Bytes()) // Best effort
		}

		if err := metaDelete(ctx, mc, model.TableObjects, string(objectHash[:])); err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}

		_ = metaDelete(ctx, mc, model.TableObjects, objectARN(bucket, key)) // Best effort

		w.WriteHeader(http.StatusNoContent)
	})
}
