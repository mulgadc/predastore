package handlers

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
)

// DeletedObjectPrefix keys the tombstone left behind by a delete whose shard
// fan-out failed, so the reaper sweep can find which nodes still hold shards
// it needs to reclaim. Exported so the reaper package, which pages only these
// rows, can build the same scan prefix without duplicating it.
const DeletedObjectPrefix = "deleted:"

// DeletedObjectInfo names the shards a failed fan-out left behind, so the
// reaper sweep can reclaim them once it is safe to. It is load-bearing: once
// the placement record is gone, this is the only record of where the shards
// are.
type DeletedObjectInfo struct {
	Bucket         string          `json:"bucket"`
	Key            string          `json:"key"`
	ObjectHash     [32]byte        `json:"object_hash"`
	DeletedAt      int64           `json:"deleted_at"`       // Unix timestamp
	WriteEpoch     uint64          `json:"write_epoch"`      // The placement's write epoch, so the reaper can tell a recreate from its own delete still in flight
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

		if err := deleteStoredObject(ctx, mc, bc, bucket, key); err != nil {
			HandleError(w, r, err)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})
}

// deleteStoredObject removes one object: its shards, then the metadata that
// names them. A key that is not there returns model.ErrNoSuchKeyError, which
// the single-object route answers as 404 and the batch route reports as
// deleted — S3's delete is idempotent only in the batch form.
//
// The shard fan-out runs inline, same as ever. Its failure does not fail the
// delete: instead a tombstone naming the shards is written, and the reaper
// sweep reclaims them later. On success no tombstone is written at all, so
// the common path pays nothing extra. A tombstone write that itself fails
// does fail the delete and leaves both index entries in place — once the
// placement record is gone the tombstone would be the only surviving record
// of which nodes hold the shards, and losing it there leaks them permanently.
//
// The caller has already established that the bucket exists.
func deleteStoredObject(ctx context.Context, mc MetaClient, bc BlobClient, bucket, key string) error {
	objectHash := model.ObjectHash(bucket, key)

	data, err := metaGet(ctx, mc, model.TableObjects, string(objectHash[:]))
	if err != nil {
		return model.ErrNoSuchKeyError.WithResource(key)
	}

	place, err := DecodePlacement(data)
	if err != nil {
		return model.NewS3Error(model.ErrInternalError, "corrupt metadata", 500)
	}

	if err := deleteObject(ctx, bc, bucket, key, objectHash, place); err != nil {
		slog.ErrorContext(ctx, "deleteObject failed", "error", err)

		deletedInfo := DeletedObjectInfo{
			Bucket:         bucket,
			Key:            key,
			ObjectHash:     objectHash,
			DeletedAt:      time.Now().Unix(),
			WriteEpoch:     place.WriteEpoch,
			DataShardNodes: place.DataShardNodes,
			ParityNodes:    place.ParityShardNodes,
		}
		var deletedBuf bytes.Buffer
		if err := gob.NewEncoder(&deletedBuf).Encode(deletedInfo); err != nil {
			return model.NewS3Error(model.ErrInternalError, fmt.Sprintf("encode tombstone: %v", err), 500)
		}
		if err := metaPut(ctx, mc, model.TableObjects, DeletedObjectPrefix+bucket+"/"+key, deletedBuf.Bytes()); err != nil {
			return model.NewS3Error(model.ErrInternalError, err.Error(), 500)
		}
	}

	if err := metaDelete(ctx, mc, model.TableObjects, objectARN(bucket, key)); err != nil {
		return model.NewS3Error(model.ErrInternalError, err.Error(), 500)
	}

	if err := metaDelete(ctx, mc, model.TableObjects, string(objectHash[:])); err != nil {
		return model.NewS3Error(model.ErrInternalError, err.Error(), 500)
	}

	return nil
}
