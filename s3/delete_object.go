package s3

import (
	"bytes"
	"encoding/gob"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/mulgadc/predastore/internal/gateway/model"
)

// deletedObjectPrefix keys the tombstone left behind by a delete, so a future
// compaction coordinator can find which nodes hold dead shards.
const deletedObjectPrefix = "deleted:"

// DeletedObjectInfo tracks a deleted object for compaction coordination.
type DeletedObjectInfo struct {
	Bucket         string   `json:"bucket"`
	Key            string   `json:"key"`
	ObjectHash     [32]byte `json:"object_hash"`
	DeletedAt      int64    `json:"deleted_at"`       // Unix timestamp
	DataShardNodes []uint32 `json:"data_shard_nodes"` // Which nodes had data shards
	ParityNodes    []uint32 `json:"parity_nodes"`     // Which nodes had parity shards
}

// deleteObject serves DELETE /{bucket}/{key} with no uploadId.
func (s *HTTP2Server) deleteObject(w http.ResponseWriter, r *http.Request) {
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

	data, err := s.stateGet(model.TableObjects, string(objectHash[:]))
	if err != nil {
		s.handleError(w, r, model.ErrNoSuchKeyError.WithResource(key))
		return
	}

	var placement ObjectToShardNodes
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&placement); err != nil { //nolint:gosec // G709: the input is state this gateway wrote, not client data.
		s.handleError(w, r, model.NewS3Error(model.ErrInternalError, "corrupt metadata", 500))
		return
	}

	// A node that refuses its shard delete leaves garbage the compactor will
	// reclaim; the object must still disappear from state.
	if err := s.deleteObjectViaQUIC(ctx, bucket, key, objectHash, placement); err != nil {
		slog.ErrorContext(ctx, "deleteObjectViaQUIC failed", "error", err)
	}

	// TODO: A future compaction coordinator can scan these entries to know which
	// shard servers have deleted data that needs WAL compaction.
	deletedInfo := DeletedObjectInfo{
		Bucket:         bucket,
		Key:            key,
		ObjectHash:     objectHash,
		DeletedAt:      time.Now().Unix(),
		DataShardNodes: placement.DataShardNodes,
		ParityNodes:    placement.ParityShardNodes,
	}
	var deletedBuf bytes.Buffer
	if err := gob.NewEncoder(&deletedBuf).Encode(deletedInfo); err == nil {
		_ = s.statePut(model.TableObjects, deletedObjectPrefix+bucket+"/"+key, deletedBuf.Bytes()) // Best effort
	}

	if err := s.stateDelete(model.TableObjects, string(objectHash[:])); err != nil {
		s.handleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
		return
	}

	_ = s.stateDelete(model.TableObjects, objectARN(bucket, key)) // Best effort

	w.WriteHeader(http.StatusNoContent)
}
