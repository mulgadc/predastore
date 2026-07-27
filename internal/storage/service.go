// Package storage serves and consumes erasure-coded shard operations over
// rpc streams. The service side fronts the shard stores hosted in a process;
// the client side is used by the distributed backend to reach storage nodes
// wherever they run.
package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/mulgadc/predastore/internal/wire"
	"github.com/mulgadc/predastore/s3db"
	"github.com/mulgadc/predastore/store"
)

// Service serves shard rpc requests for the storage nodes hosted in this
// process, one shard store per node.
type Service struct {
	mu     sync.RWMutex
	stores map[uint64]*store.Store
}

func NewService() *Service {
	return &Service{stores: make(map[uint64]*store.Store)}
}

// AddNode registers a locally hosted storage node's shard store.
func (s *Service) AddNode(id uint64, st *store.Store) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stores[id] = st
}

func (s *Service) store(id uint64) (*store.Store, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, ok := s.stores[id]
	if !ok {
		return nil, fmt.Errorf("storage node %d is not hosted here", id)
	}
	return st, nil
}

// Register installs the storage service handlers on the mux.
func (s *Service) Register(mux *rpc.Mux) {
	rpc.RegisterHandler(mux, wire.OpShardGet, s.handleGet)
	rpc.RegisterHandler(mux, wire.OpShardPut, s.handlePut)
	rpc.RegisterHandler(mux, wire.OpShardDelete, s.handleDelete)
}

// respondShard writes the newline-terminated JSON envelope; get responses
// stream the shard bytes after it.
func respondShard(stream transport.Stream, resp *wire.ShardResponse) error {
	return json.NewEncoder(stream).Encode(resp)
}

func (s *Service) handlePut(ctx context.Context, h wire.ShardRequest, stream transport.Stream) error {
	st, err := s.store(h.Target)
	if err != nil {
		return respondShard(stream, &wire.ShardResponse{Err: err.Error()})
	}
	if h.ShardSize <= 0 {
		return respondShard(stream, &wire.ShardResponse{Err: "no shard size specified"})
	}

	writer, err := st.Append(h.ObjectHash, h.ShardIndex, h.ShardSize)
	if err != nil {
		// Drain the client's in-flight body so the reply is carried
		// instead of racing a stream reset.
		if _, derr := io.Copy(io.Discard, io.LimitReader(stream, h.ShardSize)); derr != nil {
			return fmt.Errorf("drain body after append error: %w", derr)
		}
		if errors.Is(err, store.ErrStoreFull) {
			return respondShard(stream, &wire.ShardResponse{Err: wire.ErrCodeStoreFull})
		}
		return respondShard(stream, &wire.ShardResponse{Err: fmt.Sprintf("append: %v", err)})
	}

	if _, err := writer.ReadFrom(io.LimitReader(stream, h.ShardSize)); err != nil {
		return respondShard(stream, &wire.ShardResponse{Err: fmt.Sprintf("write: %v", err)})
	}
	if err := writer.Close(); err != nil {
		return respondShard(stream, &wire.ShardResponse{Err: fmt.Sprintf("commit: %v", err)})
	}

	// Surface nearfull pressure on success too, so callers can back off
	// before a write is ever outright rejected.
	return respondShard(stream, &wire.ShardResponse{ShardSize: h.ShardSize, PoolNearFull: st.NearFull()})
}

func (s *Service) handleGet(ctx context.Context, h wire.ShardRequest, stream transport.Stream) error {
	st, err := s.store(h.Target)
	if err != nil {
		return respondShard(stream, &wire.ShardResponse{Err: err.Error()})
	}

	objectHash := s3db.GenObjectHash(h.Bucket, h.Object)
	reader, err := st.Lookup(objectHash, h.ShardIndex)
	if err != nil {
		return respondShard(stream, &wire.ShardResponse{Err: wire.ErrCodeNotFound})
	}
	defer reader.Close()

	totalSize := reader.Size()

	// Values >= 0 are explicit range bounds (including 0 for "start from
	// beginning"); negative means unset and falls back to the full shard.
	rangeStart := h.RangeStart
	rangeEnd := h.RangeEnd
	if rangeStart < 0 {
		rangeStart = 0
	}
	if rangeEnd < 0 || rangeEnd >= totalSize {
		rangeEnd = totalSize - 1
	}
	if rangeStart > rangeEnd || rangeStart >= totalSize {
		return respondShard(stream, &wire.ShardResponse{Err: "invalid range"})
	}
	responseSize := rangeEnd - rangeStart + 1

	if err := respondShard(stream, &wire.ShardResponse{BodyLen: responseSize}); err != nil {
		return fmt.Errorf("write envelope: %w", err)
	}
	if _, err := stream.ReadFrom(io.NewSectionReader(reader, rangeStart, responseSize)); err != nil {
		return fmt.Errorf("stream shard: %w", err)
	}
	return nil
}

func (s *Service) handleDelete(ctx context.Context, h wire.ShardRequest, stream transport.Stream) error {
	st, err := s.store(h.Target)
	if err != nil {
		return respondShard(stream, &wire.ShardResponse{Err: err.Error()})
	}
	deleted, err := st.Delete(h.ObjectHash, h.ShardIndex)
	if err != nil {
		return respondShard(stream, &wire.ShardResponse{Err: err.Error()})
	}
	return respondShard(stream, &wire.ShardResponse{Deleted: deleted})
}
