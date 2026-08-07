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

	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/storage/engine"
	"github.com/mulgadc/predastore/internal/transport"
)

// Server serves shard rpc requests for one storage node. A process running
// several nodes builds one Server per node, each carried by its own rpc
// server, so the shard service never learns that it has siblings.
type Server struct {
	id    uint64
	store *engine.Store
}

// NewServer builds the shard service for one node's store.
func NewServer(id uint64, st *engine.Store) *Server {
	return &Server{id: id, store: st}
}

// ID is the node this server serves.
func (s *Server) ID() uint64 { return s.id }

// Run holds the node open until ctx is cancelled, then closes its store. The
// rpc server draining is the caller's concern; by the time Run returns no
// handler is still touching the store.
func (s *Server) Run(ctx context.Context) error {
	<-ctx.Done()
	return s.store.Close()
}

// Register installs the storage service handlers on the mux.
func (s *Server) Register(mux *rpc.Mux) {
	rpc.RegisterHandler(mux, OpShardGet, s.handleGet)
	rpc.RegisterHandler(mux, OpShardPut, s.handlePut)
	rpc.RegisterHandler(mux, OpShardDelete, s.handleDelete)
}

// respondShard writes the newline-terminated JSON envelope; get responses
// stream the shard bytes after it.
func respondShard(stream transport.Stream, resp *ShardResponse) error {
	return json.NewEncoder(stream).Encode(resp)
}

func (s *Server) handlePut(ctx context.Context, h ShardRequest, stream transport.Stream) error {
	st := s.store
	if h.ShardSize <= 0 {
		return respondShard(stream, &ShardResponse{Err: "no shard size specified"})
	}

	writer, err := st.Append(h.ObjectHash, h.ShardIndex, h.ShardSize)
	if err != nil {
		// Drain the client's in-flight body so the reply is carried
		// instead of racing a stream reset.
		if _, derr := io.Copy(io.Discard, io.LimitReader(stream, h.ShardSize)); derr != nil {
			return fmt.Errorf("drain body after append error: %w", derr)
		}
		if errors.Is(err, engine.ErrStoreFull) {
			return respondShard(stream, &ShardResponse{Err: ErrCodeStoreFull})
		}
		return respondShard(stream, &ShardResponse{Err: fmt.Sprintf("append: %v", err)})
	}

	if _, err := writer.ReadFrom(io.LimitReader(stream, h.ShardSize)); err != nil {
		return respondShard(stream, &ShardResponse{Err: fmt.Sprintf("write: %v", err)})
	}
	if err := writer.Close(); err != nil {
		return respondShard(stream, &ShardResponse{Err: fmt.Sprintf("commit: %v", err)})
	}

	// Surface nearfull pressure on success too, so callers can back off
	// before a write is ever outright rejected.
	return respondShard(stream, &ShardResponse{ShardSize: h.ShardSize, PoolNearFull: st.NearFull()})
}

func (s *Server) handleGet(ctx context.Context, h ShardRequest, stream transport.Stream) error {
	st := s.store

	reader, err := st.Lookup(h.ObjectHash, h.ShardIndex)
	if err != nil {
		return respondShard(stream, &ShardResponse{Err: ErrCodeNotFound})
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
		return respondShard(stream, &ShardResponse{Err: "invalid range"})
	}
	responseSize := rangeEnd - rangeStart + 1

	if err := respondShard(stream, &ShardResponse{BodyLen: responseSize}); err != nil {
		return fmt.Errorf("write envelope: %w", err)
	}
	if _, err := stream.ReadFrom(io.NewSectionReader(reader, rangeStart, responseSize)); err != nil {
		return fmt.Errorf("stream shard: %w", err)
	}
	return nil
}

func (s *Server) handleDelete(ctx context.Context, h ShardRequest, stream transport.Stream) error {
	st := s.store
	deleted, err := st.Delete(h.ObjectHash, h.ShardIndex)
	if err != nil {
		return respondShard(stream, &ShardResponse{Err: err.Error()})
	}
	return respondShard(stream, &ShardResponse{Deleted: deleted})
}
