// Package blob serves and consumes opaque value operations over rpc streams.
// The service side fronts the stores hosted in a process; the client side is
// used by the distributed backend to reach blob nodes wherever they run.
package blob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/mulgadc/predastore/internal/blob/engine"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
)

// Server serves rpc requests for one blob node. A process running several
// nodes builds one Server per node, each carried by its own rpc server, so
// the storage service never learns that it has siblings.
type Server struct {
	id    config.NodeID
	store *engine.Store
}

// NewServer builds the storage service for one node's store.
func NewServer(id config.NodeID, st *engine.Store) *Server {
	return &Server{id: id, store: st}
}

// ID is the node this server serves.
func (s *Server) ID() config.NodeID { return s.id }

// Run holds the node open until ctx is cancelled, then closes its store. The
// rpc server draining is the caller's concern; by the time Run returns no
// handler is still touching the store.
func (s *Server) Run(ctx context.Context) error {
	<-ctx.Done()
	return s.store.Close()
}

// Register installs the storage service handlers on the mux.
func (s *Server) Register(mux *rpc.Mux) {
	rpc.RegisterHandler(mux, OpGet, s.handleGet)
	rpc.RegisterHandler(mux, OpPut, s.handlePut)
	rpc.RegisterHandler(mux, OpDelete, s.handleDelete)
}

// respond writes the newline-terminated JSON envelope; get responses stream
// the body bytes after it.
func respond(stream transport.Stream, resp *Response) error {
	return json.NewEncoder(stream).Encode(resp)
}

func (s *Server) handlePut(ctx context.Context, h Request, stream transport.Stream) error {
	st := s.store
	if h.Size <= 0 {
		return respond(stream, &Response{Err: "no size specified"})
	}

	writer, err := st.Append(h.Key, h.Index, h.Size)
	if err != nil {
		// Drain the client's in-flight body so the reply is carried
		// instead of racing a stream reset.
		if _, derr := io.Copy(io.Discard, io.LimitReader(stream, h.Size)); derr != nil {
			return fmt.Errorf("drain body after append error: %w", derr)
		}
		if errors.Is(err, engine.ErrStoreFull) {
			return respond(stream, &Response{Err: ErrCodeStoreFull})
		}
		return respond(stream, &Response{Err: fmt.Sprintf("append: %v", err)})
	}

	if _, err := writer.ReadFrom(io.LimitReader(stream, h.Size)); err != nil {
		return respond(stream, &Response{Err: fmt.Sprintf("write: %v", err)})
	}
	if err := writer.Close(); err != nil {
		return respond(stream, &Response{Err: fmt.Sprintf("commit: %v", err)})
	}

	// Surface nearfull pressure on success too, so callers can back off
	// before a write is ever outright rejected.
	return respond(stream, &Response{Size: h.Size, PoolNearFull: st.NearFull()})
}

func (s *Server) handleGet(ctx context.Context, h Request, stream transport.Stream) error {
	st := s.store

	reader, err := st.Lookup(h.Key, h.Index)
	if err != nil {
		return respond(stream, &Response{Err: ErrCodeNotFound})
	}
	defer reader.Close()

	totalSize := reader.Size()

	// Values >= 0 are explicit range bounds (including 0 for "start from
	// beginning"); negative means unset and falls back to the whole value.
	rangeStart := h.RangeStart
	rangeEnd := h.RangeEnd
	if rangeStart < 0 {
		rangeStart = 0
	}
	if rangeEnd < 0 || rangeEnd >= totalSize {
		rangeEnd = totalSize - 1
	}
	if rangeStart > rangeEnd || rangeStart >= totalSize {
		return respond(stream, &Response{Err: "invalid range"})
	}
	responseSize := rangeEnd - rangeStart + 1

	if err := respond(stream, &Response{BodyLen: responseSize}); err != nil {
		return fmt.Errorf("write envelope: %w", err)
	}
	if _, err := stream.ReadFrom(io.NewSectionReader(reader, rangeStart, responseSize)); err != nil {
		return fmt.Errorf("stream body: %w", err)
	}
	return nil
}

func (s *Server) handleDelete(ctx context.Context, h Request, stream transport.Stream) error {
	st := s.store
	deleted, err := st.Delete(h.Key, h.Index)
	if err != nil {
		return respond(stream, &Response{Err: err.Error()})
	}
	return respond(stream, &Response{Deleted: deleted})
}
