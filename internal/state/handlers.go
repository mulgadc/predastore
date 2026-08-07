// Package state serves and consumes global-state operations over rpc streams.
// The server side is one raft replica; the client side reaches replicas
// wherever they run, hiding the wire protocol from callers.
package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/dgraph-io/badger/v4"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
)

// ID is the node this replica serves.
func (s *Server) ID() uint64 { return s.id }

// Run holds the replica open until ctx is cancelled, then shuts raft down.
func (s *Server) Run(ctx context.Context) error {
	<-ctx.Done()
	return s.Close()
}

// Register installs the state service handlers on the mux.
func (s *Server) Register(mux *rpc.Mux) {
	rpc.RegisterHandler(mux, OpRaftDial, s.handleRaftDial)
	rpc.RegisterHandler(mux, OpStateGet, s.handleGet)
	rpc.RegisterHandler(mux, OpStatePut, s.handlePut)
	rpc.RegisterHandler(mux, OpStateDelete, s.handleDelete)
	rpc.RegisterHandler(mux, OpStateScan, s.handleScan)
}

// handleRaftDial hands the stream to the target replica's raft transport and
// holds it for the connection's lifetime.
func (s *Server) handleRaftDial(ctx context.Context, h RaftDial, stream transport.Stream) error {
	return s.layer.Deliver(ctx, stream)
}

// respond writes the closing JSON envelope; the rpc server closes the stream
// once the handler returns.
func respond(stream transport.Stream, resp *StateResponse) error {
	return json.NewEncoder(stream).Encode(resp)
}

func (s *Server) handleGet(ctx context.Context, h StateRequest, stream transport.Stream) error {
	// The raft node keys by Go string; the bytes survive the conversion, so
	// the key badger stores is the one the client sent.
	value, err := s.Get(string(h.Key))
	switch {
	case errors.Is(err, badger.ErrKeyNotFound):
		return respond(stream, &StateResponse{Err: ErrCodeNotFound})
	case err != nil:
		return respond(stream, &StateResponse{Err: err.Error()})
	}
	return respond(stream, &StateResponse{Value: value})
}

func (s *Server) handlePut(ctx context.Context, h StateRequest, stream transport.Stream) error {
	// The value is the stream body; the client half-closes after writing.
	value, err := io.ReadAll(stream)
	if err != nil {
		return fmt.Errorf("read put value: %w", err)
	}
	return respond(stream, s.writeResult(s.Put(string(h.Key), value)))
}

func (s *Server) handleDelete(ctx context.Context, h StateRequest, stream transport.Stream) error {
	return respond(stream, s.writeResult(s.Delete(string(h.Key))))
}

// writeResult maps a consensus write outcome onto the response envelope,
// pointing the client at the leader when this replica cannot commit.
func (s *Server) writeResult(err error) *StateResponse {
	switch {
	case errors.Is(err, ErrNotLeader):
		return &StateResponse{Err: ErrCodeNotLeader, Leader: s.LeaderAddr()}
	case err != nil:
		return &StateResponse{Err: err.Error()}
	}
	return &StateResponse{}
}

func (s *Server) handleScan(ctx context.Context, h StateRequest, stream transport.Stream) error {
	// errScanLimit stops iteration once the limit is reached without
	// surfacing an error to the client.
	errScanLimit := errors.New("scan limit reached")
	var items []ScanItem
	err := s.Scan(string(h.Key), func(key string, value []byte) error {
		if h.Limit > 0 && len(items) >= h.Limit {
			return errScanLimit
		}
		items = append(items, ScanItem{Key: []byte(key), Value: value})
		return nil
	})
	if err != nil && !errors.Is(err, errScanLimit) {
		return respond(stream, &StateResponse{Err: err.Error()})
	}
	return respond(stream, &StateResponse{Items: items})
}
