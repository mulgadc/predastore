// Package state serves and consumes global-state operations over rpc streams.
// The service side fronts one raft replica; the client side reaches replicas
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
	"github.com/mulgadc/predastore/s3db"
)

// Service serves state rpc requests for one replica. A process running
// several replicas builds one Service per node, each on its own rpc server,
// so the service itself never learns that it has siblings.
type Service struct {
	id    uint64
	node  *s3db.RaftNode
	layer *s3db.RPCStreamLayer
}

// NewService builds the service for one replica and its raft stream layer.
func NewService(id uint64, node *s3db.RaftNode, layer *s3db.RPCStreamLayer) *Service {
	return &Service{id: id, node: node, layer: layer}
}

// ID is the node this service serves.
func (s *Service) ID() uint64 { return s.id }

// Run holds the replica open until ctx is cancelled, then shuts raft down.
func (s *Service) Run(ctx context.Context) error {
	<-ctx.Done()
	return s.node.Close()
}

// Register installs the state service handlers on the mux.
func (s *Service) Register(mux *rpc.Mux) {
	rpc.RegisterHandler(mux, OpRaftDial, s.handleRaftDial)
	rpc.RegisterHandler(mux, OpStateGet, s.handleGet)
	rpc.RegisterHandler(mux, OpStatePut, s.handlePut)
	rpc.RegisterHandler(mux, OpStateDelete, s.handleDelete)
	rpc.RegisterHandler(mux, OpStateScan, s.handleScan)
}

// handleRaftDial hands the stream to the target replica's raft transport and
// holds it for the connection's lifetime.
func (s *Service) handleRaftDial(ctx context.Context, h RaftDial, stream transport.Stream) error {
	return s.layer.Deliver(ctx, stream)
}

// respond writes the closing JSON envelope; the rpc server closes the stream
// once the handler returns.
func respond(stream transport.Stream, resp *StateResponse) error {
	return json.NewEncoder(stream).Encode(resp)
}

func (s *Service) handleGet(ctx context.Context, h StateRequest, stream transport.Stream) error {
	value, err := s.node.Get(h.Table, h.Key)
	switch {
	case errors.Is(err, badger.ErrKeyNotFound):
		return respond(stream, &StateResponse{Err: ErrCodeNotFound})
	case err != nil:
		return respond(stream, &StateResponse{Err: err.Error()})
	}
	return respond(stream, &StateResponse{Value: value})
}

func (s *Service) handlePut(ctx context.Context, h StateRequest, stream transport.Stream) error {
	// The value is the stream body; the client half-closes after writing.
	value, err := io.ReadAll(stream)
	if err != nil {
		return fmt.Errorf("read put value: %w", err)
	}
	return respond(stream, writeResult(s.node, s.node.Put(h.Table, h.Key, value)))
}

func (s *Service) handleDelete(ctx context.Context, h StateRequest, stream transport.Stream) error {
	return respond(stream, writeResult(s.node, s.node.Delete(h.Table, h.Key)))
}

// writeResult maps a consensus write outcome onto the response envelope,
// pointing the client at the leader when this replica cannot commit.
func writeResult(node *s3db.RaftNode, err error) *StateResponse {
	switch {
	case errors.Is(err, s3db.ErrNotLeader):
		return &StateResponse{Err: ErrCodeNotLeader, Leader: node.LeaderAddr()}
	case err != nil:
		return &StateResponse{Err: err.Error()}
	}
	return &StateResponse{}
}

func (s *Service) handleScan(ctx context.Context, h StateRequest, stream transport.Stream) error {
	// errScanLimit stops iteration once the limit is reached without
	// surfacing an error to the client.
	errScanLimit := errors.New("scan limit reached")
	var items []ScanItem
	err := s.node.Scan(h.Table, h.Key, func(key string, value []byte) error {
		if h.Limit > 0 && len(items) >= h.Limit {
			return errScanLimit
		}
		items = append(items, ScanItem{Key: key, Value: value})
		return nil
	})
	if err != nil && !errors.Is(err, errScanLimit) {
		return respond(stream, &StateResponse{Err: err.Error()})
	}
	return respond(stream, &StateResponse{Items: items})
}
