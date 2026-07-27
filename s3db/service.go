package s3db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/mulgadc/predastore/internal/wire"
)

// StateService serves state rpc requests for the state replicas hosted in
// this process. Requests carry the target replica id in the header; the
// service routes to the matching raft node or stream layer.
type StateService struct {
	mu       sync.RWMutex
	replicas map[uint64]*stateReplica
}

type stateReplica struct {
	node  *RaftNode
	layer *RPCStreamLayer
}

func NewStateService() *StateService {
	return &StateService{replicas: make(map[uint64]*stateReplica)}
}

// AddReplica registers a locally hosted state replica and its raft stream
// layer for in-band routing.
func (s *StateService) AddReplica(id uint64, node *RaftNode, layer *RPCStreamLayer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replicas[id] = &stateReplica{node: node, layer: layer}
}

func (s *StateService) replica(id uint64) (*stateReplica, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.replicas[id]
	if !ok {
		return nil, fmt.Errorf("state replica %d is not hosted here", id)
	}
	return r, nil
}

// Register installs the state service handlers on the mux.
func (s *StateService) Register(mux *rpc.Mux) {
	rpc.RegisterHandler(mux, wire.OpRaftDial, s.handleRaftDial)
	rpc.RegisterHandler(mux, wire.OpStateGet, s.handleGet)
	rpc.RegisterHandler(mux, wire.OpStatePut, s.handlePut)
	rpc.RegisterHandler(mux, wire.OpStateDelete, s.handleDelete)
	rpc.RegisterHandler(mux, wire.OpStateScan, s.handleScan)
}

// handleRaftDial hands the stream to the target replica's raft transport and
// holds it for the connection's lifetime.
func (s *StateService) handleRaftDial(ctx context.Context, h wire.RaftDial, stream transport.Stream) error {
	r, err := s.replica(h.Target)
	if err != nil {
		return err
	}
	return r.layer.Deliver(ctx, stream)
}

// respond writes the closing JSON envelope; the rpc server closes the stream
// once the handler returns.
func respond(stream transport.Stream, resp *wire.StateResponse) error {
	return json.NewEncoder(stream).Encode(resp)
}

func (s *StateService) handleGet(ctx context.Context, h wire.StateRequest, stream transport.Stream) error {
	r, err := s.replica(h.Target)
	if err != nil {
		return respond(stream, &wire.StateResponse{Err: err.Error()})
	}
	value, err := r.node.Get(h.Table, h.Key)
	switch {
	case errors.Is(err, badger.ErrKeyNotFound):
		return respond(stream, &wire.StateResponse{Err: wire.ErrCodeNotFound})
	case err != nil:
		return respond(stream, &wire.StateResponse{Err: err.Error()})
	}
	return respond(stream, &wire.StateResponse{Value: value})
}

func (s *StateService) handlePut(ctx context.Context, h wire.StateRequest, stream transport.Stream) error {
	r, err := s.replica(h.Target)
	if err != nil {
		return respond(stream, &wire.StateResponse{Err: err.Error()})
	}
	// The value is the stream body; the client half-closes after writing.
	value, err := io.ReadAll(stream)
	if err != nil {
		return fmt.Errorf("read put value: %w", err)
	}
	return respond(stream, writeResult(r.node, r.node.Put(h.Table, h.Key, value)))
}

func (s *StateService) handleDelete(ctx context.Context, h wire.StateRequest, stream transport.Stream) error {
	r, err := s.replica(h.Target)
	if err != nil {
		return respond(stream, &wire.StateResponse{Err: err.Error()})
	}
	return respond(stream, writeResult(r.node, r.node.Delete(h.Table, h.Key)))
}

// writeResult maps a consensus write outcome onto the response envelope,
// pointing the client at the leader when this replica cannot commit.
func writeResult(node *RaftNode, err error) *wire.StateResponse {
	switch {
	case errors.Is(err, ErrNotLeader):
		return &wire.StateResponse{Err: wire.ErrCodeNotLeader, Leader: node.LeaderAddr()}
	case err != nil:
		return &wire.StateResponse{Err: err.Error()}
	}
	return &wire.StateResponse{}
}

func (s *StateService) handleScan(ctx context.Context, h wire.StateRequest, stream transport.Stream) error {
	r, err := s.replica(h.Target)
	if err != nil {
		return respond(stream, &wire.StateResponse{Err: err.Error()})
	}

	// errScanLimit stops iteration once the limit is reached without
	// surfacing an error to the client.
	errScanLimit := errors.New("scan limit reached")
	var items []wire.ScanItem
	err = r.node.Scan(h.Table, h.Key, func(key string, value []byte) error {
		if h.Limit > 0 && len(items) >= h.Limit {
			return errScanLimit
		}
		items = append(items, wire.ScanItem{Key: key, Value: value})
		return nil
	})
	if err != nil && !errors.Is(err, errScanLimit) {
		return respond(stream, &wire.StateResponse{Err: err.Error()})
	}
	return respond(stream, &wire.StateResponse{Items: items})
}
