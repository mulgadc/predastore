// Package meta serves and consumes global-state operations over rpc streams.
// The server side is one raft replica; the client side reaches replicas
// wherever they run, hiding the wire protocol from callers.
package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/dgraph-io/badger/v4"
	"github.com/mulgadc/predastore/internal/transport"
)

// handleRaftDial hands the stream to the target replica's raft transport and
// holds it for the connection's lifetime.
func (s *Server) handleRaftDial(ctx context.Context, h RaftDial, stream transport.Stream) error {
	return s.layer.Deliver(ctx, stream)
}

// respond writes the closing JSON envelope; the rpc server closes the stream
// once the handler returns.
func respond(stream transport.Stream, resp *MetaResponse) error {
	return json.NewEncoder(stream).Encode(resp)
}

// refuseLeaderRead answers a leader read this replica cannot serve, and reports
// whether it did. A plain read is served from the local FSM as before.
func (s *Server) refuseLeaderRead(h MetaRequest, stream transport.Stream) (bool, error) {
	if !h.Leader {
		return false, nil
	}
	code := s.leaderRead()
	if code == "" {
		return false, nil
	}

	return true, respond(stream, &MetaResponse{Err: code, Leader: s.leaderAddr()})
}

func (s *Server) handleGet(ctx context.Context, h MetaRequest, stream transport.Stream) error {
	if refused, err := s.refuseLeaderRead(h, stream); refused {
		return err
	}
	// The raft node keys by Go string; the bytes survive the conversion, so
	// the key badger stores is the one the client sent.
	value, err := s.get(string(h.Key))
	switch {
	case errors.Is(err, badger.ErrKeyNotFound):
		return respond(stream, &MetaResponse{Err: ErrCodeNotFound})
	case err != nil:
		return respond(stream, &MetaResponse{Err: err.Error()})
	}
	return respond(stream, &MetaResponse{Value: value})
}

func (s *Server) handlePut(ctx context.Context, h MetaRequest, stream transport.Stream) error {
	// The value is the stream body; the client half-closes after writing.
	value, err := io.ReadAll(stream)
	if err != nil {
		return fmt.Errorf("read put value: %w", err)
	}
	return respond(stream, s.writeResult(s.put(string(h.Key), value)))
}

func (s *Server) handlePutMax(ctx context.Context, h MetaRequest, stream transport.Stream) error {
	value, err := io.ReadAll(stream)
	if err != nil {
		return fmt.Errorf("read put-max value: %w", err)
	}
	return respond(stream, s.writeResult(s.putMax(string(h.Key), value, h.Epoch)))
}

func (s *Server) handleSwap(ctx context.Context, h MetaRequest, stream transport.Stream) error {
	value, err := io.ReadAll(stream)
	if err != nil {
		return fmt.Errorf("read swap value: %w", err)
	}
	previous, err := s.swap(string(h.Key), value)
	resp := s.writeResult(err)
	if err == nil {
		resp.Value = previous
	}

	return respond(stream, resp)
}

// handleTxn reads the whole transaction before it proposes anything, so a
// request that did not arrive complete is refused rather than applied. It
// reads at most one byte past the limit, so an oversized body costs no more.
func (s *Server) handleTxn(ctx context.Context, h MetaRequest, stream transport.Stream) error {
	body, err := io.ReadAll(io.LimitReader(stream, MaxTxnBodyBytes+1))
	if err != nil {
		return fmt.Errorf("read transaction: %w", err)
	}
	if len(body) > MaxTxnBodyBytes {
		return respond(stream, &MetaResponse{Err: ErrCodeInvalid})
	}
	var req TxnRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return respond(stream, &MetaResponse{Err: ErrCodeInvalid})
	}
	if err := ValidateTxn(req.Checks, req.Writes); err != nil {
		return respond(stream, &MetaResponse{Err: ErrCodeInvalid})
	}
	result, err := s.txn(req.Checks, req.Writes)
	resp := s.writeResult(err)
	if err == nil && !result.Applied {
		resp.Mismatch, resp.Current = true, result.Current
	}

	return respond(stream, resp)
}

func (s *Server) handleDelete(ctx context.Context, h MetaRequest, stream transport.Stream) error {
	return respond(stream, s.writeResult(s.delete(string(h.Key))))
}

// writeResult maps a consensus write outcome onto the response envelope,
// pointing the client at the leader when this replica cannot commit.
func (s *Server) writeResult(err error) *MetaResponse {
	switch {
	case errors.Is(err, ErrNotLeader):
		return &MetaResponse{Err: ErrCodeNotLeader, Leader: s.leaderAddr()}
	case errors.Is(err, ErrReservedKey):
		return &MetaResponse{Err: ErrCodeInvalid}
	case err != nil:
		return &MetaResponse{Err: err.Error()}
	}
	return &MetaResponse{}
}

// handleStatus reports this replica's raft state. It is answered from local
// state, never blocks on consensus, and cannot fail the way a data operation
// can, so there is no error envelope to fill in.
func (s *Server) handleStatus(ctx context.Context, h MetaStatusRequest, stream transport.Stream) error {
	status := s.status()
	return json.NewEncoder(stream).Encode(&status)
}

func (s *Server) handleLastKey(ctx context.Context, h MetaRequest, stream transport.Stream) error {
	if !h.Leader || len(h.Key) == 0 || len(h.After) != 0 || h.Limit != 1 {
		return respond(stream, &MetaResponse{Err: "invalid last-key lookup"})
	}
	if !s.fsm.mapHeadIndexReady() {
		return respond(stream, &MetaResponse{Err: ErrCodeBehind})
	}
	if code := s.authorityRead(ctx); code != "" {
		return respond(stream, &MetaResponse{Err: code, Leader: s.leaderAddr()})
	}
	key, err := s.fsm.LastKey(string(h.Key))
	if errors.Is(err, errMapHeadIndexUnavailable) {
		return respond(stream, &MetaResponse{Err: ErrCodeBehind})
	}
	if err != nil {
		return respond(stream, &MetaResponse{Err: err.Error()})
	}
	response := MetaResponse{Last: true}
	if key != "" {
		response.Items = []ScanItem{{Key: []byte(key)}}
	}
	return respond(stream, &response)
}

func (s *Server) handleScan(ctx context.Context, h MetaRequest, stream transport.Stream) error {
	if h.Last {
		return respond(stream, &MetaResponse{Err: "last-key lookup requires a dedicated operation"})
	}
	if refused, err := s.refuseLeaderRead(h, stream); refused {
		return err
	}
	// The scan stops on the row that reaches the limit, so it never reads the
	// row after it.
	var items []ScanItem
	err := s.scan(string(h.Key), string(h.After), h.Limit, func(key string, value []byte) error {
		items = append(items, ScanItem{Key: []byte(key), Value: value})
		return nil
	})
	if err != nil {
		return respond(stream, &MetaResponse{Err: err.Error()})
	}
	return respond(stream, &MetaResponse{Items: items})
}
