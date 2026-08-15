// Package hostile runs a blob node that misbehaves on purpose.
//
// It exists because the failures that hurt in production are not the ones a
// node reports. A node that answers "no" is easy. A node that accepts a
// stream and never answers, or answers with fewer bytes than it promised, is
// what wedges a caller or corrupts an object, and nothing else here can
// express those: a provider-level injector sits above the wire, and an
// iptables partition kills the connection outright, which is a failure the
// transport can already see.
//
// Faults are fixed per server rather than sampled, so a test names the
// condition it is asserting against and a failure reproduces exactly.
package hostile

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
)

// Fault is one way a blob node can misbehave.
type Fault string

const (
	// FaultNone serves the value honestly.
	FaultNone Fault = "none"

	// FaultStall accepts the stream and never writes anything. This is the
	// production failure: the peer is alive, the connection is alive, and no
	// response ever comes. A caller without a bound waits forever.
	FaultStall Fault = "stall"

	// FaultStallAfterEnvelope answers, then stops partway through the body.
	// A cap on the envelope exchange alone does not catch this.
	FaultStallAfterEnvelope Fault = "stall_after_envelope"

	// FaultSlowDrip writes one byte per interval, forever. It defeats an idle
	// timeout used on its own, which is why the envelope keeps a total cap.
	FaultSlowDrip Fault = "slow_drip"

	// FaultTruncate promises BodyLen bytes and sends fewer, then closes
	// cleanly. Silently accepting it feeds a short shard into Reed-Solomon,
	// which reconstructs a plausible wrong object — worse than a failed read.
	FaultTruncate Fault = "truncate"

	// FaultEnvelopeGarbage writes an envelope that never terminates, probing
	// whether the reader caps what it will buffer.
	FaultEnvelopeGarbage Fault = "envelope_garbage"

	// FaultAbort resets the stream partway through the body.
	FaultAbort Fault = "abort"
)

// Config configures a hostile blob node.
type Config struct {
	// Fault is applied to every operation the node serves.
	Fault Fault
	// Values are served by key index; a get for a missing index answers
	// not-found. The honest path returns these bytes verbatim.
	Values map[uint32][]byte
	// DripInterval paces FaultSlowDrip. Default 1ms.
	DripInterval time.Duration
}

// Server answers blob opcodes with a configured fault.
type Server struct {
	cfg Config

	// stop releases handlers parked in a fault. Without it a stalled handler
	// only unwinds when the rpc server's drain deadline expires, which makes
	// tearing one of these down slower than the test that used it.
	stop     chan struct{}
	stopOnce sync.Once

	mu    sync.Mutex
	calls int
}

// New builds a hostile node. Values may be nil for faults that never reach
// the body.
func New(cfg Config) *Server {
	if cfg.DripInterval <= 0 {
		cfg.DripInterval = time.Millisecond
	}
	if cfg.Values == nil {
		cfg.Values = map[uint32][]byte{}
	}
	return &Server{cfg: cfg, stop: make(chan struct{})}
}

// Close releases every handler parked in a fault. It is idempotent.
func (s *Server) Close() {
	s.stopOnce.Do(func() { close(s.stop) })
}

// Calls reports how many operations the node was asked to serve, so a test
// can assert a caller gave up rather than never having arrived.
func (s *Server) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// Mux builds the rpc mux serving this node's opcodes.
func (s *Server) Mux() *rpc.Mux {
	mux := rpc.NewMux()
	rpc.RegisterHandler(mux, blob.OpGet, s.handleGet)
	rpc.RegisterHandler(mux, blob.OpPut, s.handlePut)
	rpc.RegisterHandler(mux, blob.OpDelete, s.handleDelete)
	return mux
}

func (s *Server) record() {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
}

// stall blocks until the caller gives up and the stream is torn down, the
// context ends, or the node is closed. It never returns of its own accord.
func (s *Server) stall(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.stop:
		return nil
	}
}

func writeEnvelope(stream transport.Stream, resp blob.Response) error {
	line, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	if _, err := stream.Write(append(line, '\n')); err != nil {
		return err
	}
	return nil
}

func (s *Server) handleGet(ctx context.Context, header blob.Request, stream transport.Stream) error {
	s.record()

	switch s.cfg.Fault {
	case FaultStall:
		return s.stall(ctx)

	case FaultEnvelopeGarbage:
		// A well-formed prefix that never terminates: the peer keeps buffering
		// for as long as we keep writing.
		for {
			if _, err := stream.Write([]byte(`{"body_len":1,"pad":"`)); err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-s.stop:
				return nil
			default:
			}
		}
	}

	value, ok := s.cfg.Values[header.Index]
	if !ok {
		return writeEnvelope(stream, blob.Response{Err: blob.ErrCodeNotFound})
	}

	switch s.cfg.Fault {
	case FaultTruncate:
		// Promise the whole value, send half, then close cleanly.
		if err := writeEnvelope(stream, blob.Response{BodyLen: int64(len(value))}); err != nil {
			return err
		}
		if _, err := stream.Write(value[:len(value)/2]); err != nil {
			return err
		}
		return stream.Close()

	case FaultStallAfterEnvelope:
		if err := writeEnvelope(stream, blob.Response{BodyLen: int64(len(value))}); err != nil {
			return err
		}
		if _, err := stream.Write(value[:1]); err != nil {
			return err
		}
		return s.stall(ctx)

	case FaultSlowDrip:
		if err := writeEnvelope(stream, blob.Response{BodyLen: int64(len(value))}); err != nil {
			return err
		}
		ticker := time.NewTicker(s.cfg.DripInterval)
		defer ticker.Stop()
		for i := range value {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-s.stop:
				return nil
			case <-ticker.C:
			}
			if _, err := stream.Write(value[i : i+1]); err != nil {
				return err
			}
		}
		return stream.Close()

	case FaultAbort:
		if err := writeEnvelope(stream, blob.Response{BodyLen: int64(len(value))}); err != nil {
			return err
		}
		stream.CancelWrite(1)
		return nil
	}

	if err := writeEnvelope(stream, blob.Response{BodyLen: int64(len(value))}); err != nil {
		return err
	}
	if _, err := stream.Write(value); err != nil {
		return err
	}
	return stream.Close()
}

func (s *Server) handlePut(ctx context.Context, header blob.Request, stream transport.Stream) error {
	s.record()

	if s.cfg.Fault == FaultStall {
		// Never drain the body, so the caller's writes block once the flow
		// control window fills, then never answer.
		return s.stall(ctx)
	}

	n, err := io.Copy(io.Discard, io.LimitReader(stream, header.Size))
	if err != nil {
		return fmt.Errorf("drain put body: %w", err)
	}
	return writeEnvelope(stream, blob.Response{Size: n})
}

func (s *Server) handleDelete(ctx context.Context, _ blob.Request, stream transport.Stream) error {
	s.record()

	if s.cfg.Fault == FaultStall {
		return s.stall(ctx)
	}
	return writeEnvelope(stream, blob.Response{Deleted: true})
}
