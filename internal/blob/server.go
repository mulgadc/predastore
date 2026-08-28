// Package blob serves and consumes opaque value operations over rpc streams.
// The service side fronts the stores hosted in a process; the client side is
// used by the distributed backend to reach blob nodes wherever they run.
package blob

import (
	"context"
	"crypto/cipher"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/mulgadc/predastore/internal/blob/engine"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/telemetry"
	"github.com/mulgadc/predastore/internal/transport"
)

// Store is the value store a node serves. The engine is the production
// implementation; Config.Store is the seam a test substitutes through.
type Store interface {
	Append(key [32]byte, index uint32, size int64) (engine.Writer, error)
	Lookup(key [32]byte, index uint32) (engine.Reader, error)
	Delete(key [32]byte, index uint32) (bool, error)
	NearFull() bool
	Close() error
}

// SnapshotStore is a Store that can report its capacity and compaction state.
// Kept separate from Store so a test substitute stays a handful of methods:
// a store that cannot report simply goes unreported.
type SnapshotStore interface {
	Store
	Snapshot() engine.Snapshot
}

var _ SnapshotStore = (*engine.Store)(nil)

var _ Store = (*engine.Store)(nil)

// Config is everything one blob node runs on. The listeners arrive already
// bound because the process that owns the transports registers them before
// any colocated node dials; everything else the node acquires itself.
type Config struct {
	NodeID    config.NodeID        // The node this server serves. Required.
	DataDir   string               // Where the store lives; Run creates it. Required.
	AEAD      cipher.AEAD          // Seals values at rest. Required unless Store is set.
	Listeners []transport.Listener // The bound listeners the node answers rpc on.

	// Compaction is how often dead space is reclaimed. It is always enabled:
	// without it, overwrite and delete churn frees nothing and the store
	// fills. Zero uses the engine's default interval.
	Compaction time.Duration

	// Store stands in for the engine that would be opened at DataDir. Test
	// seam: production leaves it nil.
	Store Store
}

// Server serves rpc requests for one blob node. A process running several
// nodes builds one Server per node, each with its own rpc server, so the
// storage service never learns that it has siblings.
type Server struct {
	cfg   Config
	store Store
}

// New validates cfg and applies its defaults. It creates no directory, opens
// no store, binds nothing and starts no goroutine: everything the node holds
// is acquired by Run and released by it.
func New(cfg Config) (*Server, error) {
	if cfg.NodeID == 0 {
		return nil, errors.New("node id is required")
	}
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("node %d has no data directory", cfg.NodeID)
	}
	if cfg.Store == nil && cfg.AEAD == nil {
		return nil, fmt.Errorf("node %d has no aead: values are never stored in plaintext", cfg.NodeID)
	}
	// A node nothing can reach stores nothing. Failing here says so, rather
	// than leaving a process holding an open store no peer can dial.
	if len(cfg.Listeners) == 0 {
		return nil, fmt.Errorf("node %d has no listeners", cfg.NodeID)
	}
	return &Server{cfg: cfg}, nil
}

// Run opens the store, answers rpc on the configured listeners until ctx is
// cancelled, then closes the store. A blob node never dials, so its rpc
// server donates to no pool and owns every connection it accepts.
func (s *Server) Run(ctx context.Context) error {
	s.store = s.cfg.Store
	if s.store == nil {
		if err := os.MkdirAll(s.cfg.DataDir, 0750); err != nil {
			return fmt.Errorf("create store directory %s: %w", s.cfg.DataDir, err)
		}
		st, err := engine.Open(s.cfg.DataDir,
			engine.WithAEAD(s.cfg.AEAD), engine.WithCompaction(s.cfg.Compaction))
		if err != nil {
			return fmt.Errorf("open store %s: %w", s.cfg.DataDir, err)
		}
		s.store = st
	}

	// Reporting is unregistered before the store closes, so a collection can
	// never land on a store that has already been torn down.
	if unregister := s.registerStoreGauges(); unregister != nil {
		defer func() {
			if err := unregister(); err != nil {
				slog.Warn("failed to unregister store gauges", "node", s.cfg.NodeID, "error", err)
			}
		}()
	}

	mux := rpc.NewMux()
	rpc.RegisterHandler(mux, OpGet, s.handleGet)
	rpc.RegisterHandler(mux, OpPut, s.handlePut)
	rpc.RegisterHandler(mux, OpDelete, s.handleDelete)
	srv, err := rpc.NewServer(mux, s.cfg.Listeners, nil)
	if err != nil {
		return errors.Join(err, s.store.Close())
	}

	return errors.Join(srv.Run(ctx), s.store.Close())
}

// registerStoreGauges publishes this node's store state on every metrics
// collection, returning the unregister function or nil when there is nothing
// to publish. The node id is attached here because the engine has no idea
// which node it backs, and the metrics are useless without it.
func (s *Server) registerStoreGauges() func() error {
	store, ok := s.store.(SnapshotStore)
	if !ok {
		return nil
	}

	node := strconv.FormatUint(uint64(s.cfg.NodeID), 10)
	unregister, err := telemetry.RegisterStoreGauges(func() telemetry.StoreSnapshot {
		return storeSnapshot(node, store.Snapshot())
	})
	if err != nil {
		// Reporting is not worth failing a node over: it serves data either way.
		slog.Warn("failed to register store gauges", "node", s.cfg.NodeID, "error", err)
		return nil
	}
	return unregister
}

// storeSnapshot restates an engine snapshot in the telemetry package's terms,
// which is what keeps the engine free of any dependency on instrumentation.
func storeSnapshot(node string, s engine.Snapshot) telemetry.StoreSnapshot {
	return telemetry.StoreSnapshot{
		NodeID:                 node,
		Measured:               s.Measured,
		FreeFrac:               s.FreeFrac,
		FreeBytes:              s.FreeBytes,
		TotalBytes:             s.TotalBytes,
		Pressure:               s.Pressure,
		SegNum:                 s.SegNum,
		ValueNum:               s.ValueNum,
		FragNum:                s.FragNum,
		LiveSegments:           s.LiveSegments,
		Scanned:                s.Scanned,
		LiveBytes:              s.LiveBytes,
		DeadBytes:              s.DeadBytes,
		CompactionCycles:       s.CompactionCycles,
		CompactionCyclesFailed: s.CompactionCyclesFailed,
		SegmentsScanned:        s.SegmentsScanned,
		SegmentsDropped:        s.SegmentsDropped,
		BytesRelocated:         s.BytesRelocated,
		BytesReclaimed:         s.BytesReclaimed,
		LastCycleSeconds:       s.LastCycleSeconds,
		IntegrityFailures:      s.IntegrityFailures,
	}
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
