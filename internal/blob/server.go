// Package blob serves and consumes opaque value operations over rpc streams.
// The server side answers for one node's store; the client side is used by a
// gate to reach blob nodes wherever they run.
//
// Every operation names its value by an opaque key. Whatever structure the
// caller encodes there — an object hash, a shard or stripe index, a version —
// is the caller's alone: a node compares keys and never reads into them.
package blob

import (
	"context"
	"crypto/cipher"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/mulgadc/predastore/internal/blob/engine"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
)

// Store is the value store a node serves. The engine is the production
// implementation; Config.Store is the seam a test substitutes through.
type Store interface {
	// Put commits size bytes read from r, consuming at most size so the caller
	// keeps whatever follows. Nothing it reserves outlives the call, and a
	// short r commits nothing: the key names whatever it named before.
	Put(key []byte, size uint64, r io.Reader) error
	Lookup(key []byte) (engine.Reader, error)
	Delete(key []byte) (bool, error)
	NearFull() bool
	Close() error
}

var _ Store = (*engine.Store)(nil)

// Config is everything one blob node runs on. Everything but the listeners,
// which arrive already bound, the node acquires itself.
type Config struct {
	NodeID    config.NodeID        // The node this server serves. Required.
	DataDir   string               // Where the store lives; Run creates it. Required.
	AEAD      cipher.AEAD          // Seals values at rest. Required unless Store is set.
	Listeners []transport.Listener // The bound listeners the node answers rpc on.

	// Compaction is how often dead space is reclaimed, and is always enabled.
	// Zero uses the engine's default interval.
	Compaction time.Duration

	// Store stands in for the engine that would be opened at DataDir. Test
	// seam: production leaves it nil.
	Store Store
}

// Server serves rpc requests for one blob node. A process running several
// nodes builds one Server per node, each with its own rpc server.
//
// Its handlers answer on their own half of the stream and return nil, whether
// they succeeded or reported a failure status; a returned error resets the
// stream instead, and so is reserved for one that broke.
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
	if len(cfg.Listeners) == 0 {
		return nil, fmt.Errorf("node %d has no listeners", cfg.NodeID)
	}
	return &Server{cfg: cfg}, nil
}

// Run opens the store, answers rpc on the configured listeners until ctx is
// cancelled, then closes the store.
func (s *Server) Run(ctx context.Context) error {
	// A configured store is a test standing in for the engine; production
	// leaves it nil and opens the real one here.
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

	mux := rpc.NewMux()
	rpc.RegisterHandler(mux, OpGet, s.handleGet)
	rpc.RegisterHandler(mux, OpPut, s.handlePut)
	rpc.RegisterHandler(mux, OpDelete, s.handleDelete)

	// A blob node never dials, so it donates to no pool and owns every
	// connection it accepts.
	srv, err := rpc.NewServer(mux, s.cfg.Listeners, nil)
	if err != nil {
		return errors.Join(err, s.store.Close())
	}

	return errors.Join(srv.Run(ctx), s.store.Close())
}

func (s *Server) handlePut(ctx context.Context, h PutRequest, stream transport.Stream) error {
	st := s.store
	if h.Size == 0 {
		return rpc.WriteError(stream, rpc.StatusInternal, "no size specified")
	}

	// The store reads the body straight off the stream, stopping at the
	// declared size.
	if err := st.Put(h.Key, h.Size, stream); err != nil {
		// Drain what is still in flight, bounded by that size, so the reply is
		// carried instead of racing a stream reset.
		if _, derr := io.Copy(io.Discard, io.LimitReader(stream, int64(h.Size))); derr != nil { //nolint:gosec // G115: a declared body length never exceeds int64.
			return fmt.Errorf("drain body after put error: %w", derr)
		}
		if errors.Is(err, engine.ErrStoreFull) {
			return rpc.WriteError(stream, StatusStoreFull, err.Error())
		}
		return rpc.WriteError(stream, rpc.StatusInternal, fmt.Sprintf("put: %v", err))
	}
	return rpc.WriteResponse(stream, &PutResponse{PoolNearFull: st.NearFull()})
}

func (s *Server) handleGet(ctx context.Context, h GetRequest, stream transport.Stream) error {
	st := s.store

	reader, err := st.Lookup(h.Key)
	if err != nil {
		return rpc.WriteError(stream, StatusNotFound, "")
	}
	defer reader.Close()

	// Bounding the offset first is what keeps the subtraction below from
	// wrapping.
	totalSize := uint64(reader.Size()) //nolint:gosec // G115: a stored value's size is never negative.
	if h.Off >= totalSize {
		return rpc.WriteError(stream, StatusBadRange,
			fmt.Sprintf("offset %d is past the value's %d bytes", h.Off, totalSize))
	}

	// An unset or over-long length reads to the end of the value.
	length := h.Len
	if length == 0 || length > totalSize-h.Off {
		length = totalSize - h.Off
	}

	if err := rpc.WriteResponse(stream, &GetResponse{}); err != nil {
		return err
	}
	// Returning closes this half of the stream, which is what ends the body.
	//nolint:gosec // G115: both are bounded by the value's own size.
	if _, err := stream.ReadFrom(io.NewSectionReader(reader, int64(h.Off), int64(length))); err != nil {
		return fmt.Errorf("stream body: %w", err)
	}
	return nil
}

func (s *Server) handleDelete(ctx context.Context, h DeleteRequest, stream transport.Stream) error {
	st := s.store
	deleted, err := st.Delete(h.Key)
	if err != nil {
		return rpc.WriteError(stream, rpc.StatusInternal, err.Error())
	}
	return rpc.WriteResponse(stream, &DeleteResponse{Deleted: deleted})
}
