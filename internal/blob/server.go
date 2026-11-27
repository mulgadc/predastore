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
	Append(key [32]byte, index uint32, size int64, epoch uint64) (engine.Writer, error)
	AppendHeld(key [32]byte, index uint32, size int64, epoch uint64) (engine.Writer, error)
	Lookup(key [32]byte, index uint32) (engine.Reader, error)
	LookupAt(key [32]byte, index uint32, epoch uint64) (engine.Reader, error)
	Commit(key [32]byte, index uint32, epoch uint64) (published bool, err error)
	Abort(key [32]byte, index uint32, epoch uint64) error
	Delete(key [32]byte, index uint32) (bool, error)
	DeleteEpoch(key [32]byte, index uint32, epoch uint64) (bool, error)
	ReleaseGeneration(key [32]byte, index uint32, epoch uint64) error
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

	// RetainedMaxAge is how long a superseded generation stays answerable.
	// Zero takes the store's default.
	RetainedMaxAge time.Duration

	// CompactionSlot of CompactionSlots offsets this node's routine passes
	// from its peers'. Zero slots means slot 0 of 1.
	CompactionSlot  int
	CompactionSlots int

	// Pacing for routine passes; zeros take the store's defaults, and a zero
	// YieldLatency leaves the foreground yield off.
	CompactionMaxBytesPerSec int64
	CompactionYieldLatency   time.Duration
	CompactionMaxYield       time.Duration

	// BodyIdleTimeout bounds a get body whose peer has stopped reading, and a
	// held put body that has stopped arriving. Zero uses DefaultBodyIdleTimeout.
	BodyIdleTimeout time.Duration

	// Store stands in for the engine that would be opened at DataDir. Test
	// seam: production leaves it nil.
	Store Store
}

// DefaultBodyIdleTimeout bounds a get body that stops moving. It is deliberately
// longer than the client's own idle timeout so a caller that gives up aborts its
// own stream first; this is the backstop for one that vanishes without aborting.
const DefaultBodyIdleTimeout = 60 * time.Second

// Server serves rpc requests for one blob node. A process running several
// nodes builds one Server per node, each with its own rpc server, so the
// storage service never learns that it has siblings.
type Server struct {
	cfg   Config
	store Store
	held  *heldWriters
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
	if cfg.BodyIdleTimeout <= 0 {
		cfg.BodyIdleTimeout = DefaultBodyIdleTimeout
	}
	return &Server{cfg: cfg, held: newHeldWriters()}, nil
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
		slot, slots := s.cfg.CompactionSlot, s.cfg.CompactionSlots
		if slots < 1 {
			slot, slots = 0, 1
		}
		st, err := engine.Open(s.cfg.DataDir,
			engine.WithAEAD(s.cfg.AEAD), engine.WithCompaction(s.cfg.Compaction),
			engine.WithRetainedMaxAge(s.cfg.RetainedMaxAge),
			engine.WithCompactionSlot(slot, slots),
			engine.WithCompactionPacing(s.cfg.CompactionMaxBytesPerSec, s.cfg.CompactionYieldLatency, s.cfg.CompactionMaxYield))
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
	rpc.RegisterHandler(mux, OpRelease, s.handleRelease)
	rpc.RegisterHandler(mux, OpCommit, s.handleCommit)
	rpc.RegisterHandler(mux, OpAbort, s.handleAbort)
	rpc.RegisterHandler(mux, OpStat, s.handleStat)
	rpc.RegisterHandler(mux, OpDeleteEpoch, s.handleDeleteEpoch)
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
	if h.Hold {
		return s.handlePutHeld(h, stream)
	}
	// Zero is reserved as "invalid", so a caller that forgot the epoch is
	// refused rather than storing a shard nothing can ever match.
	if h.Epoch == 0 {
		if derr := drainBody(stream, h.Size); derr != nil {
			return derr
		}
		return respond(stream, &Response{Err: "no write epoch specified"})
	}

	writer, err := st.Append(h.Key, h.Index, h.Size, h.Epoch)
	if err != nil {
		// Drain the client's in-flight body so the reply is carried
		// instead of racing a stream reset.
		if derr := drainBody(stream, h.Size); derr != nil {
			return derr
		}
		if errors.Is(err, engine.ErrStoreFull) {
			return respond(stream, &Response{Err: ErrCodeStoreFull})
		}
		return respond(stream, &Response{Err: fmt.Sprintf("append: %v", err)})
	}

	if _, err := writer.ReadFrom(io.LimitReader(stream, h.Size)); err != nil {
		// Discard releases the segment reference Append took and prepares
		// nothing, so a get cannot commit the short body before it is undone.
		if discardErr := writer.Discard(); discardErr != nil {
			slog.Warn("discard writer after failed body", "node", s.cfg.NodeID, "error", discardErr)
		}
		return respond(stream, &Response{Err: fmt.Sprintf("write: %v", err)})
	}
	// Closing prepares the extent rather than publishing it: the shard is
	// durable here and invisible until the caller commits, which is what lets
	// an overwrite keep serving its previous generation until the whole stripe
	// is in place.
	if err := writer.Close(); err != nil {
		return respond(stream, &Response{Err: fmt.Sprintf("prepare: %v", err)})
	}

	// Surface nearfull pressure on success too, so callers can back off
	// before a write is ever outright rejected.
	return respond(stream, &Response{Size: h.Size, PoolNearFull: st.NearFull(), Epoch: h.Epoch})
}

// handlePutHeld opens a held put. It sends no body before Registered, so a
// refusal is answered at once, and a second put of an open shard is refused
// before it gets a writer that could prepare over the first.
func (s *Server) handlePutHeld(h Request, stream transport.Stream) error {
	if h.Epoch == 0 {
		return respond(stream, &Response{Err: "no write epoch specified"})
	}
	k := heldKey{key: h.Key, index: h.Index, epoch: h.Epoch}
	if !s.held.register(k, func() { stream.CancelRead(transport.StreamCodeCallerGone) }) {
		return respond(stream, &Response{Err: "a held write of this shard is already open"})
	}
	writer, err := s.store.AppendHeld(h.Key, h.Index, h.Size, h.Epoch)
	if err != nil {
		s.held.finish(k)
		if errors.Is(err, engine.ErrStoreFull) {
			return respond(stream, &Response{Err: ErrCodeStoreFull})
		}
		return respond(stream, &Response{Err: fmt.Sprintf("append: %v", err)})
	}
	return s.putHeld(h, k, writer, stream)
}

// putHeld runs a registered held put once its writer exists. The caller is
// told before any body byte is read, and an unused writer is discarded before
// its registration goes, so an abort confirms only once the segment is free.
func (s *Server) putHeld(h Request, k heldKey, writer engine.Writer, stream transport.Stream) error {
	if err := respond(stream, &Response{Registered: true, Epoch: h.Epoch}); err != nil {
		s.discard(writer)
		s.held.finish(k)
		return fmt.Errorf("write registered envelope: %w", err)
	}

	// A body that stops moving ends the write, so a stalled caller cannot
	// keep the writer open and its segment pinned.
	guard := transport.NewReadGuard(io.LimitReader(stream, h.Size), stream, s.cfg.BodyIdleTimeout)
	_, err := writer.ReadFrom(guard)
	guard.Stop()
	if err != nil {
		s.discard(writer)
		s.held.finish(k)
		return respond(stream, &Response{Err: fmt.Sprintf("write: %v", err)})
	}
	if s.held.beginClose(k) {
		s.discard(writer)
		s.held.finish(k)
		return respond(stream, &Response{Err: "held write aborted"})
	}
	// An abort from here on waits for this close and drops the row itself.
	closeErr := writer.Close()
	s.held.finish(k)
	if closeErr != nil {
		return respond(stream, &Response{Err: fmt.Sprintf("prepare: %v", closeErr)})
	}
	return respond(stream, &Response{Size: h.Size, PoolNearFull: s.store.NearFull(), Epoch: h.Epoch, Holding: true})
}

// discard ends a held writer whose body will not be used. It prepares no
// row, so a crash at any point leaves nothing the sweep would keep.
func (s *Server) discard(writer engine.Writer) {
	if err := writer.Discard(); err != nil {
		slog.Warn("discard held writer", "node", s.cfg.NodeID, "error", err)
	}
}

// drainBody consumes a rejected put's in-flight body so the reply is carried
// instead of racing a stream reset.
func drainBody(stream transport.Stream, size int64) error {
	if size <= 0 {
		return nil
	}
	if _, err := io.Copy(io.Discard, io.LimitReader(stream, size)); err != nil {
		return fmt.Errorf("drain rejected body: %w", err)
	}
	return nil
}

// handleCommit publishes a prepared shard. It is the second half of the write
// and is driven by whoever holds the published placement record, so a retry of
// one already applied has to succeed rather than report a write that did land
// as failed; the engine makes that idempotent against the epoch.
//
// Being overtaken by a newer generation is also success. The caller raced
// another writer of the same shard and lost, which is a normal outcome and not
// something to report as a failed write. Superseded says so, for the log and
// the metric, and the response is still an acknowledgement.
func (s *Server) handleCommit(ctx context.Context, h Request, stream transport.Stream) error {
	if h.Epoch == 0 {
		return respond(stream, &Response{Err: "no write epoch specified"})
	}
	published, err := s.store.Commit(h.Key, h.Index, h.Epoch)
	if err != nil {
		if errors.Is(err, engine.ErrNotPrepared) {
			return respond(stream, &Response{Err: ErrCodeNotPrepared})
		}
		return respond(stream, &Response{Err: fmt.Sprintf("commit: %v", err)})
	}
	// The store answers overtaken only for a generation it holds, which is
	// what Held tells a gate counting this shard toward a write's floor.
	return respond(stream, &Response{Epoch: h.Epoch, Superseded: !published, Held: !published})
}

// handleAbort discards a prepared shard. Aborting something never prepared is
// success: the caller is asking that nothing be left pending, and nothing is.
func (s *Server) handleAbort(ctx context.Context, h Request, stream transport.Stream) error {
	if h.Epoch == 0 {
		return respond(stream, &Response{Err: "no write epoch specified"})
	}
	// A held writer still receiving its body is cut off and prepares nothing,
	// and the caller hears Open. One already closing is waited for.
	open, closing := s.held.markAborted(heldKey{key: h.Key, index: h.Index, epoch: h.Epoch})
	if closing != nil {
		select {
		case <-closing:
		case <-ctx.Done():
			return ctx.Err()
		}
	} else if open {
		return respond(stream, &Response{Epoch: h.Epoch, Open: true})
	}
	if err := s.store.Abort(h.Key, h.Index, h.Epoch); err != nil {
		return respond(stream, &Response{Err: fmt.Sprintf("abort: %v", err)})
	}
	return respond(stream, &Response{Epoch: h.Epoch})
}

// handleStat reports the generation and size of the shard a node holds,
// without its body. It never completes an abandoned commit the way a get does:
// the caller is asking what is there, and publishing something as a side effect
// of being asked would make the answer a thing the question caused.
func (s *Server) handleStat(ctx context.Context, h Request, stream transport.Stream) error {
	reader, err := s.store.Lookup(h.Key, h.Index)
	switch {
	case errors.Is(err, engine.ErrKeyNotFound):
		return respond(stream, &Response{Err: ErrCodeNotFound})
	case err != nil:
		// A store that could not answer has not shown the shard missing, and
		// reporting it as missing would send repair to rebuild over it.
		return respond(stream, &Response{Err: fmt.Sprintf("stat: %v", err)})
	}
	resp := Response{Epoch: reader.Epoch(), Size: reader.Size()}
	if closeErr := reader.Close(); closeErr != nil {
		return respond(stream, &Response{Err: fmt.Sprintf("stat: %v", closeErr)})
	}

	return respond(stream, &resp)
}

func (s *Server) handleGet(ctx context.Context, h Request, stream transport.Stream) error {
	st := s.store

	reader, err := st.Lookup(h.Key, h.Index)
	if errors.Is(err, engine.ErrKeyNotFound) && h.Epoch != 0 {
		reader, err = s.completeFirst(h)
	}
	if err != nil {
		return respond(stream, &Response{Err: ErrCodeNotFound})
	}
	if h.Epoch != 0 && reader.Epoch() != h.Epoch {
		held := reader.Epoch()
		reader, err = s.resolveEpoch(h, reader)
		if err != nil {
			return respond(stream, &Response{Err: ErrCodeEpochMismatch, Epoch: held})
		}
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
	// Guarded because the reader holds a reference to its segment until it is
	// closed. A peer that stops draining without aborting would otherwise pin
	// that reference for as long as its connection survives, and compaction
	// blocks on exactly those references.
	guard := transport.NewWriteGuard(
		io.NewSectionReader(reader, rangeStart, responseSize), stream, s.cfg.BodyIdleTimeout)
	defer guard.Stop()

	if _, err := stream.ReadFrom(guard); err != nil {
		if guard.Expired() {
			return fmt.Errorf("stream body: %w", transport.ErrIdleTimeout)
		}
		return fmt.Errorf("stream body: %w", err)
	}
	return nil
}

// resolveEpoch answers a get whose live shard is the wrong generation. Two
// cases reach here and both are ordinary:
//
// The record names a generation newer than the live row, which means a writer
// published the record and died before committing. The prepared extent under
// that epoch is still there — prepared rows are keyed by epoch, so no other
// writer can have taken it — and completing the commit is the only right
// outcome.
//
// Or the record names an older generation, because a newer writer published its
// shards and its own record has not landed yet, or never will. That generation
// was demoted rather than destroyed, so it is served from the retained
// namespace. Refusing it would fail a read of the version the cluster agreed on.
// If its writer has not committed it yet, the get commits it into that namespace.
//
// It takes ownership of the reader it is given: on every path that reader is
// closed and the caller keeps only what comes back.
func (s *Server) resolveEpoch(h Request, stale engine.Reader) (engine.Reader, error) {
	held := stale.Epoch()
	if err := stale.Close(); err != nil {
		return nil, fmt.Errorf("close stale reader: %w", err)
	}

	if h.Epoch < held {
		reader, err := s.store.LookupAt(h.Key, h.Index, h.Epoch)
		if errors.Is(err, engine.ErrKeyNotFound) {
			reader, err = s.commitOvertaken(h, err)
		}
		if err != nil {
			return nil, err
		}
		slog.Debug("served a generation a newer write superseded",
			"node", s.cfg.NodeID, "index", h.Index,
			"epoch", fmt.Sprintf("%016x", h.Epoch), "live", fmt.Sprintf("%016x", held))
		return reader, nil
	}

	if _, err := s.store.Commit(h.Key, h.Index, h.Epoch); err != nil {
		return nil, err
	}
	reader, err := s.store.LookupAt(h.Key, h.Index, h.Epoch)
	if err != nil {
		return nil, err
	}
	slog.Info("completed a shard commit abandoned by its writer",
		"node", s.cfg.NodeID, "index", h.Index, "epoch", fmt.Sprintf("%016x", h.Epoch))
	return reader, nil
}

// commitOvertaken completes a record's generation still prepared under a newer
// live row: the record is authoritative, so the commit retains it. With nothing
// prepared at that epoch, notFound is returned and nothing is committed.
func (s *Server) commitOvertaken(h Request, notFound error) (engine.Reader, error) {
	if _, err := s.store.Commit(h.Key, h.Index, h.Epoch); err != nil {
		if errors.Is(err, engine.ErrNotPrepared) {
			return nil, notFound
		}
		return nil, err
	}
	reader, err := s.store.LookupAt(h.Key, h.Index, h.Epoch)
	if err != nil {
		return nil, err
	}
	slog.Info("completed the commit of a shard a newer write overtook",
		"node", s.cfg.NodeID, "index", h.Index, "epoch", fmt.Sprintf("%016x", h.Epoch))
	return reader, nil
}

// completeFirst answers a get for a named epoch at a position with no live
// row: a record names it, so its writer has not committed yet or died first.
// Committing it is what the writer would do; nothing else is committed.
func (s *Server) completeFirst(h Request) (engine.Reader, error) {
	// A retained generation of that epoch is served as it is.
	if reader, err := s.store.LookupAt(h.Key, h.Index, h.Epoch); !errors.Is(err, engine.ErrKeyNotFound) {
		return reader, err
	}
	if _, err := s.store.Commit(h.Key, h.Index, h.Epoch); err != nil {
		return nil, err
	}
	reader, err := s.store.LookupAt(h.Key, h.Index, h.Epoch)
	if err != nil {
		return nil, err
	}
	slog.Info("completed the first commit of a shard abandoned by its writer",
		"node", s.cfg.NodeID, "index", h.Index, "epoch", fmt.Sprintf("%016x", h.Epoch))
	return reader, nil
}

// handleRelease drops a superseded generation a writer has finished with. It
// is best-effort housekeeping, so a miss is success: the generation being gone
// already is the outcome the caller wanted.
func (s *Server) handleRelease(ctx context.Context, h Request, stream transport.Stream) error {
	if err := s.store.ReleaseGeneration(h.Key, h.Index, h.Epoch); err != nil {
		return respond(stream, &Response{Err: err.Error()})
	}

	return respond(stream, &Response{})
}

// handleDeleteEpoch removes one epoch of a shard. Epoch zero is valid here:
// it names a row written before epochs, and no write path makes another.
func (s *Server) handleDeleteEpoch(ctx context.Context, h Request, stream transport.Stream) error {
	deleted, err := s.store.DeleteEpoch(h.Key, h.Index, h.Epoch)
	if err != nil {
		return respond(stream, &Response{Err: err.Error()})
	}
	return respond(stream, &Response{Deleted: deleted})
}

func (s *Server) handleDelete(ctx context.Context, h Request, stream transport.Stream) error {
	st := s.store
	deleted, err := st.Delete(h.Key, h.Index)
	if err != nil {
		return respond(stream, &Response{Err: err.Error()})
	}
	return respond(stream, &Response{Deleted: deleted})
}
