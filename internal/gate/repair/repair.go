// Package repair restores shards a blob node owns but does not hold at the
// generation its object's placement record names.
//
// It is a sweep, not a subscription. Every pass pages through the authoritative
// placement records, keeps the ring positions belonging to the nodes it repairs
// for, asks each holder which generation it has, and rebuilds the ones that
// disagree. Nothing is derived from a log window: the meta FSM applies commands
// into Badger without retaining the index that carried them, so there is no
// index-to-command history to walk and no cheaper question to ask than the
// records themselves.
//
// The sweep carries no correctness weight. A read compares each shard's epoch
// against the record it already loaded and discards a stale one whether or not
// repair has ever run, so what repair restores is redundancy — the number of
// further losses an object survives — and never the object.
package repair

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/gate/placement"
	"github.com/mulgadc/predastore/internal/meta"
)

// MetaClient is the slice of the meta client a sweep reads. ScanFrom is the
// one that matters: the object table does not fit in a single response on any
// cluster worth repairing.
type MetaClient interface {
	Get(ctx context.Context, key string) ([]byte, error)
	ScanFrom(ctx context.Context, prefix, after string, limit int) ([]meta.Item, error)
}

var _ MetaClient = (*meta.Client)(nil)

// BlobClient is what repair needs of a blob node: what it holds, the bytes to
// rebuild from, and the two halves of a write.
type BlobClient interface {
	Stat(ctx context.Context, node config.NodeID, req blob.StatRequest) (*blob.StatResponse, error)
	Get(ctx context.Context, node config.NodeID, req blob.GetRequest) (io.ReadCloser, error)
	Put(ctx context.Context, node config.NodeID, req blob.PutRequest, body io.Reader) (*blob.PutResponse, error)
	Commit(ctx context.Context, node config.NodeID, req blob.CommitRequest) error
	Abort(ctx context.Context, node config.NodeID, req blob.CommitRequest) error
}

// Config is everything a repair service runs on.
type Config struct {
	// Nodes are the blob nodes this service repairs for. In this deployment
	// that is the ones colocated with the gate running it, which makes the
	// choice of coordinator deterministic without an election: every node is
	// repaired by exactly one process, the one that shares its disk.
	Nodes []config.NodeID

	Ring *placement.Ring
	Meta MetaClient
	Blob BlobClient

	// DataShards and ParityShards fix the erasure code, and must match what the
	// objects were written under.
	DataShards   int
	ParityShards int

	// Workers bounds concurrent shard rebuilds. Zero takes the default below.
	Workers int

	// PageSize is how many records a scan asks for at a time. Zero defaults.
	PageSize int

	// Interval is the gap between the end of one pass and the start of the
	// next. Zero defaults.
	Interval time.Duration
}

// DefaultWorkers is the rebuild concurrency. A node holding stale shards is a
// durability liability until it is repaired, so repair is allowed to contend
// with serving for the duration; integer division floors, so a single-CPU box
// gets 1 rather than 0.
func DefaultWorkers() int { return runtime.NumCPU()/2 + 1 }

const (
	defaultPageSize = 512
	defaultInterval = 5 * time.Minute
)

// Stats is what a pass did. Scanned counts placement records read, owned the
// positions belonging to this service's nodes, and repaired those actually
// rebuilt. Pending is what the last completed pass left owing.
type Stats struct {
	Passes   int64
	Scanned  int64
	Owned    int64
	Repaired int64
	Failed   int64
	Pending  int64
}

// Service sweeps for shards its nodes owe and rebuilds them.
type Service struct {
	cfg      Config
	nodes    []config.NodeID
	workers  int
	pageSize int
	interval time.Duration

	passes, scanned, owned, repaired, failed, pending atomic.Int64
}

// New validates cfg and applies its defaults. It starts nothing.
func New(cfg Config) (*Service, error) {
	if len(cfg.Nodes) == 0 {
		return nil, errors.New("repair has no nodes to repair for")
	}
	if cfg.Ring == nil || cfg.Meta == nil || cfg.Blob == nil {
		return nil, errors.New("repair needs a ring, a meta client and a blob client")
	}
	if cfg.DataShards <= 0 || cfg.ParityShards < 0 {
		return nil, fmt.Errorf("repair needs a valid erasure code, got RS(%d,%d)",
			cfg.DataShards, cfg.ParityShards)
	}

	s := &Service{
		cfg:      cfg,
		nodes:    slices.Clone(cfg.Nodes),
		workers:  cmpOr(cfg.Workers, DefaultWorkers()),
		pageSize: cmpOr(cfg.PageSize, defaultPageSize),
		interval: cfg.Interval,
	}
	if s.interval <= 0 {
		s.interval = defaultInterval
	}

	return s, nil
}

func cmpOr(v, fallback int) int {
	if v > 0 {
		return v
	}

	return fallback
}

// Stats reports the running counters.
func (s *Service) Stats() Stats {
	return Stats{
		Passes:   s.passes.Load(),
		Scanned:  s.scanned.Load(),
		Owned:    s.owned.Load(),
		Repaired: s.repaired.Load(),
		Failed:   s.failed.Load(),
		Pending:  s.pending.Load(),
	}
}

// Run sweeps until ctx is cancelled. A pass that fails is logged and retried on
// the next tick rather than stopping the service: the condition it is trying to
// fix is usually the same one that made it fail.
func (s *Service) Run(ctx context.Context) error {
	slog.InfoContext(ctx, "Repair sweep started",
		"nodes", s.nodes, "workers", s.workers, "interval_ms", s.interval.Milliseconds())

	for {
		if err := s.Pass(ctx); err != nil && !errors.Is(err, context.Canceled) {
			slog.ErrorContext(ctx, "Repair pass failed", "err", err)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(s.interval):
		}
	}
}

// Pass runs one sweep to completion.
//
// It is not resumable: a restart part-way through starts the enumeration again
// from the beginning. Only the position is lost, never work — every rebuild is
// idempotent against the record's epoch, so repeating a page costs a Stat per
// position and nothing else. A durable cursor would need its own consistency
// rule against a table being written underneath it, which is a second
// correctness argument in a component that does not need one to be correct.
func (s *Service) Pass(ctx context.Context) error {
	work := make(chan task)
	var wg sync.WaitGroup
	var owed atomic.Int64

	for range s.workers {
		wg.Go(func() {
			for t := range work {
				if err := s.repairShard(ctx, t); err != nil {
					s.failed.Add(1)
					slog.WarnContext(ctx, "Shard repair failed",
						"node", t.node, "index", t.index,
						"epoch", fmt.Sprintf("%016x", t.place.WriteEpoch), "err", err)

					continue
				}
				owed.Add(-1)
				s.repaired.Add(1)
			}
		})
	}

	err := s.scan(ctx, func(t task) error {
		owed.Add(1)
		s.pending.Add(1)
		select {
		case work <- t:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	close(work)
	wg.Wait()

	// Whatever the pass could not rebuild is still owed, and the next pass will
	// find it again. Settling the gauge here rather than decrementing per item
	// keeps it from drifting when a pass is cut short.
	s.pending.Store(max(owed.Load(), 0))
	s.passes.Add(1)
	if err != nil {
		return err
	}

	return nil
}

// task is one shard one of this service's nodes owes.
type task struct {
	hash  [32]byte
	index int
	node  config.NodeID
	place handlers.ObjectToShardNodes
}

// scan pages through the placement records and reports every owned position
// whose holder does not have the generation the record names.
func (s *Service) scan(ctx context.Context, emit func(task) error) error {
	prefix := handlers.TableKey(model.TableObjects, "")
	cursor := ""
	for {
		items, err := s.cfg.Meta.ScanFrom(ctx, prefix, cursor, s.pageSize)
		if err != nil {
			return fmt.Errorf("scan placement records: %w", err)
		}
		if len(items) == 0 {
			return nil
		}

		for _, item := range items {
			cursor = item.Key
			hash, ok := handlers.ObjectHashOfKey(item.Key)
			if !ok {
				// The objects table also holds listings, tombstones and the
				// parts of uploads still in flight. A part belongs to a client
				// still writing, which will complete or abort it.
				continue
			}
			place, err := handlers.DecodePlacement(item.Value)
			if err != nil {
				slog.WarnContext(ctx, "Undecodable placement record skipped",
					"hash", hex.EncodeToString(hash[:8]), "err", err)

				continue
			}
			s.scanned.Add(1)
			if err := s.inspect(ctx, hash, place, emit); err != nil {
				return err
			}
		}

		if len(items) < s.pageSize {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// inspect asks each of this service's nodes what it holds at the positions it
// owns for one object, and emits the ones that disagree with the record.
//
// An empty object owns no shards at all: its record exists so the GET can be
// served, and there is nothing on any node to compare.
func (s *Service) inspect(
	ctx context.Context, hash [32]byte, place handlers.ObjectToShardNodes, emit func(task) error,
) error {
	if place.Size == 0 {
		return nil
	}

	for index, node := range place.AllNodes() {
		if !slices.Contains(s.nodes, node) {
			continue
		}
		s.owned.Add(1)

		held, err := s.cfg.Blob.Stat(ctx, node, blob.StatRequest{
			Key: hash, Index: uint32(index),
		})
		switch {
		case err == nil && held.Epoch == place.WriteEpoch:
			continue // the node has the generation the record names
		case err != nil && !errors.Is(err, blob.ErrNotFound):
			// A node that cannot be reached is not a node that owes a shard,
			// and rebuilding into it would fail anyway. The next pass asks
			// again.
			slog.DebugContext(ctx, "Could not stat a shard", "node", node, "index", index, "err", err)

			continue
		}

		if err := emit(task{hash: hash, index: index, node: node, place: place}); err != nil {
			return err
		}
	}

	return nil
}
