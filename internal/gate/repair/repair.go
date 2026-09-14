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
	"cmp"
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

// MetaClient is the slice of the meta client a sweep reads. Both reads go to
// the leader: a replica still replaying its log serves records that make shards
// look owed, and a rebuilt shard must not be committed on a stale epoch check.
type MetaClient interface {
	LeaderGet(ctx context.Context, key string) ([]byte, error)
	LeaderScanFrom(ctx context.Context, prefix, after string, limit int) ([]meta.Item, error)
}

var _ MetaClient = (*meta.Client)(nil)

// BlobClient is what repair needs of a blob node: what it holds, the bytes to
// rebuild from, and the two halves of a write.
type BlobClient interface {
	Stat(ctx context.Context, node config.NodeID, req blob.StatRequest) (*blob.StatResponse, error)
	Get(ctx context.Context, node config.NodeID, req blob.GetRequest) (io.ReadCloser, error)
	Put(ctx context.Context, node config.NodeID, req blob.PutRequest, body io.Reader) (*blob.PutResponse, error)
	Commit(ctx context.Context, node config.NodeID, req blob.CommitRequest) (superseded bool, err error)
	Abort(ctx context.Context, node config.NodeID, req blob.CommitRequest) error
	Delete(ctx context.Context, node config.NodeID, req blob.DeleteRequest) (*blob.DeleteResponse, error)
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

	// Ready holds each pass until the cluster is settled. Nil runs every pass
	// immediately. ReadyTimeout bounds one wait, after which the pass is
	// deferred to the next interval; ReadyPoll is the gap between checks.
	Ready        Readiness
	ReadyTimeout time.Duration
	ReadyPoll    time.Duration
}

// DefaultWorkers is the rebuild concurrency. A node holding stale shards is a
// durability liability until it is repaired, so repair is allowed to contend
// with serving for the duration; integer division floors, so a single-CPU box
// gets 1 rather than 0.
//
// The cap is what makes that safe to run unasked. Half of a 256-thread host is
// 129 concurrent rebuilds, which is not a repair sweep contending with serving
// but one displacing it, and rebuild is bounded by disk and peer bandwidth
// well before it is bounded by goroutines.
func DefaultWorkers() int { return min(runtime.NumCPU()/2+1, maxDefaultWorkers) }

const (
	maxDefaultWorkers   = 8
	defaultPageSize     = 512
	defaultInterval     = 5 * time.Minute
	defaultReadyTimeout = 2 * time.Minute
	defaultReadyPoll    = 2 * time.Second

	// readyCheckTimeout bounds one readiness check, so an unanswering replica
	// costs one poll rather than the whole wait.
	readyCheckTimeout = 10 * time.Second
)

// Stats is what a pass did. Scanned counts placement records read, owned the
// positions belonging to this service's nodes, and repaired those actually
// rebuilt. Pending is what the last completed pass left owing.
//
// Superseded counts rebuilds the node refused to publish because it had moved
// past the generation the record names. Those are separated from Repaired
// because they are work that changed nothing: the next pass finds the same
// mismatch and rebuilds it again, so counting them as repairs reports a sweep
// as productive while it loops.
type Stats struct {
	Passes     int64 `json:"passes"`
	Scanned    int64 `json:"scanned"`
	Owned      int64 `json:"owned"`
	Owed       int64 `json:"owed"`
	Repaired   int64 `json:"repaired"`
	Superseded int64 `json:"superseded"`
	Failed     int64 `json:"failed"`
	Pending    int64 `json:"pending"`
	// Deferred counts passes held back: the cluster never became ready, or no
	// peer of an owed shard answered.
	Deferred int64 `json:"deferred"`

	// Repaired split by route: publishing a prepared shard, copying it back
	// from the handoff standby, or rebuilding it from peers.
	RepairedCommit  int64 `json:"repaired_commit"`
	RepairedStandby int64 `json:"repaired_standby"`
	RepairedRebuild int64 `json:"repaired_rebuild"`

	// Failed split by cause. Unreachable is transient; a peer at another epoch
	// or missing the shard means it cannot be rebuilt from what is there.
	FailedPeerUnreachable int64 `json:"failed_peer_unreachable"`
	FailedPeerOtherEpoch  int64 `json:"failed_peer_other_epoch"`
	FailedPeerMissing     int64 `json:"failed_peer_missing"`
	FailedOther           int64 `json:"failed_other"`

	// LastPass is the most recent pass, or the most recent deferral; nil
	// until the first has ended.
	LastPass *PassSummary `json:"last_pass,omitempty"`
}

// Service sweeps for shards its nodes owe and rebuilds them.
type Service struct {
	cfg          Config
	nodes        []config.NodeID
	workers      int
	pageSize     int
	interval     time.Duration
	readyTimeout time.Duration
	readyPoll    time.Duration

	passes, scanned, owned, repaired, superseded, failed, pending atomic.Int64

	deferred, owed atomic.Int64

	repairedCommit, repairedStandby, repairedRebuild atomic.Int64

	failedPeerUnreachable, failedPeerOtherEpoch, failedPeerMissing, failedOther atomic.Int64

	// owedStreak counts consecutive passes that found shards owed, and lastPass
	// holds the most recent summary.
	owedStreak atomic.Int64
	lastPass   atomic.Pointer[PassSummary]
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
	s.readyTimeout = cmp.Or(cfg.ReadyTimeout, defaultReadyTimeout)
	s.readyPoll = cmp.Or(cfg.ReadyPoll, defaultReadyPoll)

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
		Passes:     s.passes.Load(),
		Scanned:    s.scanned.Load(),
		Owned:      s.owned.Load(),
		Owed:       s.owed.Load(),
		Repaired:   s.repaired.Load(),
		Superseded: s.superseded.Load(),
		Failed:     s.failed.Load(),
		Pending:    s.pending.Load(),
		Deferred:   s.deferred.Load(),

		RepairedCommit:  s.repairedCommit.Load(),
		RepairedStandby: s.repairedStandby.Load(),
		RepairedRebuild: s.repairedRebuild.Load(),

		FailedPeerUnreachable: s.failedPeerUnreachable.Load(),
		FailedPeerOtherEpoch:  s.failedPeerOtherEpoch.Load(),
		FailedPeerMissing:     s.failedPeerMissing.Load(),
		FailedOther:           s.failedOther.Load(),

		LastPass: s.lastPass.Load(),
	}
}

// Run sweeps until ctx is cancelled. A pass that fails is logged and retried on
// the next tick rather than stopping the service: the condition it is trying to
// fix is usually the same one that made it fail.
func (s *Service) Run(ctx context.Context) error {
	slog.InfoContext(ctx, "Repair sweep started",
		"nodes", s.nodes, "workers", s.workers, "interval_ms", s.interval.Milliseconds())

	for {
		if s.awaitReady(ctx) {
			err := s.Pass(ctx)
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, ErrPassDeferred) {
				slog.ErrorContext(ctx, "Repair pass failed", "err", err)
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(s.interval):
		}
	}
}

// awaitReady holds a pass until the cluster is settled, and reports whether it
// may run. A pass that is never ready is deferred, not failed: nothing it would
// have read could be trusted, so it has learned nothing about any shard.
func (s *Service) awaitReady(ctx context.Context) bool {
	if s.cfg.Ready == nil {
		return true
	}

	start := time.Now()
	logged := false
	for {
		checkCtx, cancel := context.WithTimeout(ctx, readyCheckTimeout)
		err := s.cfg.Ready.Ready(checkCtx)
		cancel()
		if ctx.Err() != nil {
			return false
		}
		if err == nil {
			if logged {
				slog.InfoContext(ctx, "Repair pass released: cluster ready",
					"waited_ms", time.Since(start).Milliseconds())
			}

			return true
		}
		if !logged {
			slog.InfoContext(ctx, "Repair pass waiting for cluster readiness",
				"reason", err.Error(), "timeout_ms", s.readyTimeout.Milliseconds())
			logged = true
		}
		if time.Since(start) >= s.readyTimeout {
			s.deferred.Add(1)
			s.lastPass.Store(&PassSummary{
				Outcome: OutcomeNotReady, Reason: err.Error(),
				Finished: time.Now(), DurationMs: time.Since(start).Milliseconds(),
			})
			slog.InfoContext(ctx, "Repair pass deferred: cluster not ready",
				"reason", err.Error(), "waited_ms", time.Since(start).Milliseconds(),
				"next_in_ms", s.interval.Milliseconds())

			return false
		}

		select {
		case <-ctx.Done():
			return false
		case <-time.After(s.readyPoll):
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
	// A pass that finds no peer reachable stops early, and the work queued
	// behind it is abandoned rather than failed.
	passCtx, stopPass := context.WithCancel(ctx)
	defer stopPass()
	start, before := time.Now(), s.Stats()

	work := make(chan task)
	var wg sync.WaitGroup
	var owed atomic.Int64
	var deferred atomic.Bool

	for range s.workers {
		wg.Go(func() {
			for t := range work {
				route, err := s.repairShard(passCtx, t)
				switch {
				case err == nil:
					owed.Add(-1)
					s.repaired.Add(1)
					s.countRoute(route)
				case deferred.Load():
					// Cut short by the deferral, which says nothing about this shard.
				case errors.Is(err, errSuperseded):
					// Not a failure and not a repair. The record and the node
					// disagree about the current write, which rebuilding cannot
					// settle, so this stays owed rather than reading as done.
					s.superseded.Add(1)
					slog.InfoContext(ctx, "Rebuilt shard refused: the node is past the record's generation",
						"node", t.node, "index", t.index,
						"epoch", fmt.Sprintf("%016x", t.place.WriteEpoch))
				case noPeerReachable(err):
					if deferred.CompareAndSwap(false, true) {
						slog.InfoContext(ctx, "Repair pass deferred: no peer of an owed shard answered",
							"node", t.node, "index", t.index, "err", err)
						stopPass()
					}
				default:
					reason := s.countFailure(err)
					slog.WarnContext(ctx, "Shard repair failed",
						"node", t.node, "index", t.index, "reason", string(reason),
						"epoch", fmt.Sprintf("%016x", t.place.WriteEpoch), "err", err)
				}
			}
		})
	}

	err := s.scan(passCtx, func(t task) error {
		owed.Add(1)
		s.owed.Add(1)
		s.pending.Add(1)
		select {
		case work <- t:
			return nil
		case <-passCtx.Done():
			return passCtx.Err()
		}
	})

	close(work)
	wg.Wait()

	// Whatever the pass could not rebuild is still owed, and the next pass will
	// find it again. Settling the gauge here rather than decrementing per item
	// keeps it from drifting when a pass is cut short.
	s.pending.Store(max(owed.Load(), 0))
	s.passes.Add(1)
	if deferred.Load() {
		s.deferred.Add(1)
		err = ErrPassDeferred
	}
	s.summarise(ctx, before, start, err)

	return err
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
		items, err := s.cfg.Meta.LeaderScanFrom(ctx, prefix, cursor, s.pageSize)
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
