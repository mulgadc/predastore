// Package reaper reclaims the shards a delete leaves behind.
//
// A delete no longer talks to shard nodes: it writes a tombstone naming the
// shards, removes the index entries that make the object visible, and
// returns. This is the coordinator that reads the tombstone back and does the
// deferred work.
//
// It is a sweep, not a subscription, modelled closely on internal/gate/repair:
// Config/New/Run/Pass/Stats, a worker pool over a channel, and ScanFrom paging
// with a cursor. It differs in what it pages — only the tombstone rows, so a
// cluster that is not deleting pays nothing for it — and in what it does with
// what it finds: reclaim a tombstone's shards and drop it, once every one of
// them is confirmed gone.
//
// Before deleting anything it re-reads the placement record at the
// tombstone's hash and compares its WriteEpoch against the tombstone's own.
// A plain existence check is not enough: deleteStoredObject writes the
// tombstone before it removes the placement record, so a pass landing in
// that window would see the very object being deleted, conclude it had been
// recreated, and drop the tombstone without reclaiming its shards — leaking
// them permanently, on every delete, on a window two raft writes wide. The
// epoch tells the two cases apart: a match is the same delete still in
// flight, left alone for a later pass; a mismatch is a genuine recreate —
// object hashes are deterministic, so it lands on the same nodes and
// indices — and the tombstone is dropped without touching a shard, since
// deleting there would delete the recreated object's data.
//
// It deliberately does not close the window between that check and the
// delete landing: a concurrent recreate in that narrow gap could still lose a
// race against the sweep. Closing it needs the shard delete itself to carry
// the placement's write epoch and the blob engine to honour it, which is a
// protocol change out of scope here. It is no worse than the window the
// inline delete already left against a concurrent PUT.
//
// A tombstone with WriteEpoch == 0 predates this field. Those deletes fanned
// their shards out inline before the tombstone was even durable, so there is
// nothing left to reclaim, and it is dropped without a placement check.
//
// There is no election. Every gate that runs repair also runs this sweep, and
// two gates racing the same tombstone issue the same idempotent shard deletes
// and the same idempotent tombstone removal — redundant RPCs, nothing else.
package reaper

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/mulgadc/predastore/internal/telemetry"
)

// MetaClient is the slice of the meta client a sweep needs: ScanFrom pages the
// tombstones, Get checks whether a hash has since been recreated, and Delete
// drops a tombstone once it is resolved either way.
type MetaClient interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
	ScanFrom(ctx context.Context, prefix, after string, limit int) ([]meta.Item, error)
}

var _ MetaClient = (*meta.Client)(nil)

// BlobClient is what the sweep needs of a blob node: the ability to remove
// one shard.
type BlobClient interface {
	Delete(ctx context.Context, node config.NodeID, req blob.DeleteRequest) (*blob.DeleteResponse, error)
}

var _ BlobClient = (*blob.Client)(nil)

// Config is everything a reaper sweep runs on.
type Config struct {
	Meta MetaClient
	Blob BlobClient

	// Workers bounds concurrent tombstone reclaims. Zero takes the default
	// below.
	Workers int

	// PageSize is how many tombstone rows a scan asks for at a time. Zero
	// defaults.
	PageSize int

	// Interval is the gap between the end of one pass and the start of the
	// next. Zero defaults.
	Interval time.Duration
}

// DefaultWorkers is the reclaim concurrency. Half the host, capped, the same
// reasoning repair uses: a delete waiting on the sweep is not urgent the way a
// missing shard is, but it should not be starved by everything else the
// process is doing either.
func DefaultWorkers() int { return min(runtime.NumCPU()/2+1, maxDefaultWorkers) }

const (
	maxDefaultWorkers = 8
	defaultPageSize   = 512
	defaultInterval   = 5 * time.Minute
)

// Stats is what a pass did. Scanned counts tombstones read, Reclaimed the ones
// removed — whether their shards were deleted or the key had been recreated —
// and Pending is what the last completed pass left owing.
type Stats struct {
	Passes    int64
	Scanned   int64
	Reclaimed int64
	Failed    int64
	Pending   int64
}

// Service sweeps for tombstones and reclaims what they name.
type Service struct {
	cfg      Config
	workers  int
	pageSize int
	interval time.Duration

	passes, scanned, reclaimed, failed, pending atomic.Int64
}

// New validates cfg and applies its defaults. It starts nothing.
func New(cfg Config) (*Service, error) {
	if cfg.Meta == nil || cfg.Blob == nil {
		return nil, errors.New("reaper needs a meta client and a blob client")
	}

	s := &Service{
		cfg:      cfg,
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
		Passes:    s.passes.Load(),
		Scanned:   s.scanned.Load(),
		Reclaimed: s.reclaimed.Load(),
		Failed:    s.failed.Load(),
		Pending:   s.pending.Load(),
	}
}

// Run sweeps until ctx is cancelled. A pass that fails is logged and retried
// on the next tick rather than stopping the service: the condition it is
// trying to fix is usually the same one that made it fail.
func (s *Service) Run(ctx context.Context) error {
	slog.InfoContext(ctx, "Reaper sweep started",
		"workers", s.workers, "interval_ms", s.interval.Milliseconds())

	for {
		if err := s.Pass(ctx); err != nil && !errors.Is(err, context.Canceled) {
			slog.ErrorContext(ctx, "Reaper pass failed", "err", err)
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
// It is not resumable: a restart part-way through starts the enumeration
// again from the beginning. Only the position is lost, never work — every
// reclaim is idempotent, both the shard deletes and the tombstone removal, so
// repeating a page costs redundant RPCs and nothing else.
func (s *Service) Pass(ctx context.Context) error {
	work := make(chan task)
	var wg sync.WaitGroup
	var owed atomic.Int64

	for range s.workers {
		wg.Go(func() {
			for t := range work {
				ok, err := s.reclaim(ctx, t)
				switch {
				case err != nil:
					s.failed.Add(1)
					slog.WarnContext(ctx, "Reaper reclaim failed", "key", t.key, "err", err)
				case ok:
					owed.Add(-1)
					s.reclaimed.Add(1)
				default:
					// A tombstone for a delete still in flight: neither
					// reclaimed nor failed, just still owed for a later pass.
				}
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

	// Whatever the pass could not reclaim is still owed, and the next pass
	// will find it again. Settling the gauge here rather than decrementing per
	// item keeps it from drifting when a pass is cut short.
	s.pending.Store(max(owed.Load(), 0))
	s.passes.Add(1)
	if err != nil {
		return err
	}

	return nil
}

// task is one tombstone row, decoded.
type task struct {
	key  string // the row's full stored key, as ScanFrom returned it
	info handlers.DeletedObjectInfo
}

// scan pages through the tombstone rows and emits each one it can decode. A
// row it cannot decode is logged and skipped rather than failing the pass: a
// tombstone from a future format must not stall reclaim for every other one.
func (s *Service) scan(ctx context.Context, emit func(task) error) error {
	prefix := handlers.TableKey(model.TableObjects, handlers.DeletedObjectPrefix)
	cursor := ""
	for {
		items, err := s.cfg.Meta.ScanFrom(ctx, prefix, cursor, s.pageSize)
		if err != nil {
			return fmt.Errorf("scan tombstones: %w", err)
		}
		if len(items) == 0 {
			return nil
		}

		for _, item := range items {
			cursor = item.Key

			var info handlers.DeletedObjectInfo
			if decErr := gob.NewDecoder(bytes.NewReader(item.Value)).Decode(&info); decErr != nil {
				slog.WarnContext(ctx, "Undecodable tombstone skipped", "key", item.Key, "err", decErr)

				continue
			}
			s.scanned.Add(1)
			if err := emit(task{key: item.Key, info: info}); err != nil {
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

// reclaim resolves one tombstone.
//
// It reports whether the tombstone was removed. A false result with a nil
// error means the tombstone was deliberately left alone — the delete that
// wrote it is still in flight — which is not a failure: it is still owed, and
// the caller must not count it against Failed. A false result with a non-nil
// error explains why the reclaim could not complete.
func (s *Service) reclaim(ctx context.Context, t task) (bool, error) {
	if t.info.WriteEpoch == 0 {
		// Legacy tombstone: its delete already fanned the shards out inline,
		// before this field existed to name the generation. Nothing remains
		// to reclaim.
		if err := s.cfg.Meta.Delete(ctx, t.key); err != nil {
			return false, fmt.Errorf("drop legacy tombstone: %w", err)
		}

		return true, nil
	}

	liveKey := handlers.TableKey(model.TableObjects, string(t.info.ObjectHash[:]))
	data, err := s.cfg.Meta.Get(ctx, liveKey)
	switch {
	case err == nil:
		place, decErr := handlers.DecodePlacement(data)
		if decErr != nil {
			return false, fmt.Errorf("decode live placement: %w", decErr)
		}

		if place.WriteEpoch == t.info.WriteEpoch {
			// Same generation at the same hash is the delete that wrote this
			// tombstone, caught before its placement removal, not a recreate.
			// The next pass finds the placement gone and reclaims normally.
			return false, nil
		}

		// A different epoch at the same hash is a genuine recreate. Object
		// hashes are deterministic, so the new write already superseded these
		// shards in place, and reclaiming superseded extents is the compactor's.
		if delErr := s.cfg.Meta.Delete(ctx, t.key); delErr != nil {
			return false, fmt.Errorf("drop tombstone superseded by a recreated key: %w", delErr)
		}

		return true, nil
	case errors.Is(err, meta.ErrNotFound):
		// No live record: the key has not been recreated, so it is safe to
		// reclaim the shards below.
	default:
		return false, fmt.Errorf("check for a recreated object: %w", err)
	}

	if err := s.deleteShards(ctx, t.info); err != nil {
		return false, err
	}

	if err := s.cfg.Meta.Delete(ctx, t.key); err != nil {
		return false, fmt.Errorf("drop reclaimed tombstone: %w", err)
	}

	return true, nil
}

// shardTarget is one shard a tombstone names: the node that held it and the
// index the write path assigned it.
type shardTarget struct {
	node  config.NodeID
	index int
}

// shardTargets rebuilds the (node, shardIndex) pairs a tombstone names, the
// same way the write and read paths do: data shards first at 0..k-1, then
// parity.
func shardTargets(info handlers.DeletedObjectInfo) []shardTarget {
	targets := make([]shardTarget, 0, len(info.DataShardNodes)+len(info.ParityNodes))
	for i, n := range info.DataShardNodes {
		targets = append(targets, shardTarget{node: n, index: i})
	}
	for i, n := range info.ParityNodes {
		targets = append(targets, shardTarget{node: n, index: len(info.DataShardNodes) + i})
	}

	return targets
}

// deleteShards removes every shard a tombstone names. A shard the node no
// longer holds answers Deleted: false, which is success — the delete this
// tombstone owes was already done, by an earlier pass or by the node itself.
// Anything else leaves the tombstone in place for the next pass, which is
// what recovers a node that was down when the delete happened.
func (s *Service) deleteShards(ctx context.Context, info handlers.DeletedObjectInfo) error {
	targets := shardTargets(info)

	var wg sync.WaitGroup
	errCh := make(chan error, len(targets))

	for _, target := range targets {
		wg.Add(1)
		go func(t shardTarget) {
			defer wg.Done()

			start := time.Now()
			resp, err := s.cfg.Blob.Delete(ctx, t.node, blob.DeleteRequest{
				Key:   info.ObjectHash,
				Index: uint32(t.index), //nolint:gosec // G115: index bounded by DataShards + ParityShards (small uint).
			})
			recordShardDelete(ctx, t.node, start, err)
			if err != nil {
				errCh <- fmt.Errorf("node %d index %d: %w", t.node, t.index, err)

				return
			}
			if !resp.Deleted {
				slog.DebugContext(ctx, "Reaper: shard already gone", "node", t.node, "index", t.index)
			}
		}(target)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return nil
}

// recordShardDelete counts one shard delete against one node, mirroring the
// telemetry the inline delete used to record so the shard-op series it feeds
// keeps meaning what it always has.
func recordShardDelete(ctx context.Context, node config.NodeID, start time.Time, err error) {
	outcome := telemetry.OutcomeSuccess
	if err != nil {
		outcome = telemetry.OutcomeError
	}
	telemetry.RecordShardOp(ctx, telemetry.ShardOpDelete, outcome, uint64(node), time.Since(start).Seconds())
	if err != nil {
		telemetry.RecordShardError(ctx, telemetry.ShardOpDelete, telemetry.ShardReasonTransport, uint64(node))
	}
}
