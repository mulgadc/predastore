// Package reaper reclaims the shards a failed delete leaves behind.
//
// The inline delete sends each shard delete itself. When one fails, it writes
// a tombstone naming the shards and their generation, removes the placement
// record, and returns. This is the coordinator that reads the tombstone back
// and does the deferred work.
//
// It is a sweep, not a subscription, modelled closely on internal/gate/repair:
// Config/New/Run/Pass/Stats, a worker pool over a channel, and ScanFrom paging
// with a cursor. It pages only the tombstone rows, so a cluster that is not
// deleting pays nothing for it, and it drops a tombstone only once every
// shard it names is confirmed gone.
//
// It scans two prefixes. A failed delete writes its row under
// DeletedEpochPrefix, keyed by object hash and epoch, so each failed delete of
// a distinct generation has its own row. Rows under DeletedObjectPrefix were
// written by an older release, and are still read.
//
// Every shard delete names the tombstone's WriteEpoch, and the blob node
// removes only rows at exactly that epoch. A later generation at the same
// position is untouched, whatever the order of the sweep and a write.
//
// Before deleting anything it reads the placement record at the tombstone's
// hash from the meta leader, and keeps the tombstone when the leader cannot
// answer: a lagging follower can name an older generation while the
// tombstoned one is still current. A placement at the tombstone's own epoch
// is the delete still in flight, because the delete writes its tombstone
// before it removes the placement, so the tombstone is left for a later pass.
// A placement at another epoch is a recreate: the sweep still deletes the
// tombstone's epoch, which cannot touch the recreate's shards, and keeps the
// tombstone until every shard delete succeeds.
//
// A DeletedObjectPrefix row at epoch zero predates the epoch field. Those
// deletes fanned their shards out inline before the tombstone was durable, so
// there is nothing left to reclaim, and it is dropped without a placement
// check. A DeletedEpochPrefix row at epoch zero is a failed delete of a
// pre-epoch object, and its shards are reclaimed at epoch zero.
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
// tombstones, LeaderGet reads the current placement at a hash, and Delete
// drops a tombstone once its shards are gone.
type MetaClient interface {
	LeaderGet(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
	ScanFrom(ctx context.Context, prefix, after string, limit int) ([]meta.Item, error)
}

var _ MetaClient = (*meta.Client)(nil)

// BlobClient is what the sweep needs of a blob node: the ability to remove
// one shard at one generation.
type BlobClient interface {
	DeleteEpoch(ctx context.Context, node config.NodeID, req blob.CommitRequest) (*blob.DeleteResponse, error)
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
// removed once their shards were deleted or the row was a legacy one with
// nothing owed, and Pending is what the last completed pass left owing.
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
	key    string // the row's full stored key, as ScanFrom returned it
	info   handlers.DeletedObjectInfo
	legacy bool // the row is under DeletedObjectPrefix, from an older release
}

// scan pages through both tombstone prefixes and emits each row it can decode.
func (s *Service) scan(ctx context.Context, emit func(task) error) error {
	for _, p := range []struct {
		prefix string
		legacy bool
	}{
		{handlers.DeletedEpochPrefix, false},
		{handlers.DeletedObjectPrefix, true},
	} {
		if err := s.scanPrefix(ctx, p.prefix, p.legacy, emit); err != nil {
			return err
		}
	}

	return nil
}

// scanPrefix pages through one tombstone prefix. A row it cannot decode is
// logged and skipped rather than failing the pass: a tombstone from a future
// format must not stall reclaim for every other one.
func (s *Service) scanPrefix(ctx context.Context, rowPrefix string, legacy bool, emit func(task) error) error {
	prefix := handlers.TableKey(model.TableObjects, rowPrefix)
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
			if err := emit(task{key: item.Key, info: info, legacy: legacy}); err != nil {
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
	if t.legacy && t.info.WriteEpoch == 0 {
		// Legacy tombstone: its delete already fanned the shards out inline,
		// before the epoch field existed to name the generation.
		if err := s.cfg.Meta.Delete(ctx, t.key); err != nil {
			return false, fmt.Errorf("drop legacy tombstone: %w", err)
		}

		return true, nil
	}

	liveKey := handlers.TableKey(model.TableObjects, string(t.info.ObjectHash[:]))
	data, err := s.cfg.Meta.LeaderGet(ctx, liveKey)
	switch {
	case err == nil:
		place, decErr := handlers.DecodePlacement(data)
		if decErr != nil {
			return false, fmt.Errorf("decode live placement: %w", decErr)
		}

		if place.WriteEpoch == t.info.WriteEpoch {
			// The delete that wrote this tombstone, caught before its
			// placement removal. A later pass finds the placement gone.
			return false, nil
		}
		// A recreate at another epoch. The shard deletes below name the
		// tombstone's epoch, so the recreate's shards stay.
	case errors.Is(err, meta.ErrNotFound):
		// No live record at the hash.
	default:
		return false, fmt.Errorf("read the live placement from the leader: %w", err)
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
		wg.Go(func() {
			start := time.Now()
			resp, err := s.cfg.Blob.DeleteEpoch(ctx, target.node, blob.CommitRequest{
				Key:   info.ObjectHash,
				Index: uint32(target.index), //nolint:gosec // G115: index bounded by DataShards + ParityShards (small uint).
				Epoch: info.WriteEpoch,
			})
			recordShardDelete(ctx, target.node, start, err)
			if err != nil {
				errCh <- fmt.Errorf("node %d index %d: %w", target.node, target.index, err)

				return
			}
			if !resp.Deleted {
				slog.DebugContext(ctx, "Reaper: shard already gone", "node", target.node, "index", target.index)
			}
		})
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
