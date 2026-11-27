package reaper

// The intent pass settles what a conditional write left: committed intents
// are committed and released, aborted ones aborted, and aged pending ones
// fenced first. A row goes only when every candidate answered.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mulgadc/predastore/internal/gate/handlers"
)

// intentTask is one intent row, decoded, with the bytes the scan returned.
type intentTask struct {
	id     handlers.IntentID
	row    []byte
	intent handlers.Intent
}

var (
	errCleanupIncomplete = errors.New("intent cleanup incomplete")
	errFenceUndecided    = errors.New("intent fence undecided")
)

// reapIntents settles every intent a scan finds. Every row is measured
// against one clock reading, taken when the pass starts.
func (s *Service) reapIntents(ctx context.Context) error {
	now := s.now()
	work := make(chan intentTask)
	var wg sync.WaitGroup
	var owed atomic.Int64

	for range s.workers {
		wg.Go(func() {
			for t := range work {
				settled, err := s.settleIntent(ctx, t, now)
				switch {
				case err != nil:
					s.intentsFailed.Add(1)
					slog.WarnContext(ctx, "Reaper intent not settled; it stays for a later pass",
						"epoch", fmt.Sprintf("%016x", t.id.Epoch), "state", t.intent.State, "err", err)
				case settled:
					owed.Add(-1)
					s.intentsSettled.Add(1)
				default:
					// A pending intent younger than the age: a live writer may own it.
				}
			}
		})
	}

	err := s.scanIntents(ctx, func(t intentTask) error {
		owed.Add(1)
		select {
		case work <- t:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	close(work)
	wg.Wait()
	s.intentsOwed.Store(max(owed.Load(), 0))

	return err
}

// scanIntents pages through the intent table. A row it cannot read is logged
// and kept, so a row from a later format is never settled by a guess.
func (s *Service) scanIntents(ctx context.Context, emit func(intentTask) error) error {
	cursor := ""
	for {
		items, err := s.cfg.Meta.ScanFrom(ctx, handlers.IntentPrefix, cursor, s.pageSize)
		if err != nil {
			return fmt.Errorf("scan intents: %w", err)
		}
		for _, item := range items {
			cursor = item.Key
			id, ok := handlers.ParseIntentKey(item.Key)
			if !ok {
				slog.WarnContext(ctx, "Unparsable intent key skipped", "key", fmt.Sprintf("%x", item.Key))
				continue
			}
			in, decErr := handlers.DecodeIntent(item.Value)
			if decErr != nil {
				slog.WarnContext(ctx, "Undecodable intent skipped", "epoch", fmt.Sprintf("%016x", id.Epoch), "err", decErr)
				continue
			}
			s.intentsScanned.Add(1)
			if err := emit(intentTask{id: id, row: item.Value, intent: in}); err != nil {
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

// settleIntent settles one intent and reports whether its row is gone. Only
// the fence's answer decides a pending row, so a stale scan cannot abort a
// write whose own transaction won.
func (s *Service) settleIntent(ctx context.Context, t intentTask, now time.Time) (bool, error) {
	in := t.intent
	if in.State == handlers.IntentPending {
		if now.Sub(in.Created) < s.intentAge {
			return false, nil
		}
		result, decided, err := handlers.FenceIntent(ctx, s.cfg.Meta, t.id, t.row)
		switch result {
		case handlers.FenceGone:
			return true, nil
		case handlers.FenceApplied, handlers.FenceAborted, handlers.FenceCommitted:
			in = decided
		default:
			return false, errors.Join(errFenceUndecided, err)
		}
	}

	var complete bool
	switch in.State {
	case handlers.IntentCommitted:
		complete = handlers.CommitIntent(ctx, s.cfg.Blob, t.id, in).Complete
	case handlers.IntentAborted:
		complete = handlers.AbortIntent(ctx, s.cfg.Blob, t.id, in)
	default:
		return false, fmt.Errorf("%w: a %s intent after its fence", errFenceUndecided, in.State)
	}
	if !complete {
		return false, errCleanupIncomplete
	}
	if err := s.cfg.Meta.Delete(ctx, t.id.Key()); err != nil {
		return false, fmt.Errorf("drop settled intent: %w", err)
	}

	return true, nil
}
