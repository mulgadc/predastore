package engine

import (
	"context"
	"encoding/binary"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/mulgadc/predastore/internal/gate/model"
)

const (
	deferredObject    = 1
	deferredPart      = 2
	deferredValueSize = preparedValueSize + 1 + len(model.MultipartPlacement{})
)

// WithDeferredRetirement enables exact metadata checks for aged recovery rows.
func WithDeferredRetirement(ctx context.Context, age time.Duration, retire func(context.Context, string, uint64) (bool, error)) Option {
	return func(store *Store) error {
		if age <= 0 {
			age = preparedMaxAge
		}
		store.deferredContext = ctx
		store.retirementAge = age
		store.retireRecovered = retire
		return nil
	}
}

func encodeDeferredValue(ext extent, epoch uint64, seen time.Time, record prepareRecord) []byte {
	if !record.whole && record.part == (model.MultipartPlacement{}) {
		return encodePreparedValue(ext, epoch, retainedHoldStamp)
	}
	value := encodePreparedValue(ext, epoch, seen.UnixNano())
	kind := byte(deferredObject)
	if !record.whole {
		kind = deferredPart
	}
	value = append(value, kind)
	return append(value, record.part[:]...)
}

type deferredCandidate struct {
	shard  prepareRecoveryKey
	part   string
	ext    extent
	retire bool
}

// reconcileDeferredTick bounds the metadata work between compaction passes.
func (store *Store) reconcileDeferredTick(ctx context.Context) {
	if store.retireRecovered == nil && store.authorizeRecovered == nil {
		return
	}
	if err := store.sweepDeferred(ctx, store.clock.Now().Add(-store.retirementAge)); err != nil {
		slog.Debug("deferred shard pass incomplete", "error", err)
	}
}

func (store *Store) sweepDeferred(ctx context.Context, cutoff time.Time) error {
	prefix := []byte{deferredPrefix}
	cursor := store.retirementCursor
	if len(cursor) == 0 {
		cursor = prefix
	}
	var candidates []deferredCandidate
	err := store.index.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Seek(cursor)
		for scanned := 0; it.Valid(); scanned++ {
			if scanned == 128 || len(candidates) == 8 {
				store.retirementCursor = it.Item().KeyCopy(nil)
				return nil
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			item := it.Item()
			key := item.Key()
			if len(key) != preparedKeySize {
				it.Next()
				continue
			}
			raw, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			ext, epoch, err := decodeIndexValue(raw)
			if err != nil || epoch == 0 || epoch != binary.BigEndian.Uint64(key[37:]) {
				it.Next()
				continue
			}
			candidate := deferredCandidate{
				shard: prepareRecoveryKey{
					key:   [32]byte(key[1:33]),
					index: binary.BigEndian.Uint32(key[33:37]),
					epoch: epoch,
				},
				ext: ext,
			}
			if len(raw) == deferredValueSize {
				seen, err := decodePreparedAt(raw[:preparedValueSize])
				if err != nil {
					return err
				}
				switch raw[preparedValueSize] {
				case deferredObject:
					candidate.retire = seen <= cutoff.UnixNano()
				case deferredPart:
					candidate.part = model.MultipartPlacement(raw[preparedValueSize+1:]).Key()
					if candidate.part == "" {
						it.Next()
						continue
					}
					candidate.retire = seen <= cutoff.UnixNano()
				default:
					it.Next()
					continue
				}
			}
			if candidate.retire || store.authorizeRecovered != nil {
				candidates = append(candidates, candidate)
			}
			it.Next()
		}
		store.retirementCursor = nil
		return nil
	})
	if err != nil {
		return err
	}

	lookupCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	for _, candidate := range candidates {
		if err := lookupCtx.Err(); err != nil {
			return err
		}
		named, err := store.deferredNamed(lookupCtx, candidate)
		if err != nil {
			continue
		}
		if named {
			// At most one full extent per pass. Other rows remain durable.
			idxKey := MakeKey(candidate.shard.key, candidate.shard.index)
			store.retirementCursor = append(deferredKey(idxKey, candidate.shard.epoch), 0)
			return store.recoverDeferred(ctx, candidate)
		}
		if !candidate.retire || store.retireRecovered == nil {
			continue
		}
		shard := candidate.shard
		placement := candidate.part
		if placement == "" {
			placement = string(shard.key[:])
		}
		retired, err := store.retireRecovered(lookupCtx, placement, shard.epoch)
		if err != nil || !retired {
			continue
		}
		if err := store.ReleaseGeneration(shard.key, shard.index, shard.epoch); err != nil {
			return err
		}
	}
	return nil
}

func (store *Store) deferredNamed(ctx context.Context, candidate deferredCandidate) (bool, error) {
	shard := candidate.shard
	if candidate.part != "" && store.startupPartRecovery != nil {
		return store.startupPartRecovery(ctx, candidate.part, shard.key, shard.index, shard.epoch)
	}
	if candidate.part == "" && store.authorizeRecovered != nil {
		return store.authorizeRecovered(ctx, shard.key, shard.index, shard.epoch)
	}
	return false, nil
}

func (store *Store) recoverDeferred(ctx context.Context, candidate deferredCandidate) error {
	shard := candidate.shard
	if err := store.verifyPrepareExtentContext(ctx, shard.key, shard.index, candidate.ext); err != nil {
		if errors.Is(err, ErrIntegrity) || errors.Is(err, os.ErrNotExist) {
			slog.WarnContext(ctx, "cannot promote deferred shard", "segment", candidate.ext.SegNum, "offset", candidate.ext.Off, "error", err)
			return nil
		}
		return err
	}
	lookupCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	named, err := store.deferredNamed(lookupCtx, candidate)
	if err != nil || !named {
		return err
	}
	return store.publishDeferredPrepare(shard.key, shard.index, shard.epoch, candidate.ext)
}
