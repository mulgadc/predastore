package engine

import (
	"context"
	"encoding/binary"
	"log/slog"
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
		store.retirementContext = ctx
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
	shard     prepareRecoveryKey
	placement string
}

// retireDeferredTick bounds the metadata work between regular compaction passes.
func (store *Store) retireDeferredTick() {
	if store.retireRecovered == nil {
		return
	}
	ctx, cancel := context.WithTimeout(store.retirementContext, 2*time.Second)
	defer cancel()
	if err := store.sweepDeferred(ctx, store.clock.Now().Add(-store.retirementAge)); err != nil {
		slog.Debug("deferred shard retirement pass incomplete", "error", err)
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
			if len(key) != preparedKeySize || item.ValueSize() != int64(deferredValueSize) {
				it.Next()
				continue
			}
			raw, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			seen, err := decodePreparedAt(raw[:preparedValueSize])
			if err != nil || seen > cutoff.UnixNano() {
				it.Next()
				continue
			}
			candidate := deferredCandidate{shard: prepareRecoveryKey{
				key:   [32]byte(key[1:33]),
				index: binary.BigEndian.Uint32(key[33:37]),
				epoch: binary.BigEndian.Uint64(key[37:]),
			}}
			switch raw[preparedValueSize] {
			case deferredObject:
				candidate.placement = string(candidate.shard.key[:])
			case deferredPart:
				candidate.placement = model.MultipartPlacement(raw[preparedValueSize+1:]).Key()
			}
			if candidate.placement != "" && candidate.shard.epoch != 0 {
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

	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return err
		}
		shard := candidate.shard
		retired, err := store.retireRecovered(ctx, candidate.placement, shard.epoch)
		if err != nil || !retired {
			continue
		}
		if err := store.ReleaseGeneration(shard.key, shard.index, shard.epoch); err != nil {
			return err
		}
	}
	return nil
}
