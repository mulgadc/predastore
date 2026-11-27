package engine

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/dgraph-io/badger/v4"
)

// WithStartupPrepareRecovery verifies and replays named extents before serving.
func WithStartupPrepareRecovery(
	ctx context.Context,
	authorize func(context.Context, [32]byte, uint32, uint64) (bool, error),
) Option {
	return func(store *Store) error {
		if authorize != nil {
			store.startupRecovery = func(store *Store) error {
				return store.replayRecoveredPrepares(ctx, authorize)
			}
		}
		return nil
	}
}

// replayRecoveredPrepares runs before the store serves requests or compacts.
// authorize confirms that authoritative placement still names the exact shard.
func (store *Store) replayRecoveredPrepares(
	ctx context.Context,
	authorize func(context.Context, [32]byte, uint32, uint64) (bool, error),
) error {
	sidecars, err := filepath.Glob(filepath.Join(store.dir, "*.idx"))
	if err != nil {
		return err
	}
	for _, path := range sidecars {
		if err := store.replayPrepareSidecar(ctx, path, authorize); err != nil {
			return fmt.Errorf("recover %s: %w", filepath.Base(path), err)
		}
	}
	return nil
}

func (store *Store) replayPrepareSidecar(
	ctx context.Context,
	path string,
	authorize func(context.Context, [32]byte, uint32, uint64) (bool, error),
) error {
	var number uint64
	if _, err := fmt.Sscanf(filepath.Base(path), "%d.idx", &number); err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	reader := bufio.NewReader(f)

	var row [idxEntrySize]byte
	var allocation idxEntry
	haveAllocation := false
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := io.ReadFull(reader, row[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		if row[0]&0x80 == 0 {
			allocation, err = decodeIdxEntry(row[:])
			if err != nil {
				return err
			}
			haveAllocation = true
			continue
		}
		record, ok := decodePrepareRecord(row[:])
		match := haveAllocation && ok && record.off == allocation.Off && record.epoch != 0
		haveAllocation = false
		if !match {
			continue
		}
		ext := extent{SegNum: number, Off: allocation.Off, PSize: allocation.PSize, LSize: record.size}
		if err := store.replayPrepareIfNamed(ctx, allocation.Key, record.epoch, ext, authorize); err != nil {
			return err
		}
	}
}

func (store *Store) replayPrepareIfNamed(
	ctx context.Context,
	idxKey [36]byte,
	epoch uint64,
	ext extent,
	authorize func(context.Context, [32]byte, uint32, uint64) (bool, error),
) error {
	needed, err := store.needsPrepareReplay(idxKey[:], epoch, ext)
	if err != nil || !needed {
		return err
	}
	key := [32]byte(idxKey[:32])
	index := binary.BigEndian.Uint32(idxKey[32:])
	named, err := authorize(ctx, key, index, epoch)
	if err != nil || !named {
		return err
	}
	if err := store.stageRecoveredPrepare(key, index, epoch, ext); err != nil {
		if errors.Is(err, ErrIntegrity) || errors.Is(err, os.ErrNotExist) {
			slog.WarnContext(ctx, "cannot replay prepared shard", "segment", ext.SegNum, "offset", ext.Off, "error", err)
			return nil
		}
		return err
	}
	_, err = store.Commit(key, index, epoch)
	return err
}

func (store *Store) needsPrepareReplay(idxKey []byte, epoch uint64, ext extent) (bool, error) {
	needed := true
	err := store.index.View(func(txn *badger.Txn) error {
		for _, row := range [][]byte{tombstoneKey(ext.SegNum, ext.Off), retainedKey(idxKey, epoch)} {
			if _, err := txn.Get(row); err == nil {
				needed = false
				return nil
			} else if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
		}
		_, current, err := readIndexValue(txn, idxKey)
		if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		needed = current != epoch
		return nil
	})
	return needed, err
}

// stageRecoveredPrepare stages an exact extent before the compactor starts.
func (store *Store) stageRecoveredPrepare(key [32]byte, index uint32, epoch uint64, ext extent) error {
	if epoch == 0 {
		return ErrNotPrepared
	}
	if err := store.verifyPrepareExtent(key, index, ext); err != nil {
		return err
	}

	store.mutex.Lock()
	defer store.mutex.Unlock()
	if store.closed {
		return ErrClosedStore
	}
	if store.compactor != nil {
		return errors.New("cannot stage recovery while compaction is running")
	}
	row := preparedKey(MakeKey(key, index), epoch)
	for {
		err := store.index.Update(func(txn *badger.Txn) error {
			if _, err := txn.Get(tombstoneKey(ext.SegNum, ext.Off)); err == nil {
				return ErrNotPrepared
			} else if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			if _, err := txn.Get(row); err == nil {
				return nil
			} else if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			return txn.Set(row, encodePreparedValue(ext, epoch, retainedHoldStamp))
		})
		if !errors.Is(err, badger.ErrConflict) {
			return err
		}
	}
}

// verifyPrepareExtent checks the whole extent while holding a segment reference.
// An allocation record can be on disk even if the body never finished.
func (store *Store) verifyPrepareExtent(key [32]byte, index uint32, ext extent) error {
	if ext.Off < segHeaderSize || (ext.Off-segHeaderSize)%totalFragSize != 0 ||
		ext.LSize < 0 || ext.PSize < 0 || ext.PSize%totalFragSize != 0 {
		return fmt.Errorf("%w: invalid prepare extent", ErrIntegrity)
	}
	count := ext.PSize / totalFragSize
	if ext.LSize == 0 {
		if count == 0 {
			return nil
		}
		return fmt.Errorf("%w: nonempty extent for an empty shard", ErrIntegrity)
	}
	if count != 1+(ext.LSize-1)/fragBodySize {
		return fmt.Errorf("%w: prepare extent size does not match its fragments", ErrIntegrity)
	}

	store.mutex.Lock()
	if store.closed {
		store.mutex.Unlock()
		return ErrClosedStore
	}
	seg, err := store.getSegment(ext.SegNum, false)
	if err == nil {
		seg.addRef()
	}
	store.mutex.Unlock()
	if err != nil {
		return err
	}
	defer seg.releaseRef()

	info, err := seg.Stat()
	if err != nil {
		return err
	}
	if ext.PSize > info.Size() || ext.Off > info.Size()-ext.PSize {
		return fmt.Errorf("%w: prepare extent extends beyond the segment", ErrIntegrity)
	}

	var frag fragment
	var valueNum, firstFrag uint64
	for i := range count {
		offset := ext.Off + i*totalFragSize
		if _, err := seg.ReadAt(frag[:], offset); err != nil {
			return fmt.Errorf("read prepared fragment at %d: %w", offset, err)
		}
		if i == 0 {
			valueNum, firstFrag = frag.valueNum(), frag.fragNum()
		}
		if frag.valueNum() != valueNum || frag.fragNum() < firstFrag || frag.fragNum()-firstFrag != uint64(i) {
			return fmt.Errorf("%w: discontinuous prepared fragments at %d", ErrIntegrity, offset)
		}
		body, err := frag.open(store.aead, key, index, store.storeID)
		if err != nil {
			return fmt.Errorf("open prepared fragment at %d: %w", offset, err)
		}
		expected := min(ext.LSize-i*fragBodySize, fragBodySize)
		var flags fragFlags
		if i == count-1 {
			flags = flagEndOfValue
		}
		if int64(len(body)) != expected || fragFlags(binary.BigEndian.Uint32(frag[24:28])) != flags {
			return fmt.Errorf("%w: inconsistent prepared fragment length at %d", ErrIntegrity, offset)
		}
	}
	return nil
}
