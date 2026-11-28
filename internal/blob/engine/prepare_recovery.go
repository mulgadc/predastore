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

type prepareAuthority = func(context.Context, [32]byte, uint32, uint64) (bool, error)

type prepareRecoveryKey struct {
	key   [32]byte
	index uint32
	epoch uint64
}

const deferredPrefix = 'q'

func deferredScan(idxKey []byte) []byte {
	return append(append(make([]byte, 0, preparedKeySize), deferredPrefix), idxKey...)
}

func deferredKey(idxKey []byte, epoch uint64) []byte {
	return binary.BigEndian.AppendUint64(deferredScan(idxKey), epoch)
}

// WithStartupPrepareRecovery verifies and replays named extents before serving.
func WithStartupPrepareRecovery(
	ctx context.Context,
	authorize func(context.Context, [32]byte, uint32, uint64) (bool, error),
) Option {
	return func(store *Store) error {
		if authorize != nil {
			store.authorizeRecovered = authorize
			store.deferredContext = ctx
			store.startupRecovery = func(store *Store) error {
				return store.replayRecoveredPrepares(ctx, authorize)
			}
		}
		return nil
	}
}

// WithStartupPartRecovery enables exact multipart placement checks on reopen.
func WithStartupPartRecovery(authorize func(context.Context, string, [32]byte, uint32, uint64) (bool, error)) Option {
	return func(store *Store) error {
		store.startupPartRecovery = authorize
		return nil
	}
}

// replayRecoveredPrepares runs before the store serves requests or compacts.
// authorize confirms that authoritative placement still names the exact shard.
func (store *Store) replayRecoveredPrepares(
	ctx context.Context,
	authorize func(context.Context, [32]byte, uint32, uint64) (bool, error),
) error {
	store.mutex.Lock()
	if store.closed {
		store.mutex.Unlock()
		return ErrClosedStore
	}
	if store.compactor != nil {
		store.mutex.Unlock()
		return errors.New("cannot rebuild recovery holds while compaction is running")
	}
	store.mutex.Unlock()

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
		if err := store.replayPrepareIfNamed(ctx, allocation.Key, record, ext, authorize); err != nil {
			return err
		}
	}
}

func (store *Store) replayPrepareIfNamed(
	ctx context.Context,
	idxKey [36]byte,
	record prepareRecord,
	ext extent,
	authorize func(context.Context, [32]byte, uint32, uint64) (bool, error),
) error {
	epoch := record.epoch
	needed, err := store.needsPrepareReplay(idxKey[:], epoch, ext)
	if err != nil || !needed {
		return err
	}
	key := [32]byte(idxKey[:32])
	index := binary.BigEndian.Uint32(idxKey[32:])
	var named bool
	if placement := record.part.Key(); placement != "" {
		if store.startupPartRecovery != nil {
			named, err = store.startupPartRecovery(ctx, placement, key, index, epoch)
		}
	} else {
		named, err = authorize(ctx, key, index, epoch)
	}
	if err != nil {
		return err
	}
	if !named {
		return store.deferPrepare(ctx, prepareRecoveryKey{key: key, index: index, epoch: epoch}, ext, record)
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

func (store *Store) deferPrepare(ctx context.Context, recovery prepareRecoveryKey, ext extent, record prepareRecord) error {
	if err := store.verifyPrepareExtent(recovery.key, recovery.index, ext); err != nil {
		if errors.Is(err, ErrIntegrity) || errors.Is(err, os.ErrNotExist) {
			slog.WarnContext(ctx, "cannot defer prepared shard", "segment", ext.SegNum, "offset", ext.Off, "error", err)
			return nil
		}
		return err
	}
	idxKey := MakeKey(recovery.key, recovery.index)
	row := deferredKey(idxKey, recovery.epoch)
	return store.index.Update(func(txn *badger.Txn) error {
		raw, err := readRaw(txn, row)
		if err == nil {
			current, _, err := decodeIndexValue(raw)
			if err != nil {
				return err
			}
			if ext.PSize > 0 && (current.SegNum != ext.SegNum || current.Off != ext.Off) {
				return txn.Set(tombstoneKey(ext.SegNum, ext.Off), tombstoneValue(ext.PSize))
			}
			if len(raw) == preparedValueSize && (record.whole || record.part.Key() != "") {
				return txn.Set(row, encodeDeferredValue(ext, recovery.epoch, store.clock.Now(), record))
			}
			return nil
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		if err := txn.Delete(preparedKey(idxKey, recovery.epoch)); err != nil {
			return err
		}
		return txn.Set(row, encodeDeferredValue(ext, recovery.epoch, store.clock.Now(), record))
	})
}

// LookupRecoveredAt checks current placement before opening a deferred shard.
func (store *Store) LookupRecoveredAt(
	ctx context.Context, key [32]byte, index uint32, epoch uint64,
	authorize func(context.Context, [32]byte, uint32, uint64) (bool, error),
) (Reader, error) {
	if authorize == nil {
		return nil, ErrKeyNotFound
	}
	idxKey := MakeKey(key, index)
	row := deferredKey(idxKey, epoch)
	err := store.index.View(func(txn *badger.Txn) error {
		_, err := txn.Get(row)
		return err
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}
	named, err := authorize(ctx, key, index, epoch)
	if err != nil {
		return nil, err
	}
	if !named {
		return nil, ErrKeyNotFound
	}
	r, err := store.lookupReader(row, key, index)
	if errors.Is(err, ErrKeyNotFound) {
		// Promotion or release can move the row during the authority check.
		return store.LookupAt(key, index, epoch)
	}
	if err != nil {
		return nil, err
	}
	if err := r.openDeferred(epoch); err != nil {
		_ = r.Close()
		return nil, err
	}
	return r, nil
}

func (store *Store) publishDeferredPrepare(key [32]byte, index uint32, epoch uint64, verified extent) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if store.closed {
		return ErrClosedStore
	}
	idxKey := MakeKey(key, index)
	promoted, err := store.promoteDeferred(deferredKey(idxKey, epoch), preparedKey(idxKey, epoch), verified)
	if err != nil || !promoted {
		return err
	}
	_, err = store.Commit(key, index, epoch)
	if errors.Is(err, ErrNotPrepared) {
		// A concurrent release can already have moved the epoch to retained.
		return nil
	}
	return err
}

func (store *Store) promoteDeferred(row, prepared []byte, verified extent) (bool, error) {
	for {
		promoted := false
		err := store.index.Update(func(txn *badger.Txn) error {
			ext, epoch, err := readIndexValue(txn, row)
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			if err != nil || ext != verified {
				return err
			}
			if err := txn.Set(prepared, encodePreparedValue(ext, epoch, retainedHoldStamp)); err != nil {
				return err
			}
			promoted = true
			return txn.Delete(row)
		})
		if !errors.Is(err, badger.ErrConflict) {
			return promoted, err
		}
	}
}

func (store *Store) needsPrepareReplay(idxKey []byte, epoch uint64, ext extent) (bool, error) {
	needed, duplicate := true, false
	tombstone := tombstoneKey(ext.SegNum, ext.Off)
	err := store.index.View(func(txn *badger.Txn) error {
		if _, err := txn.Get(tombstone); err == nil {
			needed = false
			return nil
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		for _, row := range [][]byte{retainedKey(idxKey, epoch), idxKey} {
			current, held, err := readIndexValue(txn, row)
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if held == epoch {
				needed = false
				duplicate = current.SegNum != ext.SegNum || current.Off != ext.Off
				return nil
			}
		}
		return nil
	})
	if err != nil || !duplicate || ext.PSize == 0 {
		return needed, err
	}
	// A prior compaction can leave a second synced copy behind.
	return false, store.index.Update(func(txn *badger.Txn) error {
		return txn.Set(tombstone, tombstoneValue(ext.PSize))
	})
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
	return store.stageVerifiedPrepareLocked(key, index, epoch, ext)
}

func (store *Store) stageVerifiedPrepareLocked(key [32]byte, index uint32, epoch uint64, ext extent) error {
	idxKey := MakeKey(key, index)
	row := preparedKey(idxKey, epoch)
	for {
		err := store.index.Update(func(txn *badger.Txn) error {
			if _, err := txn.Get(tombstoneKey(ext.SegNum, ext.Off)); err == nil {
				return ErrNotPrepared
			} else if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			if _, err := txn.Get(row); err == nil {
				return txn.Delete(deferredKey(idxKey, epoch))
			} else if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			if err := txn.Set(row, encodePreparedValue(ext, epoch, retainedHoldStamp)); err != nil {
				return err
			}
			return txn.Delete(deferredKey(idxKey, epoch))
		})
		if !errors.Is(err, badger.ErrConflict) {
			return err
		}
	}
}

func (ext extent) validatePrepare() error {
	if ext.Off < segHeaderSize || (ext.Off-segHeaderSize)%totalFragSize != 0 ||
		ext.LSize < 0 || ext.PSize < 0 || ext.PSize%totalFragSize != 0 {
		return fmt.Errorf("%w: invalid prepare extent", ErrIntegrity)
	}
	var count int64
	if ext.LSize != 0 {
		count = 1 + (ext.LSize-1)/fragBodySize
	}
	if ext.PSize/totalFragSize != count {
		return fmt.Errorf("%w: prepare extent size does not match its fragments", ErrIntegrity)
	}
	return nil
}

func (store *Store) verifyPrepareExtent(key [32]byte, index uint32, ext extent) error {
	return store.verifyPrepareExtentContext(context.Background(), key, index, ext)
}

// verifyPrepareExtentContext checks the whole extent while holding a segment reference.
func (store *Store) verifyPrepareExtentContext(ctx context.Context, key [32]byte, index uint32, ext extent) error {
	if err := ext.validatePrepare(); err != nil || ext.LSize == 0 {
		return err
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
	var anchor prepareFragmentAnchor
	for i := range ext.PSize / totalFragSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		offset := ext.Off + i*totalFragSize
		if _, err := seg.ReadAt(frag[:], offset); err != nil {
			return fmt.Errorf("read prepared fragment at %d: %w", offset, err)
		}
		if i == 0 {
			anchor = prepareFragmentAnchor{value: frag.valueNum(), first: frag.fragNum()}
		}
		body, err := frag.open(store.aead, key, index, store.storeID)
		if err != nil {
			return fmt.Errorf("open prepared fragment at %d: %w", offset, err)
		}
		if err := anchor.check(&frag, i, ext.LSize, len(body)); err != nil {
			return fmt.Errorf("prepared fragment at %d: %w", offset, err)
		}
	}
	return nil
}

type prepareFragmentAnchor struct {
	value uint64
	first uint64
}

func (a prepareFragmentAnchor) check(frag *fragment, index, size int64, bodyLen int) error {
	if index < 0 || frag.valueNum() != a.value || frag.fragNum() < a.first || frag.fragNum()-a.first != uint64(index) {
		return fmt.Errorf("%w: discontinuous prepared fragments", ErrIntegrity)
	}
	remaining := size - index*fragBodySize
	var flags fragFlags
	if remaining <= fragBodySize {
		flags = flagEndOfValue
	}
	if int64(bodyLen) != min(remaining, fragBodySize) || fragFlags(binary.BigEndian.Uint32(frag[24:28])) != flags {
		return fmt.Errorf("%w: inconsistent prepared fragment length", ErrIntegrity)
	}
	return nil
}
