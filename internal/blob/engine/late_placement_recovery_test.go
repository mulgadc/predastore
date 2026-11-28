package engine

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestCompactionReclaimsDeletedSpaceAroundDeferredPrepare(t *testing.T) {
	t.Run("object", func(t *testing.T) { testCompactionReclaimsDeletedSpace(t, "") })
	t.Run("part", func(t *testing.T) {
		testCompactionReclaimsDeletedSpace(t, "part:01010101-0101-4101-8101-010101010101:10000")
	})
}

func testCompactionReclaimsDeletedSpace(t *testing.T, part string) {
	t.Helper()
	const maxSize = 20 * KiB
	store, dir := openTestStore(t, WithMaxSegSize(maxSize))
	key, deleted := [32]byte{1}, [32]byte{2}
	payload := []byte("placement can arrive after compaction and restart")
	var epoch uint64
	if part == "" {
		epoch = prepareValue(t, store, key, 0, payload)
	} else {
		epoch = 11
		writer, err := store.AppendPlacement(key, 0, int64(len(payload)), epoch, false, part)
		require.NoError(t, err)
		_, err = writer.Write(payload)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
	}
	_, err := store.Commit(key, 0, epoch)
	require.NoError(t, err)
	source := extentOf(t, store, key, 0).SegNum
	require.NoError(t, store.index.Update(func(txn *badger.Txn) error {
		return txn.Delete(MakeKey(key, 0))
	}))
	putValue(t, store, deleted, 0, []byte("deleted bytes on the same segment"))
	require.Equal(t, source, extentOf(t, store, deleted, 0).SegNum)
	putValue(t, store, [32]byte{3}, 0, []byte("rotate the active segment"))
	removed, err := store.Delete(deleted, 0)
	require.NoError(t, err)
	require.True(t, removed)

	absent := func(context.Context, [32]byte, uint32, uint64) (bool, error) { return false, nil }
	require.NoError(t, store.replayRecoveredPrepares(t.Context(), absent))
	candidates, _, err := store.candidateSegments()
	require.NoError(t, err)
	require.Contains(t, candidates, source)
	stats, err := store.compactSegment(source, nil)
	require.NoError(t, err, "a deferred extent must move with the live data")
	require.Greater(t, stats.reclaimed, stats.bytes, "the segment must free more bytes than it copies")
	_, err = os.Stat(filepath.Join(dir, fmt.Sprintf(segFilename, source)))
	require.ErrorIs(t, err, os.ErrNotExist)

	require.NoError(t, store.Close())
	reopened, err := Open(dir, WithMaxSegSize(maxSize), WithAEAD(store.aead), WithStartupPrepareRecovery(t.Context(), absent),
		WithStartupPartRecovery(func(_ context.Context, placement string, gotKey [32]byte, index uint32, gotEpoch uint64) (bool, error) {
			return part != "" && placement == part && gotKey == key && index == 0 && gotEpoch == epoch, nil
		}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	_, err = reopened.Lookup(deleted, 0)
	require.ErrorIs(t, err, ErrKeyNotFound)
	if part != "" {
		require.Equal(t, payload, readValue(t, reopened, key, 0))
		return
	}
	_, err = reopened.Commit(key, 0, epoch)
	require.ErrorIs(t, err, ErrNotPrepared, "compaction must not make a deferred epoch publishable")
	reader, err := reopened.LookupRecoveredAt(t.Context(), key, 0, epoch, func(_ context.Context, gotKey [32]byte, index uint32, gotEpoch uint64) (bool, error) {
		return gotKey == key && index == 0 && gotEpoch == epoch, nil
	})
	require.NoError(t, err)
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, payload, got)
}

func TestReleaseKeepsDeferredPrepareReadableUntilGrace(t *testing.T) {
	store, _ := openTestStore(t, WithMaxSegSize(20*KiB))
	key := [32]byte{1}
	old := []byte("older live index row")
	putValue(t, store, key, 0, old)
	payload := []byte("displaced placement still in use by a reader")
	epoch := prepareValue(t, store, key, 0, payload)
	// The prepared row survived, but the index update from Commit did not.
	require.NoError(t, store.replayRecoveredPrepares(t.Context(), func(context.Context, [32]byte, uint32, uint64) (bool, error) {
		return false, nil
	}))
	row := deferredKey(MakeKey(key, 0), epoch)
	var source uint64
	require.NoError(t, store.index.View(func(txn *badger.Txn) error {
		ext, _, err := readIndexValue(txn, row)
		source = ext.SegNum
		return err
	}))
	putValue(t, store, [32]byte{2}, 0, []byte("roll to next segment"))

	dropped, err := store.sweepPrepared(-1)
	require.NoError(t, err)
	require.Zero(t, dropped, "an old prepared row may still back a delayed placement")

	require.NoError(t, store.ReleaseGeneration(key, 0, epoch))
	require.ErrorIs(t, store.index.View(func(txn *badger.Txn) error {
		_, err := txn.Get(row)
		return err
	}), badger.ErrKeyNotFound)
	require.Equal(t, old, readValue(t, store, key, 0))
	dropped, err = store.sweepRetained(time.Hour)
	require.NoError(t, err)
	require.Zero(t, dropped)
	reader, err := store.LookupAt(key, 0, epoch)
	require.NoError(t, err)
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, payload, got)

	dropped, err = store.sweepRetained(-1)
	require.NoError(t, err)
	require.Equal(t, 1, dropped)
	_, err = store.LookupAt(key, 0, epoch)
	require.ErrorIs(t, err, ErrKeyNotFound)
	_, err = store.Commit(key, 0, epoch)
	require.ErrorIs(t, err, ErrNotPrepared)
	_, err = store.compactSegment(source, nil)
	require.NoError(t, err)
	require.Equal(t, old, readValue(t, store, key, 0))
}

func TestPrepareReplayPreservesExtentUntilLatePlacement(t *testing.T) {
	store, _ := openTestStore(t, WithMaxSegSize(20*KiB))
	key := [32]byte{1}
	payload := []byte("placement becomes visible only after the recovery scan")
	epoch := prepareValue(t, store, key, 0, payload)
	_, err := store.Commit(key, 0, epoch)
	require.NoError(t, err)
	source := extentOf(t, store, key, 0).SegNum
	require.NoError(t, store.index.Update(func(txn *badger.Txn) error {
		return txn.Delete(MakeKey(key, 0))
	}))

	putValue(t, store, [32]byte{2}, 0, []byte("fill source"))
	putValue(t, store, [32]byte{3}, 0, []byte("roll to next segment"))

	require.NoError(t, store.replayRecoveredPrepares(t.Context(), func(context.Context, [32]byte, uint32, uint64) (bool, error) {
		return false, nil
	}))
	_, err = store.compactSegment(source, nil)
	require.NoError(t, err)
	store.authorizeRecovered = func(_ context.Context, candidate [32]byte, shard uint32, candidateEpoch uint64) (bool, error) {
		return candidate == key && shard == 0 && candidateEpoch == epoch, nil
	}
	r, err := store.LookupRecoveredAt(t.Context(), key, 0, epoch, store.authorizeRecovered)
	require.NoError(t, err, "a late-published placement still needs its durable extent")
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, payload, got)
	require.NoError(t, r.Close())
	_, err = store.LookupAt(key, 0, epoch)
	require.ErrorIs(t, err, ErrKeyNotFound)
	require.NoError(t, store.sweepDeferred(t.Context(), time.Time{}))
	require.Equal(t, payload, readValue(t, store, key, 0))
}

func TestDeleteReleasesDeferredPrepare(t *testing.T) {
	t.Run("exact epoch", func(t *testing.T) { testDeleteReleasesDeferredPrepare(t, true) })
	t.Run("all epochs", func(t *testing.T) { testDeleteReleasesDeferredPrepare(t, false) })
}

func testDeleteReleasesDeferredPrepare(t *testing.T, exact bool) {
	t.Helper()
	store, dir := openTestStore(t, WithMaxSegSize(20*KiB))
	key := [32]byte{1}
	epoch := prepareValue(t, store, key, 0, []byte("deleted deferred payload"))
	_, err := store.Commit(key, 0, epoch)
	require.NoError(t, err)
	source := extentOf(t, store, key, 0).SegNum
	require.NoError(t, store.index.Update(func(txn *badger.Txn) error {
		return txn.Delete(MakeKey(key, 0))
	}))
	putValue(t, store, [32]byte{2}, 0, []byte("fill source"))
	putValue(t, store, [32]byte{3}, 0, []byte("roll to next segment"))
	require.NoError(t, store.replayRecoveredPrepares(t.Context(), func(context.Context, [32]byte, uint32, uint64) (bool, error) {
		return false, nil
	}))

	// Delete while the destination bytes are durable and before repointing.
	store.beforeRelocateSwap = func() {
		store.beforeRelocateSwap = nil
		var deleted bool
		if exact {
			deleted, err = store.DeleteEpoch(key, 0, epoch)
		} else {
			deleted, err = store.Delete(key, 0)
		}
		require.NoError(t, err)
		require.True(t, deleted)
		if exact {
			require.NoError(t, store.syncIndex())
			panic("interrupted before relocation swap")
		}
	}
	if exact {
		require.PanicsWithValue(t, "interrupted before relocation swap", func() {
			_, _ = store.compactSegment(source, nil)
		})
	} else {
		_, err = store.compactSegment(source, nil)
		require.NoError(t, err)
	}
	require.Nil(t, store.beforeRelocateSwap)
	require.NoError(t, store.Close())

	reopened, err := Open(dir, WithAEAD(store.aead),
		WithStartupPrepareRecovery(t.Context(), func(context.Context, [32]byte, uint32, uint64) (bool, error) {
			t.Error("deleted recovery copies must not reach authority after reopen")
			return true, nil
		}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })

	_, err = reopened.LookupRecoveredAt(t.Context(), key, 0, epoch, func(context.Context, [32]byte, uint32, uint64) (bool, error) {
		t.Error("deleted recovery record should never reach the authority")
		return true, nil
	})
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestDeferredRetirementReclaimsOnlyConfirmedPlacements(t *testing.T) {
	const partPlacement = "part:01010101-0101-4101-8101-010101010101:10000"
	for _, placement := range []string{"", partPlacement} {
		name := "object"
		if placement != "" {
			name = "part"
		}
		t.Run(name, func(t *testing.T) {
			key, unknown, filler := [32]byte{1}, [32]byte{2}, [32]byte{3}
			expected := placement
			if expected == "" {
				expected = string(key[:])
			}
			const epoch uint64 = 11
			const maxSize = 20 * KiB
			var allow bool
			var calls int
			payload := []byte("acknowledged candidate bytes held through reader grace")
			store, dir := openTestStore(t, WithMaxSegSize(maxSize))
			writer, err := store.AppendPlacement(key, 0, int64(len(payload)), epoch, false, placement)
			require.NoError(t, err)
			_, err = writer.Write(payload)
			require.NoError(t, err)
			require.NoError(t, writer.Close())
			prepareValue(t, store, unknown, 0, []byte("unclassified candidate must survive"))
			putValue(t, store, filler, 0, []byte("roll over"))
			absent := func(context.Context, [32]byte, uint32, uint64) (bool, error) { return false, nil }
			require.NoError(t, store.replayRecoveredPrepares(t.Context(), absent))
			require.NoError(t, store.Close())

			store, err = Open(dir, WithAEAD(store.aead), WithMaxSegSize(maxSize),
				WithStartupPrepareRecovery(t.Context(), absent),
				WithDeferredRetirement(t.Context(), 0, func(_ context.Context, actual string, generation uint64) (bool, error) {
					require.Equal(t, expected, actual)
					require.Equal(t, epoch, generation)
					calls++
					return allow, nil
				}))
			require.NoError(t, err)
			t.Cleanup(func() { _ = store.Close() })

			require.NoError(t, store.sweepDeferred(t.Context(), time.Now().Add(-preparedMaxAge)))
			require.Zero(t, calls)
			require.NoError(t, store.sweepDeferred(t.Context(), time.Now().Add(time.Hour)))
			require.Equal(t, 1, calls)
			_, err = store.LookupAt(key, 0, epoch)
			require.ErrorIs(t, err, ErrKeyNotFound)

			allow = true
			require.NoError(t, store.sweepDeferred(t.Context(), time.Now().Add(time.Hour)))
			require.Equal(t, 2, calls)
			dropped, err := store.sweepRetained(retainedMaxAge)
			require.NoError(t, err)
			require.Zero(t, dropped)
			reader, err := store.LookupAt(key, 0, epoch)
			require.NoError(t, err)
			got, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.NoError(t, reader.Close())
			require.Equal(t, payload, got)

			dropped, err = store.sweepRetained(-1)
			require.NoError(t, err)
			require.Equal(t, 1, dropped)
			candidates, _, err := store.candidateSegments()
			require.NoError(t, err)
			require.Contains(t, candidates, uint64(0))
			stats, err := store.compactSegment(0, nil)
			require.NoError(t, err)
			require.Greater(t, stats.reclaimed, stats.bytes)
			_, err = os.Stat(filepath.Join(dir, fmt.Sprintf(segFilename, uint64(0))))
			require.ErrorIs(t, err, os.ErrNotExist)
			_, err = store.LookupAt(key, 0, epoch)
			require.ErrorIs(t, err, ErrKeyNotFound)
		})
	}
}

func TestS8DeferredRangeDoesNotReadUnrequestedFragments(t *testing.T) {
	store, _ := openTestStore(t, WithMaxSegSize(1*MiB))
	key := [32]byte{1}
	payload := make([]byte, 16*fragBodySize)
	payload[0] = 42
	epoch := prepareValue(t, store, key, 0, payload)
	require.NoError(t, store.replayRecoveredPrepares(t.Context(), func(context.Context, [32]byte, uint32, uint64) (bool, error) {
		return false, nil
	}))

	var ext extent
	require.NoError(t, store.index.View(func(txn *badger.Txn) error {
		var err error
		ext, _, err = readIndexValue(txn, deferredKey(MakeKey(key, 0), epoch))
		return err
	}))
	seg := store.segCache[ext.SegNum]
	tail := ext.Off + ext.PSize - 1
	var original [1]byte
	_, err := seg.ReadAt(original[:], tail)
	require.NoError(t, err)
	_, err = seg.WriteAt([]byte{original[0] ^ 0xff}, tail)
	require.NoError(t, err)

	named := func(context.Context, [32]byte, uint32, uint64) (bool, error) { return true, nil }
	r, err := store.LookupRecoveredAt(t.Context(), key, 0, epoch, named)
	require.NoError(t, err, "the lookup must leave unrelated fragments to background verification")
	var got [1]byte
	_, err = r.ReadAt(got[:], 0)
	require.NoError(t, err)
	require.Equal(t, payload[0], got[0])
	_, err = r.ReadAt(got[:], int64(len(payload)-1))
	require.ErrorIs(t, err, ErrIntegrity)
	require.NoError(t, r.Close())
	_, err = store.LookupAt(key, 0, epoch)
	require.ErrorIs(t, err, ErrKeyNotFound)

	store.authorizeRecovered = named
	require.NoError(t, store.sweepDeferred(t.Context(), time.Time{}))
	_, err = store.LookupAt(key, 0, epoch)
	require.ErrorIs(t, err, ErrKeyNotFound, "background recovery must reject a damaged extent")

	_, err = seg.WriteAt(original[:], tail)
	require.NoError(t, err)
	require.NoError(t, store.sweepDeferred(t.Context(), time.Time{}))
	require.NoError(t, store.sweepDeferred(t.Context(), time.Time{}))
	require.Equal(t, payload, readValue(t, store, key, 0))
}
