package engine

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

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
	if err != nil {
		require.ErrorIs(t, err, errSegmentBusy)
	}
	r, err := store.LookupRecoveredAt(t.Context(), key, 0, epoch, func(_ context.Context, candidate [32]byte, shard uint32, candidateEpoch uint64) (bool, error) {
		return candidate == key && shard == 0 && candidateEpoch == epoch, nil
	})
	require.NoError(t, err, "a late-published placement still needs its durable extent")
	require.NoError(t, r.Close())
	_, err = store.compactSegment(source, nil)
	require.NoError(t, err)
	require.Equal(t, payload, readValue(t, store, key, 0))
}

func TestDeleteReleasesDeferredPrepare(t *testing.T) {
	t.Run("exact epoch", func(t *testing.T) { testDeleteReleasesDeferredPrepare(t, true) })
	t.Run("all epochs", func(t *testing.T) { testDeleteReleasesDeferredPrepare(t, false) })
}

func testDeleteReleasesDeferredPrepare(t *testing.T, exact bool) {
	t.Helper()
	store, _ := openTestStore(t, WithMaxSegSize(20*KiB))
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

	_, err = store.compactSegment(source, nil)
	require.ErrorIs(t, err, errSegmentBusy)
	var deleted bool
	if exact {
		deleted, err = store.DeleteEpoch(key, 0, epoch)
	} else {
		deleted, err = store.Delete(key, 0)
	}
	require.NoError(t, err)
	require.True(t, deleted)
	_, err = store.compactSegment(source, nil)
	require.NoError(t, err)
	_, err = store.LookupRecoveredAt(t.Context(), key, 0, epoch, func(context.Context, [32]byte, uint32, uint64) (bool, error) {
		t.Error("deleted recovery record should never reach the authority")
		return true, nil
	})
	require.ErrorIs(t, err, ErrKeyNotFound)
}
