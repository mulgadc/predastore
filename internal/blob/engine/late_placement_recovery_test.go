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
	require.NoError(t, store.replayRecoveredPrepares(t.Context(), func(_ context.Context, candidate [32]byte, shard uint32, candidateEpoch uint64) (bool, error) {
		return candidate == key && shard == 0 && candidateEpoch == epoch, nil
	}))
	r, err := store.LookupAt(key, 0, epoch)
	require.NoError(t, err, "a late-published placement still needs its durable extent")
	require.NoError(t, r.Close())
}
