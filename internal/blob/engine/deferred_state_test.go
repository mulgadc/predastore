package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4"

	"github.com/stretchr/testify/require"
)

func TestS7DeferredStateFencesOlderStoreFormat(t *testing.T) {
	for _, trial := range []bool{false, true} {
		name := "first-deferred-row"
		if trial {
			name = "existing-deferred-row"
		}
		t.Run(name, func(t *testing.T) {
			store, dir := openTestStore(t)
			path := filepath.Join(dir, stateFilename)
			raw, err := os.ReadFile(path)
			require.NoError(t, err)
			var old state
			require.NoError(t, json.Unmarshal(raw, &old), "stores without deferred recovery retain the existing format")

			key := [32]byte{1}
			payload := []byte("acknowledged extent survives older open")
			epoch := prepareValue(t, store, key, 0, payload)
			absent := func(context.Context, [32]byte, uint32, uint64) (bool, error) { return false, nil }
			require.NoError(t, store.replayRecoveredPrepares(t.Context(), absent))
			require.NoError(t, store.Close())

			raw, err = os.ReadFile(path)
			require.NoError(t, err)
			require.Error(t, json.Unmarshal(raw, &old), "the old state reader must refuse the store")
			_, err = os.OpenFile(filepath.Join(dir, legacyStateTemp), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
			require.Error(t, err, "a stale older save must fail before replacing state")
			if trial {
				counters, ok := bytes.CutPrefix(raw, []byte(deferredStateHeader))
				require.True(t, ok)
				require.NoError(t, os.WriteFile(path, counters, 0600))
			}

			reopened, err := Open(dir, WithAEAD(testAEAD(t)), WithFreeSpaceWatermark(0, 0))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, reopened.Close()) })
			raw, err = os.ReadFile(path)
			require.NoError(t, err)
			require.Error(t, json.Unmarshal(raw, &old))

			named := func(context.Context, [32]byte, uint32, uint64) (bool, error) { return true, nil }
			r, err := reopened.LookupRecoveredAt(t.Context(), key, 0, epoch, named)
			require.NoError(t, err)
			got, err := io.ReadAll(r)
			require.NoError(t, err)
			require.NoError(t, r.Close())
			require.Equal(t, payload, got)
		})
	}
}

func TestS7DeferredStateRefusesAnActiveOlderStateWriter(t *testing.T) {
	store, dir := openTestStore(t)
	key := [32]byte{1}
	payload := []byte("prepared shard")
	epoch := prepareValue(t, store, key, 0, payload)
	legacy := filepath.Join(dir, legacyStateTemp)
	require.NoError(t, os.WriteFile(legacy, []byte("older writer owns this path"), 0600))

	absent := func(context.Context, [32]byte, uint32, uint64) (bool, error) { return false, nil }
	require.ErrorContains(t, store.replayRecoveredPrepares(t.Context(), absent), "legacy state writer still owns")
	require.ErrorIs(t, store.index.View(func(txn *badger.Txn) error {
		_, err := txn.Get(deferredKey(MakeKey(key, 0), epoch))
		return err
	}), badger.ErrKeyNotFound)
}
