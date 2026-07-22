package s3db

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFSM_Restore_ExceedsSingleTransaction pins mulga-tjoz9: Restore must not be
// bounded by badger's per-transaction cap.
//
// Restore used to drop and rewrite every key inside one db.Update. Badger limits
// how much a single transaction may hold, so once a store's metadata set grew
// past that cap its snapshots became permanently unrestorable — the node failed
// to start with
//
//	raft: failed to restore snapshot: error="Txn is too big to fit into one request"
//	failed to create raft node: failed to load any existing snapshots
//
// and because every node restores the same oversized snapshot, they all failed
// together, taking the metadata plane (and the AMI catalogue behind
// DescribeImages) with them. This is a latent trap rather than a race: any
// restart trips it once the store is large enough, and the snapshot WRITE path
// has no matching limit, so unrestorable snapshots get written happily.
func TestFSM_Restore_ExceedsSingleTransaction(t *testing.T) {
	// Comfortably past badger's default per-transaction ceiling (~15% of a
	// 64MiB memtable), so the restore has to span multiple commits.
	const (
		entryCount = 2048
		valueSize  = 8 << 10 // 16MiB total
	)

	data := make(map[string][]byte, entryCount)
	for i := range entryCount {
		v := bytes.Repeat([]byte{byte(i % 251)}, valueSize)
		data[fmt.Sprintf("objects/blob-%06d", i)] = v
	}

	sink := &mockSnapshotSink{}
	require.NoError(t, (&FSMSnapshot{data: data}).Persist(sink))

	db := newTestDB(t)

	// Guard the premise: this payload genuinely cannot fit in one transaction,
	// so the test would still fail if the old single-Update Restore came back.
	err := db.Badger.Update(func(txn *badger.Txn) error {
		for k, v := range data {
			if err := txn.Set([]byte(k), v); err != nil {
				return err
			}
		}
		return nil
	})
	require.ErrorIs(t, err, badger.ErrTxnTooBig,
		"premise broken: payload now fits one txn, so this test no longer exercises the bug")

	require.NoError(t, NewFSM(db.Badger).Restore(io.NopCloser(bytes.NewReader(sink.buf))))

	for k, want := range data {
		got, err := db.Get([]byte(k))
		require.NoError(t, err, "key %s missing after restore", k)
		assert.Equal(t, want, got)
	}
}

// TestFSM_Restore_ClearsPriorState pins the other half of Restore's contract:
// the snapshot replaces existing state rather than merging into it. The clear
// moved from an in-transaction delete loop to badger's DropAll, so it needs its
// own guard.
func TestFSM_Restore_ClearsPriorState(t *testing.T) {
	db := newTestDB(t)
	require.NoError(t, db.Set([]byte("stale/key"), []byte("must not survive")))

	sink := &mockSnapshotSink{}
	require.NoError(t, (&FSMSnapshot{data: map[string][]byte{
		"fresh/key": []byte("from snapshot"),
	}}).Persist(sink))

	require.NoError(t, NewFSM(db.Badger).Restore(io.NopCloser(bytes.NewReader(sink.buf))))

	got, err := db.Get([]byte("fresh/key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("from snapshot"), got)

	_, err = db.Get([]byte("stale/key"))
	assert.Error(t, err, "state absent from the snapshot must not survive a restore")
}
