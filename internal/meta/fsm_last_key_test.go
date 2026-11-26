package meta

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

const testHeadPrefix = "objects/arn:aws:s3:::predastore/vol/map/head."

func TestLastKeyUsesExactPrefix(t *testing.T) {
	db := newTestDB(t)
	for _, suffix := range []string{"1", "9", "1/map/head.3"} {
		dbSet(t, db, []byte(testHeadPrefix+suffix), []byte("value"))
	}
	dbSet(t, db, []byte("objects/arn:aws:s3:::predastore/vol/map/head/zz"), []byte("outside"))
	fsm := NewFSM(db)
	_, err := fsm.LastKey(testHeadPrefix)
	require.ErrorIs(t, err, errMapHeadIndexUnavailable)
	require.NoError(t, fsm.rebuildMapHeads())

	for _, test := range []struct{ prefix, want string }{
		{testHeadPrefix, testHeadPrefix + "9"},
		{testHeadPrefix + "1/map/head.", testHeadPrefix + "1/map/head.3"},
		{"objects/arn:aws:s3:::predastore/empty/map/head.", ""},
	} {
		key, err := fsm.LastKey(test.prefix)
		require.NoError(t, err)
		require.Equal(t, test.want, key)
	}
}

func TestLastKeyRebuildIgnoresPrunedRange(t *testing.T) {
	db := newTestDB(t)
	live := testHeadPrefix + "0000"
	for n := range 130 {
		dbSet(t, db, fmt.Appendf(nil, "%s%04d", testHeadPrefix, n), []byte("head"))
	}
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		for n := 1; n < 130; n++ {
			if err := txn.Delete(fmt.Appendf(nil, "%s%04d", testHeadPrefix, n)); err != nil {
				return err
			}
		}
		return nil
	}))
	fsm := NewFSM(db)
	require.NoError(t, fsm.rebuildMapHeads())
	key, err := fsm.LastKey(testHeadPrefix)
	require.NoError(t, err)
	require.Equal(t, live, key)
}

func TestLastKeyTracksCommittedBatch(t *testing.T) {
	fsm := NewFSM(newTestDB(t))
	require.NoError(t, fsm.rebuildMapHeads())
	commands := []Command{
		{Type: CommandPut, Key: []byte(testHeadPrefix + "1"), Value: []byte("first")},
		{Type: CommandPut, Key: []byte(testHeadPrefix + "2"), Value: []byte("second")},
		{Type: CommandSwap, Key: []byte(testHeadPrefix + "3"), Value: []byte("third")},
		{Type: CommandSwap, Key: []byte(testHeadPrefix + "3"), Value: []byte("third updated")},
		{Type: CommandDelete, Key: []byte(testHeadPrefix + "3")},
	}
	batch := make([]*raft.Log, 0, len(commands))
	for i, cmd := range commands {
		batch = append(batch, commandLog(t, uint64(i+1), cmd))
	}
	got := fsm.ApplyBatch(batch)
	require.Nil(t, got[0])
	require.Nil(t, got[1])
	require.Equal(t, []byte("third"), got[3])
	key, err := fsm.LastKey(testHeadPrefix)
	require.NoError(t, err)
	require.Equal(t, testHeadPrefix+"2", key)
	require.Nil(t, fsm.Apply(commandLog(t, 6, Command{Type: CommandDelete, Key: []byte(key)})))
	key, err = fsm.LastKey(testHeadPrefix)
	require.NoError(t, err)
	require.Equal(t, testHeadPrefix+"1", key)
}

func TestLastKeyRebuildAfterSnapshotRestore(t *testing.T) {
	key := testHeadPrefix + "2"
	src := newTestDB(t)
	dbSet(t, src, []byte(key), []byte("new snapshot"))
	sorted := snapshotStream(t, NewFSM(src))
	legacy, err := json.Marshal(map[string][]byte{key: []byte("new snapshot")})
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		data []byte
	}{{"sorted", sorted}, {"legacy", legacy}} {
		t.Run(tc.name, func(t *testing.T) {
			dst := newTestDB(t)
			dbSet(t, dst, []byte(testHeadPrefix+"3"), []byte("old view"))
			fsm := NewFSM(dst)
			require.NoError(t, fsm.rebuildMapHeads())
			require.NoError(t, fsm.Restore(io.NopCloser(bytes.NewReader(tc.data))))
			got, err := fsm.LastKey(testHeadPrefix)
			require.NoError(t, err)
			require.Equal(t, key, got)
			require.Error(t, fsm.Restore(io.NopCloser(bytes.NewReader([]byte("{")))))
			got, err = fsm.LastKey(testHeadPrefix)
			require.ErrorIs(t, err, errMapHeadIndexUnavailable)
			require.Empty(t, got)
		})
	}
}
