package meta

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/require"
)

func encodedTestPlacement(epoch uint64) []byte {
	record := make([]byte, 45)
	record[1], record[2], record[44] = 3, 1, 1
	binary.BigEndian.PutUint64(record[model.PlacementEpochOffset:model.PlacementEpochEnd], epoch)
	return record
}

func TestPlacementRetirementOrdersAgainstExistingPublication(t *testing.T) {
	for name, key := range map[string][]byte{
		"whole object": append([]byte("objects/"), bytes.Repeat([]byte{0xff}, 32)...),
		"multipart":    []byte("objects/part:01010101-0101-4101-8101-010101010101:00001"),
	} {
		t.Run(name, func(t *testing.T) {
			db := newTestDB(t)
			fsm := NewFSM(db)
			old, next, newest := encodedTestPlacement(1), encodedTestPlacement(2), encodedTestPlacement(3)
			commands := []Command{
				{Type: CommandPut, Key: key, Value: old},
				{Type: CommandSwap, Key: key, Value: next},
				{Type: CommandRetirePlacement, Key: key, Value: old, Epoch: 2},
				{Type: CommandRetirePlacement, Key: key, Value: old, Epoch: 3},
				{Type: CommandRetirePlacement, Key: key, Value: next, Epoch: 3},
				{Type: CommandSwap, Key: key, Value: newest},
				{Type: CommandPut, Key: key, Value: newest},
				{Type: CommandPutMax, Key: key, Value: newest, Epoch: 3},
				{Type: CommandTxn, Writes: []TxnWrite{{Key: []byte("side-effect"), Value: []byte("must stay absent")}, {Key: key, Value: newest}}},
				{Type: CommandSwap, Key: key, Value: encodedTestPlacement(4)},
				{Type: CommandRetirePlacement, Key: key, Value: next, Epoch: 3},
			}
			logs := make([]*raft.Log, len(commands))
			for i, cmd := range commands {
				logs[i] = commandLog(t, uint64(i+1), cmd)
			}
			got := fsm.ApplyBatch(logs)
			require.Nil(t, got[0])
			require.Equal(t, old, got[1])
			require.ErrorIs(t, got[2].(error), errPlacementLive)
			require.ErrorIs(t, got[3].(error), errPlacementChanged)
			require.Nil(t, got[4])
			for _, result := range got[5:9] {
				require.ErrorIs(t, result.(error), errPlacementRetired)
			}
			require.Equal(t, next, got[9])
			require.Nil(t, got[10], "retry after the placement changes must preserve retirement")

			value, err := dbGet(db, key)
			require.NoError(t, err)
			require.Equal(t, encodedTestPlacement(4), value)
			_, err = dbGet(db, []byte("side-effect"))
			require.ErrorIs(t, err, badger.ErrKeyNotFound)
			_, err = dbGet(db, placementRetirementKey(key, 2))
			require.ErrorIs(t, err, badger.ErrKeyNotFound)
		})
	}
}

func TestPlacementRetirementSurvivesSnapshotRestore(t *testing.T) {
	key := append([]byte("objects/"), bytes.Repeat([]byte{0x01}, 32)...)
	source := NewFSM(newTestDB(t))
	require.Nil(t, source.Apply(commandLog(t, 1, Command{Type: CommandRetirePlacement, Key: key, Epoch: 7})))
	snapshot, err := source.Snapshot()
	require.NoError(t, err)
	defer snapshot.Release()
	sink := &mockSnapshotSink{}
	require.NoError(t, snapshot.Persist(sink))

	db := newTestDB(t)
	restored := NewFSM(db)
	require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(sink.buf))))
	require.ErrorIs(t, restored.Apply(commandLog(t, 2, Command{Type: CommandSwap, Key: key, Value: encodedTestPlacement(7)})).(error), errPlacementRetired)
	_, err = dbGet(db, key)
	require.ErrorIs(t, err, badger.ErrKeyNotFound)
	require.Equal(t, []byte(nil), restored.Apply(commandLog(t, 3, Command{Type: CommandSwap, Key: key, Value: encodedTestPlacement(8)})))
}

func TestPlacementRetirementSurvivesRaftLeaderChange(t *testing.T) {
	cluster := startMapHeadTestCluster(t)
	client := cluster.client(cluster.voters...)
	leader := cluster.leader(t, client)
	key := append([]byte("objects/"), bytes.Repeat([]byte{0x81}, 32)...)
	old, abandoned, replacement := encodedTestPlacement(1), encodedTestPlacement(2), encodedTestPlacement(3)
	ctx := t.Context()
	require.NoError(t, client.Put(ctx, string(key), old))
	require.NoError(t, cluster.replicas[leader].apply(Command{
		Type: CommandRetirePlacement, Key: key, Value: old, Epoch: 2,
	}))

	cluster.stops[leader]()
	var newLeader config.NodeID
	require.Eventually(t, func() bool {
		for _, id := range cluster.voters {
			if id == leader {
				continue
			}
			status, err := client.Status(ctx, id)
			if err == nil && status.IsLeader {
				newLeader = id
				return true
			}
		}
		return false
	}, 5*time.Second, 20*time.Millisecond)
	client = cluster.client(newLeader)
	_, err := client.Swap(ctx, string(key), abandoned)
	require.ErrorContains(t, err, errPlacementRetired.Error())
	stored, err := client.AuthorityGet(ctx, string(key))
	require.NoError(t, err)
	require.Equal(t, old, stored)

	displaced, err := client.Swap(ctx, string(key), replacement)
	require.NoError(t, err)
	require.Equal(t, old, displaced)
	stored, err = client.AuthorityGet(ctx, string(key))
	require.NoError(t, err)
	require.Equal(t, replacement, stored)
}
