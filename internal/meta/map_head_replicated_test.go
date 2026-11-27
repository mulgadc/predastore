package meta

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestLastKeyLeaderConfirmationWithLagAndFailover(t *testing.T) {
	cluster := startMapHeadTestCluster(t)
	client, replicas, stops, start := cluster.client, cluster.replicas, cluster.stops, cluster.start
	ctx := t.Context()
	all := client(cluster.voters...)
	leader := cluster.leader(t, all)
	follower := leader%3 + 1
	remaining := follower%3 + 1

	old := testHeadPrefix + "0001"
	latest := testHeadPrefix + "0002"
	require.NoError(t, all.Put(ctx, old, []byte("old")))
	held := client(follower)
	require.Eventually(t, func() bool {
		_, err := held.Get(ctx, old)
		return err == nil
	}, 4*time.Second, 10*time.Millisecond)

	fsm := replicas[follower].fsm
	fsm.mu.Lock()
	func() {
		defer fsm.mu.Unlock()
		require.NoError(t, all.Put(ctx, latest, []byte("acknowledged")))
		require.ErrorIs(t, fsm.db.View(func(txn *badger.Txn) error {
			_, err := txn.Get([]byte(latest))
			return err
		}), badger.ErrKeyNotFound, "held follower must not have applied the new head")
		key, err := fsm.LastKey(testHeadPrefix)
		require.NoError(t, err)
		require.Equal(t, old, key)
		_, err = held.LeaderLastKey(ctx, testHeadPrefix)
		require.ErrorIs(t, err, ErrNoLeaderRead)

		raw, err := held.call(ctx, follower, OpMetaLastKey, request(testHeadPrefix, 1), nil)
		require.NoError(t, err)
		require.NotEmpty(t, raw.Err)
		require.False(t, raw.Last, "a follower must not confirm a request without the leader flag")

		route := client(follower, leader, remaining)
		require.Equal(t, follower, route.readOrder()[0])
		key, err = route.LeaderLastKey(ctx, testHeadPrefix)
		require.NoError(t, err)
		require.Equal(t, latest, key)
	}()

	stops[leader]()
	stops[remaining]()
	_, err := held.LeaderLastKey(ctx, testHeadPrefix)
	require.ErrorIs(t, err, ErrNoLeaderRead)

	start(remaining, false)
	replacement := client(follower, remaining)
	var key string
	require.Eventually(t, func() bool {
		var err error
		key, err = replacement.LeaderLastKey(ctx, testHeadPrefix)
		return err == nil
	}, 5*time.Second, 20*time.Millisecond)
	require.Equal(t, latest, key)

	// The serving leader must refuse before entering the barrier if a
	// restore left the head view unavailable.
	current := follower
	if st, err := replacement.Status(ctx, remaining); err == nil && st.IsLeader {
		current = remaining
	}
	replicas[current].fsm.setMapHeads(nil)
	_, err = client(current).LeaderLastKey(ctx, testHeadPrefix)
	require.ErrorIs(t, err, ErrNoLeaderRead)
	require.NoError(t, replicas[current].fsm.rebuildMapHeads())
	key, err = client(current).LeaderLastKey(ctx, testHeadPrefix)
	require.NoError(t, err)
	require.Equal(t, latest, key)
}

func TestRaftCannotCompactToFailedMetadataRestore(t *testing.T) {
	cluster := startMapHeadTestCluster(t)
	client := cluster.client(cluster.voters...)
	leader := cluster.leader(t, client)
	replica := cluster.replicas[leader]
	ctx := t.Context()

	oldKey := testHeadPrefix + "1"
	require.NoError(t, client.Put(ctx, oldKey, []byte("earlier snapshot")))
	require.NoError(t, replica.raft.Snapshot().Error())
	oldSnapshot, err := os.ReadDir(filepath.Join(replica.cfg.DataDir, "snapshots", "snapshots"))
	require.NoError(t, err)
	require.Len(t, oldSnapshot, 1)

	acknowledged := testHeadPrefix + "2"
	want := []byte("acknowledged after snapshot")
	require.NoError(t, client.Put(ctx, acknowledged, want))
	firstLog, err := replica.bolt.FirstIndex()
	require.NoError(t, err)

	truncated := append(snapshotMagic[:len(snapshotMagic):len(snapshotMagic)], 0, 0, 0, 1)
	require.Error(t, replica.fsm.Restore(io.NopCloser(bytes.NewReader(truncated))))
	require.Error(t, replica.raft.Snapshot().Error())
	_, err = cluster.client(leader).LeaderLastKey(ctx, testHeadPrefix)
	require.ErrorIs(t, err, ErrNoLeaderRead)

	after, err := os.ReadDir(filepath.Join(replica.cfg.DataDir, "snapshots", "snapshots"))
	require.NoError(t, err)
	require.Len(t, after, 1)
	require.Equal(t, oldSnapshot[0].Name(), after[0].Name())
	logAfter, err := replica.bolt.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, firstLog, logAfter)

	cluster.stops[leader]()
	cluster.start(leader, false)
	restarted := cluster.client(leader)
	require.Eventually(t, func() bool {
		data, err := restarted.Get(ctx, acknowledged)
		return err == nil && bytes.Equal(data, want)
	}, 5*time.Second, 20*time.Millisecond, "the restarted replica must replay the acknowledged head from retained logs")
}
