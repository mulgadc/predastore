package meta

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/stretchr/testify/require"
)

func TestLastKeyLeaderConfirmationWithLagAndFailover(t *testing.T) {
	const host = "map-head-replica-test"
	voters := []config.NodeID{1, 2, 3}
	cfg := &config.Config{Hosts: []config.Host{{
		ID: 1, Addr: host, Nodes: []config.Node{
			{ID: 1, Role: config.RoleMeta, Port: 6311},
			{ID: 2, Role: config.RoleMeta, Port: 6312},
			{ID: 3, Role: config.RoleMeta, Port: 6313},
			{ID: 4, Role: config.RoleGate, Port: 6314},
		},
	}}}
	dirs := map[config.NodeID]string{1: t.TempDir(), 2: t.TempDir(), 3: t.TempDir()}
	replicas := make(map[config.NodeID]*Server)
	stops := make(map[config.NodeID]func())
	start := func(id config.NodeID, bootstrap bool) {
		tr := transport.NewPipeTransport(host, 6310+int(id))
		listener, err := tr.Listen()
		require.NoError(t, err)
		resolver, err := rpc.NewResolver(cfg, id, tr)
		require.NoError(t, err)
		replica, err := New(Config{
			NodeID: id, DataDir: dirs[id], Peers: voters, Bootstrap: bootstrap,
			Listeners: []transport.Listener{listener}, Resolver: resolver,
			HeartbeatTimeout: 150 * time.Millisecond, ElectionTimeout: 150 * time.Millisecond,
			LeaderLeaseTimeout: 150 * time.Millisecond, CommitTimeout: 10 * time.Millisecond,
		})
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- replica.Run(ctx) }()
		replicas[id] = replica
		stops[id] = func() {
			cancel()
			require.NoError(t, <-done)
			tr.Close()
			delete(stops, id)
		}
	}
	t.Cleanup(func() {
		for _, stop := range stops {
			stop()
		}
	})
	for _, id := range voters {
		start(id, id == 1)
	}

	clientTransport := transport.NewPipeTransport("map-head-test-client", 0)
	t.Cleanup(func() { clientTransport.Close() })
	resolver, err := rpc.NewResolver(cfg, 4, clientTransport)
	require.NoError(t, err)
	pool := rpc.NewConnPool(4, resolver)
	t.Cleanup(func() { pool.Close() })
	client := func(ids ...config.NodeID) *Client {
		c, err := NewClient(ClientConfig{Client: rpc.NewClient(pool), Replicas: ids, Timeout: time.Second})
		require.NoError(t, err)
		return c
	}

	ctx := t.Context()
	all := client(voters...)
	var leader config.NodeID
	require.Eventually(t, func() bool {
		for _, id := range voters {
			status, err := all.Status(ctx, id)
			if err == nil && status.IsLeader {
				leader = id
				return true
			}
		}
		return false
	}, 5*time.Second, 20*time.Millisecond)
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

		route := client(follower, leader, remaining)
		require.Equal(t, follower, route.readOrder()[0])
		key, err = route.LeaderLastKey(ctx, testHeadPrefix)
		require.NoError(t, err)
		require.Equal(t, latest, key)
	}()

	stops[leader]()
	stops[remaining]()
	_, err = held.LeaderLastKey(ctx, testHeadPrefix)
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
}
