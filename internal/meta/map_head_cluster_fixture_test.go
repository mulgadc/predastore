package meta

import (
	"context"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/stretchr/testify/require"
)

type mapHeadTestCluster struct {
	voters   []config.NodeID
	replicas map[config.NodeID]*Server
	stops    map[config.NodeID]func()
	start    func(config.NodeID, bool)
	client   func(...config.NodeID) *Client
}

func startMapHeadTestCluster(t testing.TB) mapHeadTestCluster {
	t.Helper()
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

	return mapHeadTestCluster{voters: voters, replicas: replicas, stops: stops, start: start, client: client}
}

func (c mapHeadTestCluster) leader(t testing.TB, all *Client) config.NodeID {
	t.Helper()

	ctx := t.Context()
	var leader config.NodeID
	require.Eventually(t, func() bool {
		for _, id := range c.voters {
			status, err := all.Status(ctx, id)
			if err == nil && status.IsLeader {
				leader = id
				return true
			}
		}
		return false
	}, 5*time.Second, 20*time.Millisecond)
	return leader
}
