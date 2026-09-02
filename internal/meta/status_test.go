package meta_test

import (
	"context"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// statusTestCfg names two meta nodes on one host: node 1 is the replica
// under test and node 2 exists only so a resolver can be built for a
// distinct "client" source, addressed to node 1 over the pipe.
func statusTestCfg() *config.Config {
	return &config.Config{
		Hosts: []config.Host{{
			ID:   1,
			Addr: "status-test-host",
			Nodes: []config.Node{
				{ID: 1, Role: config.RoleMeta, Port: 6101},
				{ID: 2, Role: config.RoleMeta, Port: 6102},
			},
		}},
	}
}

// startStatusReplica runs one meta replica over a pipe transport, bootstrapping
// it as a single-node cluster only when bootstrap is true, and returns a
// client addressed to it plus a cleanup that stops the replica.
func startStatusReplica(t *testing.T, bootstrap bool) *meta.Client {
	t.Helper()
	cfg := statusTestCfg()

	serverTr := transport.NewPipeTransport("status-test-host", 6101)
	t.Cleanup(func() { serverTr.Close() })
	ln, err := serverTr.Listen()
	require.NoError(t, err)

	serverRes, err := rpc.NewResolver(cfg, 1, serverTr)
	require.NoError(t, err)

	svc, err := meta.New(meta.Config{
		NodeID:    1,
		DataDir:   t.TempDir(),
		Peers:     []config.NodeID{1},
		Bootstrap: bootstrap,
		Listeners: []transport.Listener{ln},
		Resolver:  serverRes,
		// A single voter elects itself unopposed, so the production second is
		// dead wait rather than contention this needs to exercise.
		HeartbeatTimeout:   50 * time.Millisecond,
		ElectionTimeout:    50 * time.Millisecond,
		LeaderLeaseTimeout: 50 * time.Millisecond,
		CommitTimeout:      5 * time.Millisecond,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	clientTr := transport.NewPipeTransport("status-test-client", 0)
	t.Cleanup(func() { clientTr.Close() })
	clientRes, err := rpc.NewResolver(cfg, 2, clientTr)
	require.NoError(t, err)
	connPool := rpc.NewConnPool(2, clientRes)
	t.Cleanup(func() { connPool.Close() })

	cli, err := meta.NewClient(meta.ClientConfig{
		Client:   rpc.NewClient(connPool),
		Replicas: []config.NodeID{1},
	})
	require.NoError(t, err)
	return cli
}

// TestClient_Status_Leader confirms a bootstrapped single-node replica
// eventually reports itself as leader, with the wire fields sourced from
// the plan: node id, raft state, its own leader observation and log
// position.
func TestClient_Status_Leader(t *testing.T) {
	cli := startStatusReplica(t, true)

	require.Eventually(t, func() bool {
		st, err := cli.Status(context.Background(), 1)
		return err == nil && st.IsLeader
	}, 5*time.Second, 20*time.Millisecond, "replica never became leader")

	st, err := cli.Status(context.Background(), 1)
	require.NoError(t, err)
	assert.Equal(t, "1", st.NodeID)
	assert.Equal(t, "Leader", st.State)
	assert.True(t, st.IsLeader)
	assert.Equal(t, meta.RaftAddress(1), st.Leader)
	assert.NotEmpty(t, st.Term)
	assert.NotEmpty(t, st.CommitIndex)
	// A replica holds no route to dial itself, so it cannot resolve a dial
	// address for a leader that is itself.
	assert.Empty(t, st.LeaderAddr)
}

// TestClient_Status_NoLeader pins the exact condition this feature exists
// for: a replica that has never bootstrapped stays a follower with no
// leader indefinitely, and Status must answer that successfully rather than
// returning an error.
func TestClient_Status_NoLeader(t *testing.T) {
	cli := startStatusReplica(t, false)

	st, err := cli.Status(context.Background(), 1)
	require.NoError(t, err)
	assert.Equal(t, "1", st.NodeID)
	assert.Equal(t, "Follower", st.State)
	assert.False(t, st.IsLeader)
	assert.Empty(t, st.Leader)
	assert.Empty(t, st.LeaderAddr)
}
