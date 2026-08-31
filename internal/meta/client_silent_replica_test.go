package meta

import (
	"context"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// silentTestCfg names three meta nodes on one host: node 1 answers, node 2
// accepts connections and never answers, and node 3 is the client's own source.
func silentTestCfg() *config.Config {
	return &config.Config{
		Hosts: []config.Host{{
			ID:   1,
			Addr: "silent-test-host",
			Nodes: []config.Node{
				{ID: 1, Role: config.RoleMeta, Port: 6201},
				{ID: 2, Role: config.RoleMeta, Port: 6202},
				{ID: 3, Role: config.RoleMeta, Port: 6203},
			},
		}},
	}
}

// startSilentReplica binds node 2's address and accepts connections without
// ever serving one. That is the fault under test: a replica whose transport is
// up and whose process answers nothing, which is what a stopped or wedged node
// looks like from the other end. A refused dial would fail in microseconds and
// prove nothing.
func startSilentReplica(t *testing.T) {
	t.Helper()
	tr := transport.NewPipeTransport("silent-test-host", 6202)
	t.Cleanup(func() { tr.Close() })
	ln, err := tr.Listen()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		var held []transport.Conn
		defer func() {
			for _, c := range held {
				_ = c.Close()
			}
		}()
		for {
			conn, err := ln.Accept(ctx)
			if err != nil {
				return
			}
			held = append(held, conn)
		}
	}()
}

// startAnsweringReplica runs a real single-node meta replica as node 1.
func startAnsweringReplica(t *testing.T, cfg *config.Config) {
	t.Helper()
	tr := transport.NewPipeTransport("silent-test-host", 6201)
	t.Cleanup(func() { tr.Close() })
	ln, err := tr.Listen()
	require.NoError(t, err)

	res, err := rpc.NewResolver(cfg, 1, tr)
	require.NoError(t, err)

	svc, err := New(Config{
		NodeID:    1,
		DataDir:   t.TempDir(),
		Peers:     []config.NodeID{1},
		Bootstrap: true,
		Listeners: []transport.Listener{ln},
		Resolver:  res,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	probe := newSilentTestClient(t, cfg, []config.NodeID{1}, 2*time.Second)
	require.Eventually(t, func() bool {
		st, serr := probe.Status(context.Background(), 1)

		return serr == nil && st.IsLeader
	}, 10*time.Second, 20*time.Millisecond, "the answering replica never became leader")
}

func newSilentTestClient(t *testing.T, cfg *config.Config, replicas []config.NodeID, timeout time.Duration) *Client {
	t.Helper()
	tr := transport.NewPipeTransport("silent-test-client", 0)
	t.Cleanup(func() { tr.Close() })
	res, err := rpc.NewResolver(cfg, 3, tr)
	require.NoError(t, err)
	pool := rpc.NewConnPool(3, res)
	t.Cleanup(func() { pool.Close() })

	cli, err := NewClient(ClientConfig{
		Client:   rpc.NewClient(pool),
		Replicas: replicas,
		Timeout:  timeout,
	})
	require.NoError(t, err)

	return cli
}

// TestASilentCachedLeaderIsPaidForOnce is the incident. A four-host stress
// cluster had one host stopped while it held the raft leadership the gate had
// cached, so every read led with it, waited out the full deadline and then
// fell through to a replica that answered. Twenty-four operations at ten
// seconds each was 240s of a 274s pass, and every one of them returned 200:
// nothing failed, nothing retried, the cluster was simply ten seconds slower
// per operation for as long as the host stayed stopped.
func TestASilentCachedLeaderIsPaidForOnce(t *testing.T) {
	cfg := silentTestCfg()
	startSilentReplica(t)
	startAnsweringReplica(t, cfg)

	const attemptTimeout = 500 * time.Millisecond

	// Seeded through a client that cannot see node 2, so the reads below start
	// with node 2 cached as leader rather than displaced by this write.
	writer := newSilentTestClient(t, cfg, []config.NodeID{1}, attemptTimeout)
	require.NoError(t, writer.Put(context.Background(), "k", []byte("v")))

	cli := newSilentTestClient(t, cfg, []config.NodeID{1, 2}, attemptTimeout)
	cli.cacheLeader(2)

	// The first read has nothing to go on and waits node 2 out. That cost is
	// unavoidable and is not what was wrong.
	start := time.Now()
	got, err := cli.Get(context.Background(), "k")
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), got)
	assert.GreaterOrEqual(t, time.Since(start), attemptTimeout,
		"the first read should have waited out the silent leader")

	// Every read after it is the defect.
	for i := range 3 {
		start = time.Now()
		got, err = cli.Get(context.Background(), "k")
		took := time.Since(start)
		require.NoError(t, err)
		assert.Equal(t, []byte("v"), got)
		assert.Less(t, took, attemptTimeout,
			"read %d paid the silent leader's deadline again", i+1)
	}
}
