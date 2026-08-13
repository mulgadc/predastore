package predastore_test

import (
	"context"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mulgadc/predastore"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/testcerts"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startStatusCluster runs one meta replica reachable over quic on loopback,
// the shape a caller outside the predastore process — the intended use of
// predastore.NodeStatus — actually reaches a node through. It returns the
// config describing that one-node cluster and blocks until the replica has
// elected itself leader.
func startStatusCluster(t *testing.T) *predastore.Config {
	t.Helper()

	certPath, keyPath, _ := testcerts.Generate(t)

	quic, err := transport.NewQUICTransport("127.0.0.1", 0, certPath, keyPath)
	require.NoError(t, err)
	t.Cleanup(func() { quic.Close() })
	ln, err := quic.Listen()
	require.NoError(t, err)

	_, portStr, err := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)

	cfg := &config.Config{
		Hosts: []config.Host{{
			ID:      1,
			Addr:    "127.0.0.1",
			TLSCert: certPath,
			TLSKey:  keyPath,
			Nodes: []config.Node{
				{ID: 1, Role: config.RoleMeta, Port: port},
			},
		}},
	}

	res, err := rpc.NewResolver(cfg, 1, quic)
	require.NoError(t, err)

	leader := make(chan struct{})
	svc, err := meta.New(meta.Config{
		NodeID:    1,
		DataDir:   t.TempDir(),
		Peers:     []config.NodeID{1},
		Bootstrap: true,
		Listeners: []transport.Listener{ln},
		Resolver:  res,
		OnLeader:  sync.OnceFunc(func() { close(leader) }),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	select {
	case <-leader:
	case <-time.After(5 * time.Second):
		t.Fatal("replica never elected a leader")
	}

	return cfg
}

// TestNodeStatus_EndToEnd exercises the whole re-exported surface a caller
// outside this module has: build a Config, hand it and a node id to
// NodeStatus, get back the raft status that Config's own node answers with —
// with no other predastore import required, which is the point of status.go.
func TestNodeStatus_EndToEnd(t *testing.T) {
	cfg := startStatusCluster(t)

	require.Equal(t, []predastore.NodeID{1}, predastore.MetaNodesOnHost(cfg, 1))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	st, err := predastore.NodeStatus(ctx, cfg, 1)
	require.NoError(t, err)
	assert.Equal(t, "1", st.NodeID)
	assert.Equal(t, "Leader", st.State)
	assert.True(t, st.IsLeader)
	assert.NotEmpty(t, st.Leader)
	assert.NotEmpty(t, st.Term)
}

// TestNodeStatus_UnknownNode confirms a node id absent from the
// configuration is a plain error rather than a nil-pointer surprise: this is
// the mistake a caller passing the wrong id makes, not the cluster's, so it
// should read as one.
func TestNodeStatus_UnknownNode(t *testing.T) {
	cfg := startStatusCluster(t)

	_, err := predastore.NodeStatus(context.Background(), cfg, 99)
	require.Error(t, err)
}

// TestMetaNodesOnHost_NoHost confirms an id naming no host is nil rather
// than an error: a host running no meta node of its own is a normal
// topology, not a mistake.
func TestMetaNodesOnHost_NoHost(t *testing.T) {
	cfg := startStatusCluster(t)
	assert.Nil(t, predastore.MetaNodesOnHost(cfg, 99))
}
