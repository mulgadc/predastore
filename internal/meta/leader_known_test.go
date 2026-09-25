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

// newLeaderKnownReplica builds one meta replica over a pipe transport without
// starting it, and returns it alongside a start function so a test can choose
// whether Run has happened before it reads the replica's health.
func newLeaderKnownReplica(t *testing.T) (*meta.Server, func()) {
	t.Helper()
	cfg := &config.Config{
		Hosts: []config.Host{{
			ID:    1,
			Addr:  "leader-known-host",
			Nodes: []config.Node{{ID: 1, Role: config.RoleMeta, Port: 6201}},
		}},
	}

	tr := transport.NewPipeTransport("leader-known-host", 6201)
	t.Cleanup(func() { tr.Close() })
	ln, err := tr.Listen()
	require.NoError(t, err)

	res, err := rpc.NewResolver(cfg, 1, tr)
	require.NoError(t, err)

	svc, err := meta.New(meta.Config{
		NodeID:    1,
		DataDir:   t.TempDir(),
		Peers:     []config.NodeID{1},
		Bootstrap: true,
		Listeners: []transport.Listener{ln},
		Resolver:  res,
		// A single voter elects itself unopposed, so the production second is
		// dead wait rather than contention this needs to exercise.
		HeartbeatTimeout:   50 * time.Millisecond,
		ElectionTimeout:    50 * time.Millisecond,
		LeaderLeaseTimeout: 50 * time.Millisecond,
		CommitTimeout:      5 * time.Millisecond,
	})
	require.NoError(t, err)

	start := func() {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- svc.Run(ctx) }()
		t.Cleanup(func() {
			cancel()
			<-done
		})
	}
	return svc, start
}

// A health check reaches the replica as soon as the node is built, which is
// before Run has constructed raft. Answering there must not take the process
// down: the honest answer is that no leader is observed yet.
func TestLeaderKnown_BeforeRunReportsNoLeader(t *testing.T) {
	svc, _ := newLeaderKnownReplica(t)

	assert.False(t, svc.LeaderKnown())
}

func TestLeaderKnown_AfterElectionReportsLeader(t *testing.T) {
	svc, start := newLeaderKnownReplica(t)
	start()

	assert.Eventually(t, svc.LeaderKnown, 10*time.Second, 20*time.Millisecond)
}

// A probe holding no replica at all must answer, not dereference nothing.
func TestLeaderKnown_NilServerReportsNoLeader(t *testing.T) {
	var svc *meta.Server

	assert.False(t, svc.LeaderKnown())
}

// The admin sampler polls from its own goroutine while Run builds raft, so
// the read must be safe against that write rather than merely usually early.
func TestLeaderKnown_PolledWhileRunStarts(t *testing.T) {
	svc, start := newLeaderKnownReplica(t)

	stop := make(chan struct{})
	polled := make(chan struct{})
	go func() {
		defer close(polled)
		for {
			select {
			case <-stop:
				return
			default:
				_ = svc.LeaderKnown()
			}
		}
	}()
	start()

	assert.Eventually(t, svc.LeaderKnown, 10*time.Second, 20*time.Millisecond)
	close(stop)
	<-polled
}
