package meta

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/stretchr/testify/require"
)

// trimTestReplica starts a single voter that snapshots only when asked and
// keeps one trailing entry, so a snapshot truncates nearly the whole log.
func trimTestReplica(t *testing.T) *Server {
	t.Helper()
	cfg := &config.Config{Hosts: []config.Host{{
		ID: 1, Addr: "trim-host",
		Nodes: []config.Node{{ID: 1, Role: config.RoleMeta, Port: 6401}},
	}}}
	tr := transport.NewPipeTransport("trim-host", 6401)
	t.Cleanup(func() { tr.Close() })
	ln, err := tr.Listen()
	require.NoError(t, err)
	res, err := rpc.NewResolver(cfg, 1, tr)
	require.NoError(t, err)

	svc, err := New(Config{
		NodeID: 1, DataDir: t.TempDir(), Peers: []config.NodeID{1}, Bootstrap: true,
		Listeners: []transport.Listener{ln}, Resolver: res,
		HeartbeatTimeout: 50 * time.Millisecond, ElectionTimeout: 50 * time.Millisecond,
		LeaderLeaseTimeout: 50 * time.Millisecond, CommitTimeout: 5 * time.Millisecond,
		SnapshotInterval: time.Hour, SnapshotThreshold: 1 << 40, TrailingLogs: 1,
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	require.Eventually(t, svc.LeaderKnown, 10*time.Second, 10*time.Millisecond)
	return svc
}

// TestSnapshotLogTrimDoesNotStallMetadataWrites is the metadata write every S3
// PUT waits on, issued while a snapshot truncates a long log. A truncation in
// one bolt transaction holds every append until it commits.
func TestSnapshotLogTrimDoesNotStallMetadataWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("fills a raft log of 320 MB")
	}
	const entries = 40_000
	svc := trimTestReplica(t)

	value := bytes.Repeat([]byte{0x70}, 8<<10)
	futures := make([]raft.ApplyFuture, 0, 1000)
	for i := range entries {
		data, err := json.Marshal(Command{Type: CommandPut, Key: fmt.Appendf(nil, "objects/%d", i%1000), Value: value})
		require.NoError(t, err)
		futures = append(futures, svc.raft.Apply(data, applyTimeout))
		if len(futures) == cap(futures) {
			for _, f := range futures {
				require.NoError(t, f.Error())
			}
			futures = futures[:0]
		}
	}
	for _, f := range futures {
		require.NoError(t, f.Error())
	}

	snapDone := make(chan error, 1)
	go func() { snapDone <- svc.raft.Snapshot().Error() }()

	var worst time.Duration
	for i := 0; ; i++ {
		select {
		case err := <-snapDone:
			require.NoError(t, err)
			first, err := svc.bolt.FirstIndex()
			require.NoError(t, err)
			require.Greater(t, first, uint64(entries-1000), "the snapshot did not truncate the log")
			t.Logf("worst %v over %d writes, first index %d", worst, i, first)
			require.Less(t, worst, 250*time.Millisecond, "a metadata write waited %v behind the log truncation", worst)
			return
		default:
		}
		start := time.Now()
		require.NoError(t, svc.apply(Command{Type: CommandPut, Key: fmt.Appendf(nil, "during/%d", i), Value: []byte("v")}))
		worst = max(worst, time.Since(start))
	}
}
