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

func replayTestCfg() *config.Config {
	return &config.Config{
		Hosts: []config.Host{{
			ID:   1,
			Addr: "replay-test-host",
			Nodes: []config.Node{
				{ID: 1, Role: config.RoleMeta, Port: 6401},
				{ID: 2, Role: config.RoleMeta, Port: 6402},
			},
		}},
	}
}

// startReplayReplica runs a single-voter replica that serves OpMetaTxn on the
// store in dataDir, and returns a client for it and a stop that waits for the
// replica to shut down.
func startReplayReplica(t *testing.T, dataDir string, bootstrap bool) (*meta.Client, func()) {
	t.Helper()
	cfg := replayTestCfg()

	serverTr := transport.NewPipeTransport("replay-test-host", 6401)
	ln, err := serverTr.Listen()
	require.NoError(t, err)
	serverRes, err := rpc.NewResolver(cfg, 1, serverTr)
	require.NoError(t, err)

	metaCfg := meta.Config{
		NodeID:             1,
		DataDir:            dataDir,
		Peers:              []config.NodeID{1},
		Bootstrap:          bootstrap,
		Listeners:          []transport.Listener{ln},
		Resolver:           serverRes,
		HeartbeatTimeout:   50 * time.Millisecond,
		ElectionTimeout:    50 * time.Millisecond,
		LeaderLeaseTimeout: 50 * time.Millisecond,
		CommitTimeout:      5 * time.Millisecond,
	}
	meta.ServeTxn(&metaCfg)
	svc, err := meta.New(metaCfg)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()

	clientTr := transport.NewPipeTransport("replay-test-client", 0)
	clientRes, err := rpc.NewResolver(cfg, 2, clientTr)
	require.NoError(t, err)
	connPool := rpc.NewConnPool(2, clientRes)
	cli, err := meta.NewClient(meta.ClientConfig{Client: rpc.NewClient(connPool), Replicas: []config.NodeID{1}})
	require.NoError(t, err)

	var stopped bool
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		connPool.Close()
		clientTr.Close()
		cancel()
		<-done
		serverTr.Close()
	}
	t.Cleanup(stop)

	require.Eventually(t, func() bool {
		st, err := cli.Status(t.Context(), 1)
		return err == nil && st.IsLeader
	}, 5*time.Second, 20*time.Millisecond, "replica never became leader")
	return cli, stop
}

// A replica with no snapshot restarts on its store and replays its whole log.
// The conditional create keeps its result, so its intent stays committed and
// its placement stays published, and a cleanup that trusts the intent keeps
// the object.
func TestAConditionalCreateSurvivesARestartWithoutASnapshot(t *testing.T) {
	dataDir := t.TempDir()
	cli, stop := startReplayReplica(t, dataDir, true)
	ctx := t.Context()

	require.NoError(t, cli.Put(ctx, "intents/k", []byte("pending")))
	outcome, _, err := cli.Txn(ctx,
		[]meta.TxnCheck{{Key: []byte("objects/k"), Absent: true}, {Key: []byte("intents/k"), Value: []byte("pending")}},
		[]meta.TxnWrite{{Key: []byte("objects/k"), Value: []byte("gen-1")}, {Key: []byte("intents/k"), Value: []byte("committed")}})
	require.NoError(t, err)
	require.Equal(t, meta.TxnApplied, outcome)
	stop()

	cli, _ = startReplayReplica(t, dataDir, false)

	// A leader read waits until the replica has applied its whole log.
	intent, err := cli.LeaderGet(ctx, "intents/k")
	require.NoError(t, err)
	assert.Equal(t, "committed", string(intent))
	placement, err := cli.LeaderGet(ctx, "objects/k")
	require.NoError(t, err)
	assert.Equal(t, "gen-1", string(placement))
	items, err := cli.LeaderScanFrom(ctx, "", "", 0)
	require.NoError(t, err)
	keys := make([]string, 0, len(items))
	for _, item := range items {
		keys = append(keys, item.Key)
	}
	assert.Equal(t, []string{"intents/k", "objects/k"}, keys, "a scan never returns the applied row")

	err = cli.Put(ctx, "\x00raft/applied", []byte("x"))
	assert.ErrorIs(t, err, meta.ErrReservedKey, "the client refuses before it sends")
	for op, body := range map[rpc.Opcode][]byte{meta.OpMetaPut: []byte("x"), meta.OpMetaPutMax: []byte("x"), meta.OpMetaSwap: []byte("x"), meta.OpMetaDelete: nil} {
		resp, err := cli.SendWrite(ctx, 1, op, "\x00raft/applied", body)
		require.NoError(t, err)
		assert.Equal(t, meta.ErrCodeInvalid, resp.Err, "opcode %d", op)
	}
	intent, err = cli.LeaderGet(ctx, "intents/k")
	require.NoError(t, err)
	assert.Equal(t, "committed", string(intent))
}
