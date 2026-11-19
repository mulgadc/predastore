package meta

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// swapRetryCfg names two meta nodes on one host: node 1 is the replica under
// test and node 2 is the client's own source.
func swapRetryCfg() *config.Config {
	return &config.Config{
		Hosts: []config.Host{{
			ID:   1,
			Addr: "swap-retry-host",
			Nodes: []config.Node{
				{ID: 1, Role: config.RoleMeta, Port: 6301},
				{ID: 2, Role: config.RoleMeta, Port: 6302},
			},
		}},
	}
}

// lossySwapReplica applies every swap it receives and drops the response to
// the first one. That is the fault: the command reached the log and the writer
// never learned it, which no amount of care on the client can distinguish from
// a command that never arrived.
type lossySwapReplica struct {
	mu      sync.Mutex
	stored  []byte
	applied [][]byte
}

func (r *lossySwapReplica) handleSwap(_ context.Context, h MetaRequest, stream transport.Stream) error {
	value, err := io.ReadAll(stream)
	if err != nil {
		return err
	}

	r.mu.Lock()
	previous := r.stored
	r.stored = value
	r.applied = append(r.applied, value)
	first := len(r.applied) == 1
	r.mu.Unlock()

	if first {
		// Applied, and the answer never gets back.
		stream.CancelWrite(0)

		return nil
	}

	return respond(stream, &MetaResponse{Value: previous})
}

func (r *lossySwapReplica) applies() [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([][]byte(nil), r.applied...)
}

func startLossySwapReplica(t *testing.T, r *lossySwapReplica) {
	t.Helper()
	tr := transport.NewPipeTransport("swap-retry-host", 6301)
	t.Cleanup(func() { tr.Close() })
	ln, err := tr.Listen()
	require.NoError(t, err)

	mux := rpc.NewMux()
	rpc.RegisterHandler(mux, OpMetaSwap, r.handleSwap)
	srv, err := rpc.NewServer(mux, []transport.Listener{ln}, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

func newSwapRetryClient(t *testing.T, cfg *config.Config) *Client {
	t.Helper()
	tr := transport.NewPipeTransport("swap-retry-client", 0)
	t.Cleanup(func() { tr.Close() })
	res, err := rpc.NewResolver(cfg, 2, tr)
	require.NoError(t, err)
	pool := rpc.NewConnPool(2, res)
	t.Cleanup(func() { pool.Close() })

	cli, err := NewClient(ClientConfig{
		Client:   rpc.NewClient(pool),
		Replicas: []config.NodeID{1},
		Timeout:  2 * time.Second,
	})
	require.NoError(t, err)

	return cli
}

// A swap is not idempotent, so an attempt whose response was lost must not be
// tried again. Re-applying it moves the placement record a second time and
// answers with a different displaced generation, and the writer then releases
// a generation that is live: every shard of an acknowledged object is dropped
// five minutes later.
//
// The sequence the gate reaches is: A's swap commits and its response is lost,
// B replaces A and releases A's generation, then A's retry republishes A and
// is told it displaced B. A releases B. The record names A, which B released,
// and nothing will bring it back.
//
// Verifies: ADR-0003:S7.
func TestS7_AnAmbiguousSwapIsNotRetried(t *testing.T) {
	cfg := swapRetryCfg()
	replica := &lossySwapReplica{}
	startLossySwapReplica(t, replica)
	client := newSwapRetryClient(t, cfg)

	previous, err := client.Swap(context.Background(), "placement-key", []byte("generation-A"))

	assert.Len(t, replica.applies(), 1,
		"the swap was applied %d times: a retried swap republishes a record the cluster has moved past",
		len(replica.applies()))
	require.Error(t, err, "a swap whose outcome is unknown has to be reported as unknown")
	assert.Empty(t, previous,
		"the writer was told which generation it displaced by an attempt it cannot trust, and will release it")
}

// A replica that answers at all may still have applied the command. Raft
// reports a leadership lost while committing after the entry is in the log, and
// the next leader can commit it, so that answer is as ambiguous as a lost
// response and must not be retried either.
//
// Verifies: ADR-0003:S7.
func TestS7_ASwapIsNotRetriedAfterALeadershipLoss(t *testing.T) {
	cfg := swapRetryCfg()
	replica := &applyThenFailReplica{fail: "raft apply failed: leadership lost while committing log"}
	startApplyThenFailReplica(t, replica)
	client := newSwapRetryClient(t, cfg)

	previous, err := client.Swap(context.Background(), "placement-key", []byte("generation-A"))

	assert.Equal(t, 1, replica.calls(),
		"the swap was applied %d times: a lost leadership is not proof the command was rejected",
		replica.calls())
	require.Error(t, err)
	assert.Empty(t, previous)
}

// applyThenFailReplica applies the swap and answers with an error envelope,
// which is how a raft apply that cannot be accounted for reaches the client.
type applyThenFailReplica struct {
	mu   sync.Mutex
	fail string
	seen int
}

func (r *applyThenFailReplica) handleSwap(_ context.Context, h MetaRequest, stream transport.Stream) error {
	if _, err := io.ReadAll(stream); err != nil {
		return err
	}

	r.mu.Lock()
	r.seen++
	r.mu.Unlock()

	return respond(stream, &MetaResponse{Err: r.fail})
}

func (r *applyThenFailReplica) calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.seen
}

func startApplyThenFailReplica(t *testing.T, r *applyThenFailReplica) {
	t.Helper()
	tr := transport.NewPipeTransport("swap-retry-host", 6301)
	t.Cleanup(func() { tr.Close() })
	ln, err := tr.Listen()
	require.NoError(t, err)

	mux := rpc.NewMux()
	rpc.RegisterHandler(mux, OpMetaSwap, r.handleSwap)
	srv, err := rpc.NewServer(mux, []transport.Listener{ln}, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

// The retry that is safe is the one a replica refuses outright: it names the
// leader, which proves nothing was applied, and following that redirect is how
// a write survives an election. Narrowing the ambiguous case must not narrow
// this one.
func TestASwapFollowsALeaderRedirect(t *testing.T) {
	cfg := swapRetryCfg()
	replica := &redirectingSwapReplica{leader: "swap-retry-host:6301"}
	startRedirectingSwapReplica(t, replica)
	client := newSwapRetryClient(t, cfg)

	previous, err := client.Swap(context.Background(), "placement-key", []byte("generation-A"))
	require.NoError(t, err, "a redirect followed to the same replica has to succeed on the second attempt")
	assert.Equal(t, []byte("generation-0"), previous)
	assert.Equal(t, 2, replica.calls(), "the redirect was not followed")
}

// redirectingSwapReplica refuses the first swap as a not-leader redirect and
// answers the second, which is an election settling under a write.
type redirectingSwapReplica struct {
	mu     sync.Mutex
	leader string
	seen   int
}

func (r *redirectingSwapReplica) handleSwap(_ context.Context, h MetaRequest, stream transport.Stream) error {
	if _, err := io.ReadAll(stream); err != nil {
		return err
	}

	r.mu.Lock()
	r.seen++
	first := r.seen == 1
	r.mu.Unlock()

	if first {
		return respond(stream, &MetaResponse{Err: ErrCodeNotLeader, Leader: r.leader})
	}

	return respond(stream, &MetaResponse{Value: []byte("generation-0")})
}

func (r *redirectingSwapReplica) calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.seen
}

func startRedirectingSwapReplica(t *testing.T, r *redirectingSwapReplica) {
	t.Helper()
	tr := transport.NewPipeTransport("swap-retry-host", 6301)
	t.Cleanup(func() { tr.Close() })
	ln, err := tr.Listen()
	require.NoError(t, err)

	mux := rpc.NewMux()
	rpc.RegisterHandler(mux, OpMetaSwap, r.handleSwap)
	srv, err := rpc.NewServer(mux, []transport.Listener{ln}, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}
