package meta

import (
	"context"
	"encoding/json"
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

// txnReplica counts every transaction it receives in full and answers through
// answer, which may also drop the reply.
type txnReplica struct {
	mu       sync.Mutex
	received []TxnRequest
	answer   func(stream transport.Stream) error
}

func (r *txnReplica) handleTxn(_ context.Context, _ MetaRequest, stream transport.Stream) error {
	body, err := io.ReadAll(stream)
	if err != nil {
		return err
	}
	var req TxnRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return err
	}
	r.mu.Lock()
	r.received = append(r.received, req)
	r.mu.Unlock()

	return r.answer(stream)
}

func (r *txnReplica) calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.received)
}

// startTxnReplica serves r as node 1 of swapRetryCfg.
func startTxnReplica(t *testing.T, r *txnReplica) {
	t.Helper()
	startTxnReplicaOn(t, r, 6301)
}

// startTxnReplicaOn serves r on port of the swapRetryCfg host.
func startTxnReplicaOn(t *testing.T, r *txnReplica, port int) {
	t.Helper()
	tr := transport.NewPipeTransport("swap-retry-host", port)
	t.Cleanup(func() { tr.Close() })
	ln, err := tr.Listen()
	require.NoError(t, err)

	mux := rpc.NewMux()
	rpc.RegisterHandler(mux, OpMetaTxn, r.handleTxn)
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

var (
	txnChecks = []TxnCheck{{Key: []byte("objects/k"), Absent: true}}
	txnWrites = []TxnWrite{{Key: []byte("objects/k"), Value: []byte("gen-1")}}
)

// A reply lost after the replica received the whole request may follow an
// applied transaction, so the client must answer unknown and send nothing
// again, not even to another reachable replica: a second apply could publish
// a record the cluster has moved past.
func TestClientTxn_ALostReplyIsUnknownAndNotResent(t *testing.T) {
	cfg := swapRetryCfg()
	cfg.Hosts[0].Nodes = append(cfg.Hosts[0].Nodes, config.Node{ID: 3, Role: config.RoleMeta, Port: 6303})
	lossy := &txnReplica{answer: func(stream transport.Stream) error {
		stream.CancelWrite(0)
		return nil
	}}
	startTxnReplicaOn(t, lossy, 6301)
	next := &txnReplica{answer: func(stream transport.Stream) error {
		return respond(stream, &MetaResponse{})
	}}
	startTxnReplicaOn(t, next, 6303)
	client := newTxnClient(t, cfg, 1, 3)

	outcome, current, err := client.Txn(context.Background(), txnChecks, txnWrites)

	assert.Equal(t, TxnUnknown, outcome)
	require.ErrorIs(t, err, ErrTxnUnknown)
	assert.Nil(t, current)
	assert.Equal(t, 1, lossy.calls(), "the transaction was sent %d times", lossy.calls())
	assert.Zero(t, next.calls(), "the client failed over after an ambiguous send")
}

// An error envelope can follow a proposal: raft reports a lost leadership
// after the entry is in the log. That answer is unknown, not refused.
func TestClientTxn_AnErrorAfterProposalIsUnknownAndNotResent(t *testing.T) {
	replica := &txnReplica{answer: func(stream transport.Stream) error {
		return respond(stream, &MetaResponse{Err: "raft apply failed: leadership lost while committing log"})
	}}
	startTxnReplica(t, replica)
	client := newSwapRetryClient(t, swapRetryCfg())

	outcome, _, err := client.Txn(context.Background(), txnChecks, txnWrites)

	assert.Equal(t, TxnUnknown, outcome)
	require.ErrorIs(t, err, ErrTxnUnknown)
	assert.NotErrorIs(t, err, ErrTxnRefused)
	assert.Equal(t, 1, replica.calls())
}

// A not-leader answer proves nothing was proposed. The client answers refused
// without following the redirect itself, and remembers the leader so the
// caller's next transaction goes there.
func TestClientTxn_ANotLeaderAnswerIsRefusedAndNotFollowed(t *testing.T) {
	replica := &txnReplica{answer: func(stream transport.Stream) error {
		return respond(stream, &MetaResponse{Err: ErrCodeNotLeader, Leader: RaftAddress(1)})
	}}
	startTxnReplica(t, replica)
	client := newSwapRetryClient(t, swapRetryCfg())

	outcome, _, err := client.Txn(context.Background(), txnChecks, txnWrites)

	assert.Equal(t, TxnRefused, outcome)
	require.ErrorIs(t, err, ErrTxnRefused)
	assert.ErrorIs(t, err, ErrNotLeader)
	assert.Equal(t, 1, replica.calls(), "the redirect was followed")
	client.mu.Lock()
	defer client.mu.Unlock()
	assert.Equal(t, config.NodeID(1), client.leader)
}

func TestClientTxn_AnInvalidAnswerIsRefused(t *testing.T) {
	replica := &txnReplica{answer: func(stream transport.Stream) error {
		return respond(stream, &MetaResponse{Err: ErrCodeInvalid})
	}}
	startTxnReplica(t, replica)
	client := newSwapRetryClient(t, swapRetryCfg())

	outcome, _, err := client.Txn(context.Background(), txnChecks, txnWrites)

	assert.Equal(t, TxnRefused, outcome)
	require.ErrorIs(t, err, ErrTxnRefused)
	assert.Equal(t, 1, replica.calls())
}

// The client refuses a malformed transaction itself, so it never reaches a
// replica.
func TestClientTxn_AMalformedTransactionIsRefusedBeforeSending(t *testing.T) {
	replica := &txnReplica{answer: func(stream transport.Stream) error {
		return respond(stream, &MetaResponse{})
	}}
	startTxnReplica(t, replica)
	client := newSwapRetryClient(t, swapRetryCfg())

	outcome, _, err := client.Txn(context.Background(), txnChecks, nil)

	assert.Equal(t, TxnRefused, outcome)
	require.ErrorIs(t, err, ErrInvalidTxn)
	assert.Zero(t, replica.calls())
}

// A replica whose stream cannot be opened received nothing, so the client may
// try the next one, and it sends the request exactly once in total.
func TestClientTxn_AnUndeliveredAttemptMovesToTheNextReplica(t *testing.T) {
	cfg := swapRetryCfg()
	cfg.Hosts[0].Nodes = append(cfg.Hosts[0].Nodes, config.Node{ID: 3, Role: config.RoleMeta, Port: 6303})
	replica := &txnReplica{answer: func(stream transport.Stream) error {
		return respond(stream, &MetaResponse{Mismatch: true, Current: []TxnValue{
			{Key: []byte("objects/k"), Value: []byte("gen-0"), Exists: true},
		}})
	}}
	startTxnReplica(t, replica)
	client := newTxnClient(t, cfg, 3, 1)

	outcome, current, err := client.Txn(context.Background(), txnChecks, txnWrites)

	require.NoError(t, err)
	assert.Equal(t, TxnMismatch, outcome)
	assert.Equal(t, []TxnValue{{Key: []byte("objects/k"), Value: []byte("gen-0"), Exists: true}}, current)
	assert.Equal(t, 1, replica.calls())
}

// With no replica reachable, nothing left the client, so the answer is a
// definite refusal rather than unknown.
func TestClientTxn_NoReachableReplicaIsRefused(t *testing.T) {
	client := newSwapRetryClient(t, swapRetryCfg())

	outcome, _, err := client.Txn(context.Background(), txnChecks, txnWrites)

	assert.Equal(t, TxnRefused, outcome)
	require.ErrorIs(t, err, ErrTxnRefused)
	assert.ErrorIs(t, err, ErrUndelivered)
}

// newTxnClient builds a client, as node 2 of cfg, that tries replicas in order.
func newTxnClient(t *testing.T, cfg *config.Config, replicas ...config.NodeID) *Client {
	t.Helper()
	tr := transport.NewPipeTransport("swap-retry-client", 0)
	t.Cleanup(func() { tr.Close() })
	res, err := rpc.NewResolver(cfg, 2, tr)
	require.NoError(t, err)
	pool := rpc.NewConnPool(2, res)
	t.Cleanup(func() { pool.Close() })
	client, err := NewClient(ClientConfig{
		Client:   rpc.NewClient(pool),
		Replicas: replicas,
		Timeout:  2 * time.Second,
	})
	require.NoError(t, err)
	return client
}
