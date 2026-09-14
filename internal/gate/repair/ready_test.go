package repair

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeStatus answers Status from a table a test edits while readiness polls.
type fakeStatus struct {
	mu       sync.Mutex
	replicas map[config.NodeID]meta.MetaStatus
}

func (f *fakeStatus) set(id config.NodeID, leader bool, commit, applied uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.replicas == nil {
		f.replicas = make(map[config.NodeID]meta.MetaStatus)
	}
	f.replicas[id] = meta.MetaStatus{
		IsLeader:     leader,
		CommitIndex:  strconv.FormatUint(commit, 10),
		AppliedIndex: strconv.FormatUint(applied, 10),
	}
}

func (f *fakeStatus) Status(_ context.Context, id config.NodeID) (meta.MetaStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	st, ok := f.replicas[id]
	if !ok {
		return meta.MetaStatus{}, errors.New("replica unreachable")
	}

	return st, nil
}

// flipReady is a Readiness that becomes ready once released.
type flipReady struct {
	ready  atomic.Bool
	checks atomic.Int64
}

func (f *flipReady) Ready(context.Context) error {
	f.checks.Add(1)
	if f.ready.Load() {
		return nil
	}

	return errors.New("no meta leader known")
}

func TestReadinessNeedsALeader(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 2, 1, 4)
	st := &fakeStatus{}
	st.set(10, false, 100, 100)
	st.set(11, false, 100, 100)
	r := &ClusterReadiness{
		Meta: st, MetaReplicas: []config.NodeID{10, 11}, LocalMeta: []config.NodeID{10},
		Blob: c.blob, BlobNodes: c.nodes,
	}

	err := r.Ready(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no meta leader known")

	st.set(11, true, 100, 100)
	assert.NoError(t, r.Ready(t.Context()))
}

// TestReadinessWaitsForTheLocalReplicaToCatchUp is the startup race itself: a
// follower replaying its log reports a leader but serves records it has not
// applied yet.
func TestReadinessWaitsForTheLocalReplicaToCatchUp(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 2, 1, 4)
	st := &fakeStatus{}
	st.set(10, false, 90, 40)
	st.set(11, true, 500, 500)
	r := &ClusterReadiness{
		Meta: st, MetaReplicas: []config.NodeID{10, 11}, LocalMeta: []config.NodeID{10},
		Blob: c.blob, BlobNodes: c.nodes,
	}

	err := r.Ready(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "applied 40 of leader commit 500")

	// The leader moves on, but the goal does not: reaching the commit index
	// first sampled is enough, so a busy leader cannot hold repair forever.
	st.set(11, true, 900, 900)
	st.set(10, false, 600, 500)
	assert.NoError(t, r.Ready(t.Context()))
}

func TestReadinessNeedsTwoBlobNodes(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 2, 1, 4)
	st := &fakeStatus{}
	st.set(10, true, 5, 5)
	c.blob.stop(1, 2, 3)
	r := &ClusterReadiness{
		Meta: st, MetaReplicas: []config.NodeID{10}, LocalMeta: []config.NodeID{10},
		Blob: c.blob, BlobNodes: c.nodes,
	}

	err := r.Ready(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "1 of 4 blob nodes answered, need 2")
}

// A single-node development setup has one blob node, and must not wait for a
// second that will never exist.
func TestReadinessOnASingleBlobNode(t *testing.T) {
	t.Parallel()

	b := newFakeBlob()
	st := &fakeStatus{}
	st.set(1, true, 5, 5)
	r := &ClusterReadiness{
		Meta: st, MetaReplicas: []config.NodeID{1}, LocalMeta: []config.NodeID{1},
		Blob: b, BlobNodes: []config.NodeID{1},
	}

	assert.NoError(t, r.Ready(t.Context()))
}

// A blob node whose store fails is not one repair can use, even though it
// answered the rpc.
func TestReadinessDoesNotCountAStoreError(t *testing.T) {
	t.Parallel()

	st := &fakeStatus{}
	st.set(1, true, 5, 5)
	r := &ClusterReadiness{
		Meta: st, MetaReplicas: []config.NodeID{1},
		Blob: statErr{err: errors.New("stat: get extent: io error")}, BlobNodes: []config.NodeID{1, 2},
	}

	assert.Error(t, r.Ready(t.Context()))
}

type statErr struct{ err error }

func (s statErr) Stat(context.Context, config.NodeID, blob.StatRequest) (*blob.StatResponse, error) {
	return nil, s.err
}

// TestRunHoldsTheFirstPassUntilReady is the fix for the startup burst: the
// pass is held rather than skipped, and runs as soon as the cluster settles.
func TestRunHoldsTheFirstPassUntilReady(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 2, 1, 4)
	obj := c.store("held", 4096)
	victim := obj.place.AllNodes()[0]
	c.blob.forget(victim, obj.hash, 0)

	ready := &flipReady{}
	svc, err := New(Config{
		Nodes: []config.NodeID{victim}, Ring: c.ring, Meta: c.meta, Blob: c.blob,
		DataShards: c.k, ParityShards: c.m, Interval: time.Hour,
		Ready: ready, ReadyTimeout: time.Minute, ReadyPoll: 5 * time.Millisecond,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	require.Eventually(t, func() bool { return ready.checks.Load() >= 3 }, 5*time.Second, time.Millisecond)
	assert.Zero(t, svc.Stats().Passes, "no pass may run before the cluster is ready")
	assert.Zero(t, c.blob.stats.Load(), "and no shard may be asked about")

	ready.ready.Store(true)
	require.Eventually(t, func() bool { return svc.Stats().Passes == 1 }, 5*time.Second, time.Millisecond)
	_, ok := c.blob.held(victim, obj.hash, 0)
	assert.True(t, ok, "the held pass must run once released")
	assert.Zero(t, svc.Stats().Deferred)
}

// A cluster that never settles defers the pass rather than failing shards.
func TestRunDefersAPassThatNeverBecomesReady(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 2, 1, 4)
	obj := c.store("deferred", 4096)
	victim := obj.place.AllNodes()[0]
	c.blob.forget(victim, obj.hash, 0)

	svc, err := New(Config{
		Nodes: []config.NodeID{victim}, Ring: c.ring, Meta: c.meta, Blob: c.blob,
		DataShards: c.k, ParityShards: c.m, Interval: time.Hour,
		Ready: &flipReady{}, ReadyTimeout: 20 * time.Millisecond, ReadyPoll: 5 * time.Millisecond,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	require.Eventually(t, func() bool { return svc.Stats().Deferred == 1 }, 5*time.Second, time.Millisecond)
	stats := svc.Stats()
	assert.Zero(t, stats.Passes)
	assert.Zero(t, stats.Failed, "a deferred pass fails nothing")
	assert.Zero(t, c.blob.stats.Load())
}
