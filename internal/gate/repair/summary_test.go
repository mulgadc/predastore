package repair

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPassSummaryCountsWhatThePassDid is the line an operator reads to judge a
// pass: what it looked at, what it found owed and how each owed shard ended.
func TestPassSummaryCountsWhatThePassDid(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 3, 2, 6)
	prepared := c.store("prepared", 30_000)
	pnode := prepared.place.AllNodes()[4]
	c.blob.forget(pnode, prepared.hash, 4)
	c.blob.prepare(pnode, prepared.hash, 4, prepared.place.WriteEpoch, prepared.shards[4])

	lost := c.store("lost", 30_000)
	c.blob.forget(lost.place.AllNodes()[1], lost.hash, 1)

	c.store("healthy", 30_000)

	svc := c.service(c.nodes...)
	require.NoError(t, svc.Pass(t.Context()))

	sum := svc.Stats().LastPass
	require.NotNil(t, sum)
	assert.Equal(t, OutcomeComplete, sum.Outcome)
	assert.Equal(t, int64(3), sum.Scanned)
	assert.Equal(t, int64(15), sum.Owned)
	assert.Equal(t, int64(2), sum.Owed)
	assert.Equal(t, int64(1), sum.RepairedCommit)
	assert.Equal(t, int64(1), sum.RepairedRebuild)
	assert.Zero(t, sum.RepairedStandby)
	assert.Zero(t, sum.FailedPeerUnreachable+sum.FailedPeerOtherEpoch+sum.FailedPeerMissing+sum.FailedOther)
	assert.Zero(t, sum.Remaining)
	assert.Equal(t, int64(1), sum.OwedStreak)
	assert.False(t, sum.Finished.IsZero())

	// The second pass counts only its own work, not the running totals.
	require.NoError(t, svc.Pass(t.Context()))
	sum = svc.Stats().LastPass
	assert.Equal(t, int64(3), sum.Scanned)
	assert.Zero(t, sum.Owed)
	assert.Zero(t, sum.RepairedCommit+sum.RepairedRebuild)
	assert.Zero(t, sum.OwedStreak, "a pass that found nothing owed ends the streak")
	assert.Equal(t, int64(2), svc.Stats().Repaired, "the running totals keep counting")
}

// TestOwedStreakCountsConsecutivePassesThatFoundWork is what the warning keys
// on: a shard that stays owed pass after pass is the case worth a person.
func TestOwedStreakCountsConsecutivePassesThatFoundWork(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 2, 1, 4)
	obj := c.store("stuck", 4096)
	nodes := obj.place.AllNodes()
	c.blob.forget(nodes[0], obj.hash, 0)
	c.blob.forget(nodes[1], obj.hash, 1)
	svc := c.service(nodes[0])

	require.NoError(t, svc.Pass(t.Context()))
	sum := svc.Stats().LastPass
	assert.Equal(t, int64(1), sum.OwedStreak)
	assert.Equal(t, int64(1), sum.FailedPeerMissing)
	assert.Equal(t, int64(1), sum.Remaining)

	require.NoError(t, svc.Pass(t.Context()))
	assert.Equal(t, int64(2), svc.Stats().LastPass.OwedStreak)

	// The missing peer comes back, so the next pass rebuilds, and the one
	// after finds nothing.
	c.blob.hold(nodes[1], obj.hash, 1, obj.place.WriteEpoch, obj.shards[1])
	require.NoError(t, svc.Pass(t.Context()))
	sum = svc.Stats().LastPass
	assert.Equal(t, int64(1), sum.RepairedRebuild)
	assert.Zero(t, sum.Remaining)

	require.NoError(t, svc.Pass(t.Context()))
	assert.Zero(t, svc.Stats().LastPass.OwedStreak)
}

func TestPassSummaryReportsADeferral(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 2, 1, 4)
	obj := c.store("unreachable", 4096)
	nodes := obj.place.AllNodes()
	c.blob.forget(nodes[0], obj.hash, 0)
	c.blob.stop(nodes[1], nodes[2])

	svc := c.service(nodes[0])
	require.ErrorIs(t, svc.Pass(t.Context()), ErrPassDeferred)

	sum := svc.Stats().LastPass
	require.NotNil(t, sum)
	assert.Equal(t, OutcomeDeferred, sum.Outcome)
	assert.NotEmpty(t, sum.Reason)
	assert.Equal(t, int64(1), sum.Owed)
	assert.Equal(t, int64(1), sum.Remaining)
}

func TestNotReadyIsRecordedAsTheLastPass(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 2, 1, 4)
	svc, err := New(Config{
		Nodes: c.nodes, Ring: c.ring, Meta: c.meta, Blob: c.blob,
		DataShards: c.k, ParityShards: c.m, Interval: time.Hour,
		Ready: &flipReady{}, ReadyTimeout: 10 * time.Millisecond, ReadyPoll: time.Millisecond,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	require.Eventually(t, func() bool {
		sum := svc.Stats().LastPass
		return sum != nil && sum.Outcome == OutcomeNotReady
	}, 5*time.Second, time.Millisecond)
	assert.Contains(t, svc.Stats().LastPass.Reason, "no meta leader known")
}
