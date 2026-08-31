package meta

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testClient builds a Client with no rpc client, which is enough for the
// ordering policy: readOrder and observeReplica never reach the wire.
func testClient(t *testing.T, now func() time.Time) *Client {
	t.Helper()

	return &Client{
		replicas:     []config.NodeID{1, 2, 3},
		timeout:      time.Second,
		maxRetries:   3,
		now:          now,
		unresponsive: make(map[config.NodeID]time.Time),
	}
}

var errReplicaSilent = errors.New("replica said nothing")

func TestTheCachedLeaderIsTriedFirstWhileItAnswers(t *testing.T) {
	t.Parallel()
	c := testClient(t, time.Now)
	c.cacheLeader(3)

	assert.Equal(t, []config.NodeID{3, 1, 2}, c.readOrder())
}

// TestAReplicaThatDidNotAnswerIsTriedLast is the defect itself: without this
// the cached leader keeps its precedence and every operation pays its deadline.
func TestAReplicaThatDidNotAnswerIsTriedLast(t *testing.T) {
	t.Parallel()
	c := testClient(t, time.Now)
	c.cacheLeader(3)
	require.Equal(t, []config.NodeID{3, 1, 2}, c.readOrder())

	c.observeReplica(context.Background(), 3, errReplicaSilent)

	assert.Equal(t, []config.NodeID{1, 2, 3}, c.readOrder())
}

func TestAnAnsweringReplicaRegainsItsPlace(t *testing.T) {
	t.Parallel()
	c := testClient(t, time.Now)
	c.cacheLeader(3)
	c.observeReplica(context.Background(), 3, errReplicaSilent)
	require.Equal(t, []config.NodeID{1, 2, 3}, c.readOrder())

	c.observeReplica(context.Background(), 3, nil)

	assert.Equal(t, []config.NodeID{3, 1, 2}, c.readOrder())
}

func TestTheCooldownExpires(t *testing.T) {
	t.Parallel()
	now := time.Now()
	c := testClient(t, func() time.Time { return now })
	c.cacheLeader(3)
	c.observeReplica(context.Background(), 3, errReplicaSilent)
	require.Equal(t, []config.NodeID{1, 2, 3}, c.readOrder())

	now = now.Add(replicaCooldown - time.Second)
	assert.Equal(t, []config.NodeID{1, 2, 3}, c.readOrder(), "demoted before the cooldown ran out")

	now = now.Add(2 * time.Second)
	assert.Equal(t, []config.NodeID{3, 1, 2}, c.readOrder(), "still demoted after the cooldown ran out")
	assert.Empty(t, c.unresponsive, "an expired mark was not forgotten")
}

// TestEveryReplicaSilentStillOffersEveryReplica holds the property that makes a
// wrong mark harmless: this orders replicas, it never removes one.
func TestEveryReplicaSilentStillOffersEveryReplica(t *testing.T) {
	t.Parallel()
	c := testClient(t, time.Now)
	for _, id := range c.replicas {
		c.observeReplica(context.Background(), id, errReplicaSilent)
	}

	assert.ElementsMatch(t, []config.NodeID{1, 2, 3}, c.readOrder())
}

// TestAGiveUpByTheCallerBlamesNobody guards the false signal: a client that
// goes away fails every attempt in flight, and marking on that would demote the
// whole replica set for one abandoned request.
func TestAGiveUpByTheCallerBlamesNobody(t *testing.T) {
	t.Parallel()
	c := testClient(t, time.Now)
	c.cacheLeader(3)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.observeReplica(ctx, 3, context.Canceled)

	assert.Equal(t, []config.NodeID{3, 1, 2}, c.readOrder())
	assert.Empty(t, c.unresponsive)
}

// TestASuccessCountsEvenAfterTheCallerGaveUp keeps the guard one-sided. A
// replica that answered, answered, whatever the caller did next.
func TestASuccessCountsEvenAfterTheCallerGaveUp(t *testing.T) {
	t.Parallel()
	c := testClient(t, time.Now)
	c.cacheLeader(3)
	c.observeReplica(context.Background(), 3, errReplicaSilent)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.observeReplica(ctx, 3, nil)

	assert.Equal(t, []config.NodeID{3, 1, 2}, c.readOrder())
}
