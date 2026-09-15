package repair

import (
	"fmt"
	"testing"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rebuildOnce runs one owed shard through the rebuild and returns its error, so
// a test can read the message an operator would see.
func rebuildOnce(t *testing.T, svc *Service, obj *object, index int) error {
	t.Helper()

	return svc.rebuildShard(t.Context(), task{
		hash: obj.hash, index: index, node: obj.place.AllNodes()[index], place: obj.place,
	})
}

func TestAFailureNamesAnUnreachablePeer(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 2, 1, 4)
	obj := c.store("unreachable-peer", 4096)
	nodes := obj.place.AllNodes()
	c.blob.forget(nodes[0], obj.hash, 0)
	c.blob.stop(nodes[1])

	svc := c.service(nodes[0])
	require.NoError(t, svc.Pass(t.Context()), "one peer still answered, so the pass is not deferred")

	stats := svc.Stats()
	assert.Equal(t, int64(1), stats.Failed)
	assert.Equal(t, int64(1), stats.FailedPeerUnreachable)
	assert.Zero(t, stats.FailedPeerOtherEpoch+stats.FailedPeerMissing+stats.FailedOther)
	assert.Zero(t, stats.Deferred)

	err := rebuildOnce(t, svc, obj, 0)
	require.ErrorIs(t, err, errTooFewPeers)
	assert.Contains(t, err.Error(), "peer_unreachable: 1 of 2 peers hold epoch")
	assert.Contains(t, err.Error(), "(1 unreachable, 0 at another epoch, 0 missing the shard)")
}

func TestAFailureNamesAPeerAtAnotherEpoch(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 2, 1, 4)
	obj := c.store("other-epoch-peer", 4096)
	nodes := obj.place.AllNodes()
	c.blob.forget(nodes[0], obj.hash, 0)
	c.blob.hold(nodes[1], obj.hash, 1, obj.place.WriteEpoch+9, obj.shards[1])

	svc := c.service(nodes[0])
	require.NoError(t, svc.Pass(t.Context()))

	stats := svc.Stats()
	assert.Equal(t, int64(1), stats.FailedPeerOtherEpoch)
	assert.Zero(t, stats.FailedPeerUnreachable+stats.FailedPeerMissing+stats.FailedOther)

	err := rebuildOnce(t, svc, obj, 0)
	assert.Contains(t, err.Error(), "(0 unreachable, 1 at another epoch, 0 missing the shard)")
}

func TestAFailureNamesAPeerGenuinelyMissingTheShard(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 2, 1, 4)
	obj := c.store("missing-peer", 4096)
	nodes := obj.place.AllNodes()
	c.blob.forget(nodes[0], obj.hash, 0)
	c.blob.forget(nodes[1], obj.hash, 1)

	svc := c.service(nodes[0])
	require.NoError(t, svc.Pass(t.Context()))

	stats := svc.Stats()
	assert.Equal(t, int64(1), stats.FailedPeerMissing)
	assert.Zero(t, stats.FailedPeerUnreachable+stats.FailedPeerOtherEpoch+stats.FailedOther)

	err := rebuildOnce(t, svc, obj, 0)
	assert.Contains(t, err.Error(), "peer_missing: 1 of 2 peers hold epoch")
}

// TestAPassWithNoPeerReachableIsDeferredNotFailed is the second half of the
// startup burst: a node that can reach none of its peers must not file every
// shard it owes as a failure.
func TestAPassWithNoPeerReachableIsDeferredNotFailed(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 2, 1, 4)
	victim := config.NodeID(1)
	owed := 0
	for i := range 12 {
		obj := c.store(fmt.Sprintf("object-%02d", i), 4096)
		for index, node := range obj.place.AllNodes() {
			if node == victim {
				c.blob.forget(node, obj.hash, index)
				owed++
			}
		}
	}
	require.Positive(t, owed)
	c.blob.stop(2, 3, 4)

	svc := c.service(victim)
	require.ErrorIs(t, svc.Pass(t.Context()), ErrPassDeferred)

	stats := svc.Stats()
	assert.Zero(t, stats.Failed, "a pass that reached no peer has not shown any shard lost")
	assert.Equal(t, int64(1), stats.Deferred)
	assert.Positive(t, stats.Pending, "the shards are still owed")
	assert.Zero(t, c.blob.puts.Load(), "nothing can be written from peers that never answered")
}
