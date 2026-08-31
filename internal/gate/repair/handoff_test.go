package repair

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// handOff moves a stored shard from its owner to the node one step off the end
// of the stripe, which is exactly where a write whose owner refused puts it.
func (c *cluster) handOff(o *object, index int) (owner, holder config.NodeID) {
	c.t.Helper()

	nodes, err := c.ring.Nodes(o.hash, c.k+c.m+1)
	require.NoError(c.t, err)
	require.Greater(c.t, len(nodes), c.k+c.m, "the ring needs a node to spare")

	owner, holder = o.place.AllNodes()[index], nodes[c.k+c.m]
	require.NotEqual(c.t, owner, holder)

	c.blob.forget(owner, o.hash, index)
	c.blob.hold(holder, o.hash, index, o.place.WriteEpoch, o.shards[index])

	return owner, holder
}

// The cheap repair: the shard already exists at the right generation, one step
// along the ring, so returning it is a single transfer rather than fetching k
// peers and decoding.
func TestRepairReturnsAHandedOffShardToItsOwner(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 2, 1, 4)
	obj := c.store("handed-off", 64<<10)
	owner, holder := c.handOff(obj, 1)

	require.NoError(t, c.service(owner).Pass(context.Background()))

	held, ok := c.blob.held(owner, obj.hash, 1)
	require.True(t, ok, "the owner has to end the pass holding its own shard")
	assert.Equal(t, obj.place.WriteEpoch, held.epoch)
	assert.Equal(t, obj.shards[1], held.body, "byte for byte what the write encoded")

	_, still := c.blob.held(holder, obj.hash, 1)
	assert.False(t, still, "the holder's copy is released once the owner has it")
}

// Returning it must not cost a reconstruction. One Get against the holder is
// the whole transfer; k Gets against the peers means the pull was skipped and
// the shard was rebuilt instead.
func TestReturningAHandedOffShardDoesNotReconstruct(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 3, 2, 6)
	obj := c.store("handed-off", 96<<10)
	owner, _ := c.handOff(obj, 4)

	before := c.blob.gets.Load()
	require.NoError(t, c.service(owner).Pass(context.Background()))

	assert.Equal(t, int64(1), c.blob.gets.Load()-before,
		"a shard that exists somewhere must be fetched, not recomputed")
}

// refusingPuts is a cluster whose named node answers everything except a write,
// which is the state that makes the release order observable: the owner is
// reachable enough to be inspected and cannot be given the shard.
type refusingPuts struct {
	BlobClient

	node config.NodeID
}

func (r refusingPuts) Put(
	ctx context.Context, node config.NodeID, req blob.PutRequest, body io.Reader,
) (*blob.PutResponse, error) {
	if node == r.node {
		return nil, errors.New("disk full")
	}

	return r.BlobClient.Put(ctx, node, req, body)
}

// The holder is emptied only after the owner holds the shard. Releasing it
// first would turn a failed return into the loss handoff existed to prevent.
func TestAFailedReturnLeavesTheHolderHoldingIt(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 2, 1, 4)
	obj := c.store("handed-off", 32<<10)
	owner, holder := c.handOff(obj, 0)

	svc, err := New(Config{
		Nodes: []config.NodeID{owner}, Ring: c.ring, Meta: c.meta,
		Blob:       refusingPuts{BlobClient: c.blob, node: owner},
		DataShards: c.k, ParityShards: c.m, Workers: 4, PageSize: 8,
	})
	require.NoError(t, err)
	require.NoError(t, svc.Pass(context.Background()))

	held, ok := c.blob.held(holder, obj.hash, 0)
	require.True(t, ok, "the only copy of this shard must survive a failed return")
	assert.Equal(t, obj.place.WriteEpoch, held.epoch)
	assert.Zero(t, c.blob.deletes.Load())
	assert.Equal(t, int64(1), svc.Stats().Failed, "a repair that placed nothing is a failure")
}

// With nothing on the holder the pull is one wasted Stat-sized round trip and
// the rebuild proceeds, which is the ordinary case: most repairs are not
// returning a handoff.
func TestRepairRebuildsWhenTheHolderHasNothing(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 2, 1, 4)
	obj := c.store("lost", 64<<10)
	owner := obj.place.AllNodes()[1]
	c.blob.forget(owner, obj.hash, 1)

	require.NoError(t, c.service(owner).Pass(context.Background()))

	held, ok := c.blob.held(owner, obj.hash, 1)
	require.True(t, ok)
	assert.Equal(t, obj.shards[1], held.body)
	assert.Zero(t, c.blob.deletes.Load(), "there was no holder's copy to release")
}

// A cluster with no node to spare has no handoff position to ask about, and
// the repair has to fall straight through to the rebuild rather than deriving
// a holder that is one of the object's own shard nodes.
func TestNoSpareNodeMeansNoHandoffToPull(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 2, 1, 3)
	obj := c.store("lost", 64<<10)
	owner := obj.place.AllNodes()[2]
	c.blob.forget(owner, obj.hash, 2)

	svc := c.service(owner)
	require.Zero(t, svc.handoffNode(obj.hash))
	require.NoError(t, svc.Pass(context.Background()))

	held, ok := c.blob.held(owner, obj.hash, 2)
	require.True(t, ok)
	assert.Equal(t, obj.shards[2], held.body)
}

// A holder still carrying an older generation must not be pulled from: the
// shard it holds belongs to an object that has since been overwritten, and
// returning it would put a stale shard back under a current record.
func TestAStaleShardOnTheHolderIsNotPulled(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 2, 1, 4)
	obj := c.store("handed-off", 64<<10)
	owner, holder := c.handOff(obj, 1)
	c.blob.forget(holder, obj.hash, 1)
	c.blob.hold(holder, obj.hash, 1, obj.place.WriteEpoch-1, []byte("an older generation"))

	require.NoError(t, c.service(owner).Pass(context.Background()))

	held, ok := c.blob.held(owner, obj.hash, 1)
	require.True(t, ok, "the shard is rebuilt from the peers instead")
	assert.Equal(t, obj.shards[1], held.body)
	assert.Equal(t, obj.place.WriteEpoch, held.epoch)
}
