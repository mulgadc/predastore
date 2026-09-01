package reaper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"testing"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tombstoneRow is one tombstone as a test builds and stores it.
type tombstoneRow struct {
	bucket, key string
	hash        [32]byte
	epoch       uint64
	data, m     []config.NodeID
}

// tombstone builds a tombstone naming the given data and parity nodes, keyed
// by a hash unique to the name — deterministic, the way model.ObjectHash is,
// so the recreate case can reuse it.
func tombstone(name string, epoch uint64, data, parity []config.NodeID) tombstoneRow {
	return tombstoneRow{
		bucket: "b", key: name,
		hash: sha256.Sum256([]byte(name)), epoch: epoch,
		data: data, m: parity,
	}
}

// store writes a tombstone row to the cluster's meta store, gob-encoded the
// way deleteStoredObject leaves one, and holds the shards it names on their
// nodes, as a completed write left them.
func (c *cluster) store(t *testing.T, row tombstoneRow) string {
	t.Helper()

	stored := c.storeTombstoneOnly(t, row)

	for i, n := range row.data {
		c.blob.hold(n, row.hash, uint32(i))
	}
	for i, n := range row.m {
		c.blob.hold(n, row.hash, uint32(len(row.data)+i))
	}

	return stored
}

// storeTombstoneOnly writes the tombstone row without holding any shard on
// any node, as if the node had already reclaimed them.
func (c *cluster) storeTombstoneOnly(t *testing.T, row tombstoneRow) string {
	t.Helper()

	info := handlers.DeletedObjectInfo{
		Bucket: row.bucket, Key: row.key, ObjectHash: row.hash,
		WriteEpoch: row.epoch, DataShardNodes: row.data, ParityNodes: row.m,
	}
	var buf bytes.Buffer
	require.NoError(t, gob.NewEncoder(&buf).Encode(info))

	stored := handlers.TableKey(model.TableObjects,
		handlers.DeletedObjectPrefix+row.bucket+"/"+row.key)
	c.meta.put(stored, buf.Bytes())

	return stored
}

// recordLivePlacement writes a placement record at hash, at the given epoch.
// A recreate lands its new write on the same nodes and indices a delete
// tombstoned — object hashes are deterministic on bucket and key — but always
// mints a new WriteEpoch, which is the only thing that tells it apart from
// the record deleteStoredObject has not removed yet for this very tombstone.
func (c *cluster) recordLivePlacement(t *testing.T, hash [32]byte, epoch uint64, data, parity []config.NodeID) {
	t.Helper()
	place := handlers.ObjectToShardNodes{
		Size: 1, WriteEpoch: epoch, DataShardNodes: data, ParityShardNodes: parity,
	}
	raw, err := handlers.EncodePlacement(place)
	require.NoError(t, err)
	c.meta.put(handlers.TableKey(model.TableObjects, string(hash[:])), raw)
}

func nodes(ids ...int) []config.NodeID {
	out := make([]config.NodeID, len(ids))
	for i, id := range ids {
		out[i] = config.NodeID(id)
	}
	return out
}

// A pass must reclaim exactly the shards a tombstone names, at the indices
// the write path assigned them — data shards first, then parity — and then
// remove the tombstone itself.
func TestPassReclaimsExactlyTheNamedShardsThenDropsTheTombstone(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	row := tombstone("object", 5, nodes(1, 2), nodes(3))
	stored := c.store(t, row)

	svc := c.service(8)
	require.NoError(t, svc.Pass(t.Context()))

	assert.False(t, c.blob.has(1, row.hash, 0), "data shard 0 must be reclaimed")
	assert.False(t, c.blob.has(2, row.hash, 1), "data shard 1 must be reclaimed")
	assert.False(t, c.blob.has(3, row.hash, 2), "parity shard must be reclaimed at index 2")
	assert.False(t, c.meta.has(stored), "the tombstone must be dropped once every shard is gone")

	stats := svc.Stats()
	assert.Equal(t, int64(1), stats.Scanned)
	assert.Equal(t, int64(1), stats.Reclaimed)
	assert.Zero(t, stats.Failed)
	assert.Zero(t, stats.Pending)
}

// The recreate hazard: object hashes are deterministic, so a key deleted and
// then recreated lands its new write on the same nodes and indices the old
// tombstone names, but a recreate always mints a fresh WriteEpoch — that is
// the discriminator this test exercises, not mere existence of a placement
// record. The sweep must not delete a single shard in that case — doing so
// would delete the recreated object's data — and must still drop the
// now-superseded tombstone.
func TestRecreatedKeyDeletesNoShards(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	row := tombstone("recreated", 5, nodes(1, 2), nodes(3))
	stored := c.store(t, row)
	c.recordLivePlacement(t, row.hash, row.epoch+1, row.data, row.m)

	svc := c.service(8)
	require.NoError(t, svc.Pass(t.Context()))

	assert.Empty(t, c.blob.deletes, "no shard delete may reach the blob client for a recreated key")
	assert.True(t, c.blob.has(1, row.hash, 0), "the recreated shard must survive untouched")
	assert.False(t, c.meta.has(stored), "the superseded tombstone must still be dropped")
	assert.Equal(t, int64(1), svc.Stats().Reclaimed)
}

// The regression case: deleteStoredObject writes the tombstone before it
// removes the placement record, so a pass can land in that window and read
// the tombstone's own object still in the index. A plain existence check
// would mistake that for a recreate and drop the tombstone without reclaiming
// a single shard — leaking them permanently, on every delete. The epoch
// match is what tells the two apart: the tombstone must survive untouched
// while the placement is still there at the same epoch, and only once the
// placement is gone does a later pass reclaim it.
func TestSameEpochPlacementIsTheDeleteStillInFlightNotARecreate(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	row := tombstone("in-flight", 5, nodes(1, 2), nodes(3))
	stored := c.store(t, row)
	c.recordLivePlacement(t, row.hash, row.epoch, row.data, row.m)

	svc := c.service(8)
	require.NoError(t, svc.Pass(t.Context()))

	assert.Empty(t, c.blob.deletes, "no shard delete may reach the blob client while the delete is still in flight")
	assert.True(t, c.meta.has(stored), "the tombstone must survive while its own placement record is still there")
	stats := svc.Stats()
	assert.Zero(t, stats.Reclaimed)
	assert.Zero(t, stats.Failed, "a delete still in flight is not a failure")
	assert.Equal(t, int64(1), stats.Pending)

	// deleteStoredObject completes: the placement record is gone. The next
	// pass must now reclaim the shards and drop the tombstone normally.
	c.meta.mu.Lock()
	delete(c.meta.rows, handlers.TableKey(model.TableObjects, string(row.hash[:])))
	c.meta.mu.Unlock()

	require.NoError(t, svc.Pass(t.Context()))

	assert.False(t, c.blob.has(1, row.hash, 0))
	assert.False(t, c.blob.has(2, row.hash, 1))
	assert.False(t, c.blob.has(3, row.hash, 2))
	assert.False(t, c.meta.has(stored), "the tombstone must be reclaimed once the placement record is gone")
	assert.Equal(t, int64(1), svc.Stats().Reclaimed)
	assert.Zero(t, svc.Stats().Pending)
}

// A tombstone with WriteEpoch == 0 predates the field: its delete already
// fanned the shards out inline, so the sweep must drop it without touching a
// shard or even consulting the placement table.
func TestLegacyTombstoneIsDroppedWithoutDeletingShards(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	row := tombstone("legacy", 0, nodes(1, 2), nodes(3))
	stored := c.store(t, row)

	svc := c.service(8)
	require.NoError(t, svc.Pass(t.Context()))

	assert.Empty(t, c.blob.deletes, "a legacy tombstone's shards were already fanned out inline")
	assert.True(t, c.blob.has(1, row.hash, 0), "shards a legacy tombstone names must be left untouched")
	assert.False(t, c.meta.has(stored), "the legacy tombstone must still be dropped")
	assert.Equal(t, int64(1), svc.Stats().Reclaimed)
	assert.Zero(t, svc.Stats().Failed)
}

// The whole point of the design: a tombstone whose node is down at delete
// time survives the pass rather than leaking the shard, and a later pass
// reclaims it once the node returns.
func TestUnreachableNodeKeepsTheTombstoneUntilItRecovers(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	row := tombstone("stuck", 5, nodes(1, 2), nodes(3))
	stored := c.store(t, row)
	c.blob.stop(2)

	svc := c.service(8)
	require.NoError(t, svc.Pass(t.Context()))

	assert.True(t, c.meta.has(stored), "a tombstone with an unreachable node must survive the pass")
	assert.False(t, c.blob.has(1, row.hash, 0), "reachable shards are reclaimed even when one node is down")
	stats := svc.Stats()
	assert.Equal(t, int64(1), stats.Failed)
	assert.Equal(t, int64(1), stats.Pending)

	c.blob.resume(2)
	require.NoError(t, svc.Pass(t.Context()))

	assert.False(t, c.meta.has(stored), "the tombstone must be reclaimed once the node returns")
	assert.False(t, c.blob.has(2, row.hash, 1))
	assert.Equal(t, int64(1), svc.Stats().Reclaimed)
	assert.Zero(t, svc.Stats().Pending)
}

// DeleteResponse{Deleted: false} means the node never held the shard, which is
// success — a previous pass or the node itself already reclaimed it — and
// must not be treated as a failure that keeps the tombstone around.
func TestAnAlreadyGoneShardIsNotAFailure(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	// No c.blob.hold call: the node has nothing at this address, so Delete
	// answers Deleted: false without an error.
	row := tombstone("already-gone", 5, nodes(1), nil)
	stored := c.storeTombstoneOnly(t, row)

	svc := c.service(8)
	require.NoError(t, svc.Pass(t.Context()))

	assert.False(t, c.meta.has(stored))
	assert.Equal(t, int64(1), svc.Stats().Reclaimed)
	assert.Zero(t, svc.Stats().Failed)
}

// The sweep pages only the tombstone prefix, so listing rows, part keys and
// placement records sharing the objects table must never surface as a task.
func TestSweepIgnoresOtherTableRows(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	row := tombstone("real", 5, nodes(1), nodes(2))
	c.store(t, row)

	otherHash := sha256.Sum256([]byte("some-other-object"))
	for _, key := range []string{
		"arn:aws:s3:::bucket/an-object-key-here",
		string(otherHash[:]),
		"part:0198f2a1-6c3e-7c19-9b1e-4f2d5a6b7c8d:00001",
	} {
		c.meta.put(handlers.TableKey(model.TableObjects, key), []byte("not a tombstone"))
	}

	svc := c.service(8)
	require.NoError(t, svc.Pass(t.Context()))

	assert.Equal(t, int64(1), svc.Stats().Scanned, "only the tombstone row is a reclaim candidate")
}

// TestPagesThroughMoreTombstonesThanOnePage exercises the cursor the same way
// repair's does: the tombstone table does not fit one response on a cluster
// deleting at any volume, and a scan that silently stopped at the first page
// would reclaim only the alphabetical head of the keyspace.
func TestPagesThroughMoreTombstonesThanOnePage(t *testing.T) {
	t.Parallel()

	const count = 37
	c := newCluster(t)

	stored := make([]string, 0, count)
	for i := range count {
		row := tombstone(fmt.Sprintf("object-%03d", i), 1, nodes(1), nil)
		stored = append(stored, c.store(t, row))
	}

	svc, err := New(Config{Meta: c.meta, Blob: c.blob, Workers: 3, PageSize: 4})
	require.NoError(t, err)
	require.NoError(t, svc.Pass(t.Context()))

	assert.Equal(t, int64(count), svc.Stats().Scanned, "every page must be walked")
	assert.Equal(t, int64(count), svc.Stats().Reclaimed)
	for _, key := range stored {
		assert.False(t, c.meta.has(key))
	}
}

func TestRunStopsWithItsContext(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	c.store(t, tombstone("object", 1, nodes(1), nil))
	svc := c.service(8)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()

	cancel()
	require.NoError(t, <-done, "a cancelled sweep is a clean stop, not a failure")
}

func TestNewRejectsAServiceThatCouldNotSweep(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	base := Config{Meta: c.meta, Blob: c.blob}

	t.Run("no clients", func(t *testing.T) {
		t.Parallel()
		cfg := base
		cfg.Meta = nil
		_, err := New(cfg)
		assert.Error(t, err)
	})

	t.Run("defaults fill in", func(t *testing.T) {
		t.Parallel()
		svc, err := New(base)
		require.NoError(t, err)
		assert.Positive(t, svc.workers)
		assert.Equal(t, defaultPageSize, svc.pageSize)
		assert.Equal(t, defaultInterval, svc.interval)
	})
}
