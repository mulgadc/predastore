package repair

import (
	"context"
	"fmt"
	"testing"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRebuildsEveryShardIndexByteForByte is the property the whole package
// rests on: whatever position was lost, what repair writes back is exactly what
// the write path stored there. Sizes are deliberately not multiples of k, so
// the last data shard carries the padding the split introduced and a rebuild
// that got the shard size wrong cannot agree by coincidence.
func TestRebuildsEveryShardIndexByteForByte(t *testing.T) {
	t.Parallel()

	codes := []struct{ k, m, nodes int }{{2, 1, 4}, {3, 2, 6}, {7, 3, 12}}
	sizes := []int{1, 1000, 65537}

	for _, code := range codes {
		for _, size := range sizes {
			name := fmt.Sprintf("RS(%d,%d)/%dB", code.k, code.m, size)
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				for lost := range code.k + code.m {
					c := newCluster(t, code.k, code.m, code.nodes)
					obj := c.store("object", size)
					victim := obj.place.AllNodes()[lost]

					// Losing the shard outright is the harder case than holding
					// a stale one: there is nothing to publish, so the sweep has
					// to reconstruct.
					c.blob.forget(victim, obj.hash, lost)

					require.NoError(t, c.service(victim).Pass(t.Context()))

					held, ok := c.blob.held(victim, obj.hash, lost)
					require.True(t, ok, "index %d was not restored", lost)
					assert.Equal(t, obj.place.WriteEpoch, held.epoch,
						"index %d was restored at the wrong generation", lost)
					assert.Equal(t, obj.shards[lost], held.body,
						"index %d was not rebuilt byte for byte", lost)
				}
			})
		}
	}
}

func TestRestoresAShardHeldAtAnOlderGeneration(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 3, 2, 6)
	obj := c.store("overwritten", 40_000)
	stale := obj.place.AllNodes()[1]
	c.blob.hold(stale, obj.hash, 1, obj.place.WriteEpoch-1, []byte("shard from a write two generations ago"))

	svc := c.service(stale)
	require.NoError(t, svc.Pass(t.Context()))

	held, ok := c.blob.held(stale, obj.hash, 1)
	require.True(t, ok)
	assert.Equal(t, obj.shards[1], held.body, "the stale generation must be replaced, not kept")
	assert.Equal(t, obj.place.WriteEpoch, held.epoch)
	assert.Equal(t, int64(1), svc.Stats().Repaired)
	assert.Zero(t, svc.Stats().Pending, "nothing is owed once the shard is back")
}

// TestPublishesAPreparedShardWithoutRebuilding covers the cheap path: a node
// that prepared the shard and never had the commit driven home already holds
// the right bytes, so repair must publish them rather than pay for a stripe
// read it does not need.
func TestPublishesAPreparedShardWithoutRebuilding(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 3, 2, 6)
	obj := c.store("interrupted", 30_000)
	node := obj.place.AllNodes()[4]
	c.blob.forget(node, obj.hash, 4)
	c.blob.prepare(node, obj.hash, 4, obj.place.WriteEpoch, obj.shards[4])

	before := c.blob.gets.Load()
	require.NoError(t, c.service(node).Pass(t.Context()))

	held, ok := c.blob.held(node, obj.hash, 4)
	require.True(t, ok)
	assert.Equal(t, obj.shards[4], held.body)
	assert.Equal(t, before, c.blob.gets.Load(),
		"publishing what the node already prepared must not read a single peer shard")
}

// TestRefusesToRebuildFromMixedGenerations is the rule repair exists to uphold
// from the other side: reconstructing across generations would replace a
// detectable loss with an object that reads back as plausible nonsense.
func TestRefusesToRebuildFromMixedGenerations(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 3, 2, 6)
	obj := c.store("mixed", 20_000)
	nodes := obj.place.AllNodes()
	c.blob.forget(nodes[0], obj.hash, 0)
	// Three peers survive but two of them answer for another write, which
	// leaves fewer than k shards at the generation the record names.
	for _, index := range []int{1, 2} {
		c.blob.hold(nodes[index], obj.hash, index, obj.place.WriteEpoch+9, obj.shards[index])
	}

	svc := c.service(nodes[0])
	require.NoError(t, svc.Pass(t.Context()), "a shard it cannot rebuild is not a failed pass")

	_, ok := c.blob.held(nodes[0], obj.hash, 0)
	assert.False(t, ok, "nothing may be published from a mixture of generations")
	assert.Equal(t, int64(1), svc.Stats().Failed)
	assert.Equal(t, int64(1), svc.Stats().Pending, "the shard is still owed")
}

// TestDiscardsARebuildTheRecordOvertook covers the write that lands mid-repair:
// the rebuilt shard is for a generation the object no longer has, and
// committing it would demote the fresh shard the new write published.
func TestDiscardsARebuildTheRecordOvertook(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 3, 2, 6)
	obj := c.store("overtaken", 20_000)
	node := obj.place.AllNodes()[0]
	c.blob.forget(node, obj.hash, 0)

	// The record moves on while the sweep is between its scan and its commit.
	moved := obj.place
	moved.WriteEpoch = obj.place.WriteEpoch + 1
	c.blob.onPut = func() { c.record(obj.hash, moved) }

	svc := c.service(node)
	require.NoError(t, svc.Pass(t.Context()))

	_, ok := c.blob.held(node, obj.hash, 0)
	assert.False(t, ok, "a shard built for a superseded generation must not be published")
	assert.Equal(t, int64(1), c.blob.aborts.Load(), "and it must not be left prepared either")
}

func TestSkipsPositionsItDoesNotRepairFor(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 3, 2, 6)
	obj := c.store("elsewhere", 20_000)
	nodes := obj.place.AllNodes()
	// Two positions are missing, leaving exactly k peers to rebuild from, and
	// this service owns one of the two.
	c.blob.forget(nodes[1], obj.hash, 1)
	c.blob.forget(nodes[2], obj.hash, 2)

	svc := c.service(nodes[2])
	require.NoError(t, svc.Pass(t.Context()))

	assert.Equal(t, int64(1), svc.Stats().Owned,
		"a gate repairs the nodes sharing its process and no others")
	held, ok := c.blob.held(nodes[2], obj.hash, 2)
	require.True(t, ok)
	assert.Equal(t, obj.shards[2], held.body)
	_, ok = c.blob.held(nodes[1], obj.hash, 1)
	assert.False(t, ok, "the position another gate repairs for must be left alone")
}

// TestAnUnreachableNodeIsNotAShardOwed keeps the pass honest about what it
// found. A node that cannot be asked has not been shown to be missing anything,
// and rebuilding into it would fail anyway.
func TestAnUnreachableNodeIsNotAShardOwed(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 3, 2, 6)
	obj := c.store("unreachable", 20_000)
	node := obj.place.AllNodes()[1]
	c.blob.stop(node)

	svc := c.service(node)
	require.NoError(t, svc.Pass(t.Context()))

	stats := svc.Stats()
	assert.Zero(t, stats.Repaired)
	assert.Zero(t, stats.Failed)
	assert.Zero(t, stats.Pending)
	assert.Zero(t, c.blob.puts.Load(), "nothing may be written to a node that never answered")
}

func TestEmptyObjectsOwnNoShards(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 3, 2, 6)
	obj := c.store("empty", 0)

	svc := c.service(c.nodes...)
	require.NoError(t, svc.Pass(t.Context()))

	assert.Equal(t, int64(1), svc.Stats().Scanned)
	assert.Zero(t, svc.Stats().Owned, "an empty object's record exists so the GET can be served")
	assert.Zero(t, c.blob.stats.Load(), "so there is nothing on any node to ask about")
	assert.Len(t, obj.place.AllNodes(), 5)
}

// TestPagesThroughMoreRecordsThanOnePage exercises the cursor: the object table
// does not fit in one response on any cluster worth repairing, and a scan that
// silently stopped at the first page would repair the alphabetical head of the
// keyspace and report success.
func TestPagesThroughMoreRecordsThanOnePage(t *testing.T) {
	t.Parallel()

	const objects = 37
	c := newCluster(t, 2, 1, 4)

	stored := make([]*object, 0, objects)
	for i := range objects {
		stored = append(stored, c.store(fmt.Sprintf("object-%03d", i), 4096))
	}

	// Everything on one node is gone, as a wiped disk would leave it.
	victim := config.NodeID(1)
	for _, obj := range stored {
		for index, node := range obj.place.AllNodes() {
			if node == victim {
				c.blob.forget(node, obj.hash, index)
			}
		}
	}

	svc, err := New(Config{
		Nodes: []config.NodeID{victim}, Ring: c.ring, Meta: c.meta, Blob: c.blob,
		DataShards: c.k, ParityShards: c.m, Workers: 3, PageSize: 4,
	})
	require.NoError(t, err)
	require.NoError(t, svc.Pass(t.Context()))

	assert.Equal(t, int64(objects), svc.Stats().Scanned, "every page must be walked")
	assert.Zero(t, svc.Stats().Failed)
	assert.Zero(t, svc.Stats().Pending)
	for _, obj := range stored {
		for index, node := range obj.place.AllNodes() {
			if node != victim {
				continue
			}
			held, ok := c.blob.held(node, obj.hash, index)
			require.True(t, ok, "%x index %d was left behind", obj.hash[:4], index)
			assert.Equal(t, obj.shards[index], held.body)
		}
	}
}

// TestIgnoresTheTablesOtherRows keeps the sweep off the rows that share the
// objects prefix. A part belongs to an upload still in flight, and a listing or
// a tombstone names no shards at all.
func TestIgnoresTheTablesOtherRows(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 2, 1, 4)
	obj := c.store("real", 8192)

	raw, err := handlers.EncodePlacement(obj.place)
	require.NoError(t, err)
	for _, key := range []string{
		"arn:aws:s3:::bucket/an-object-key-here",
		"deleted:bucket/a-deleted-object-key",
		"part:0198f2a1-6c3e-7c19-9b1e-4f2d5a6b7c8d:00001",
	} {
		c.meta.put(handlers.TableKey(model.TableObjects, key), raw)
	}

	svc := c.service(c.nodes...)
	require.NoError(t, svc.Pass(t.Context()))

	assert.Equal(t, int64(1), svc.Stats().Scanned,
		"only the placement record keyed by an object hash is a repair candidate")
}

func TestRunStopsWithItsContext(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 2, 1, 4)
	c.store("object", 4096)
	svc := c.service(c.nodes...)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()

	cancel()
	require.NoError(t, <-done, "a cancelled sweep is a clean stop, not a failure")
}

func TestNewRejectsAServiceThatCouldNotSweep(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 2, 1, 4)
	base := Config{
		Nodes: c.nodes, Ring: c.ring, Meta: c.meta, Blob: c.blob,
		DataShards: 2, ParityShards: 1,
	}

	t.Run("no nodes", func(t *testing.T) {
		t.Parallel()
		cfg := base
		cfg.Nodes = nil
		_, err := New(cfg)
		assert.Error(t, err)
	})

	t.Run("no clients", func(t *testing.T) {
		t.Parallel()
		cfg := base
		cfg.Meta = nil
		_, err := New(cfg)
		assert.Error(t, err)
	})

	t.Run("no erasure code", func(t *testing.T) {
		t.Parallel()
		cfg := base
		cfg.DataShards = 0
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
