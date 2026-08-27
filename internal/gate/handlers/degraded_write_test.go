package handlers

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// degradedFixture is a write fixture with degraded writes turned on and one
// named shard position refusing, which is what a node being down looks like to
// the write path.
func degradedFixture(t *testing.T, data, parity int, down ...uint32) writeFixture {
	t.Helper()

	f := newWriteFixture(data, parity)
	f.cfg.DegradedWrites = true
	f.bc.failPutOn = func(index uint32) bool { return slices.Contains(down, index) }

	return f
}

// The availability property: a node down must not refuse writes. Any k of the
// stripe reconstruct the object, so k is the floor, and the write is durable
// at it.
func TestDegradedWriteSucceedsWithANodeDown(t *testing.T) {
	t.Parallel()

	for _, rs := range []struct{ data, parity int }{{2, 1}, {3, 2}} {
		for down := range rs.data + rs.parity {
			t.Run(fmt.Sprintf("rs-%d-%d/node-%d", rs.data, rs.parity, down), func(t *testing.T) {
				t.Parallel()
				f := degradedFixture(t, rs.data, rs.parity, uint32(down))
				want := randomBytes(t, 1<<16)

				ctx := context.Background()
				objectHash := model.ObjectHash("b", "k")
				place, written, err := f.write(ctx, objectHash, bytes.NewReader(want), int64(len(want)))

				require.NoError(t, err, "one node down is what the parity is for")
				assert.Equal(t, rs.data+rs.parity-1, written.landedCount())
				assert.True(t, written.degraded(), "a write short of full width has to say so")
				require.Len(t, written.missing, 1)

				// The object is what matters: it has to read back exactly,
				// rebuilding the shard that never landed.
				got, _, err := readObject(ctx, f.bc, f.cfg, "b", "k", place, place.Size)
				require.NoError(t, err)
				assert.Equal(t, want, got)
			})
		}
	}
}

// Below the floor the write fails and says which nodes were missing, rather
// than acknowledging something that cannot be reconstructed.
func TestDegradedWriteFailsBelowTheFloor(t *testing.T) {
	t.Parallel()

	// RS(2,1) tolerates one loss; two leaves fewer than k.
	f := degradedFixture(t, 2, 1, 0, 2)
	body := randomBytes(t, 1<<15)

	_, written, err := f.write(context.Background(), model.ObjectHash("b", "k"),
		bytes.NewReader(body), int64(len(body)))

	require.ErrorIs(t, err, errShardWriteFloor)
	assert.Equal(t, 1, written.landedCount())
	assert.Len(t, written.missing, 2, "the error has to name every node that was missing")
}

// Off is the default, and off means the full stripe. A cluster with no repair
// running must not quietly accumulate objects one loss from unreadable.
func TestFullWidthIsTheDefaultAndOneNodeDownFailsTheWrite(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	require.False(t, f.cfg.DegradedWrites, "degraded writes must be opt-in")
	require.Equal(t, f.cfg.TotalShards(), f.cfg.MinShards())

	f.bc.failPutOn = func(index uint32) bool { return index == 1 }
	body := randomBytes(t, 1<<15)

	_, _, err := f.write(context.Background(), model.ObjectHash("b", "k"),
		bytes.NewReader(body), int64(len(body)))

	require.ErrorIs(t, err, errShardWriteFloor)
}

// The node that missed the write must hold no shard under the record's epoch.
// That absence is the whole discovery mechanism: repair finds the outstanding
// shards by comparing each holder's stored epoch against the record's, so a
// shard published at the new epoch on a node that never received it would be
// invisible to repair and wrong to a reader.
func TestDegradedWriteLeavesTheMissingNodeWithoutTheEpoch(t *testing.T) {
	t.Parallel()

	f := degradedFixture(t, 2, 1, 1)
	body := randomBytes(t, 1<<15)

	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")
	place, _, err := f.write(ctx, objectHash, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err)

	f.bc.mu.Lock()
	defer f.bc.mu.Unlock()
	assert.Empty(t, f.bc.prepared, "every shard that landed must be published")
	for index := range uint32(3) {
		id := shardID{key: objectHash, index: index}
		shard, held := f.bc.shards[id]
		if index == 1 {
			assert.False(t, held, "the node that was down must hold nothing at this epoch")

			continue
		}
		require.True(t, held, "shard %d landed and must be published", index)
		assert.Equal(t, place.WriteEpoch, shard.epoch)
	}
	assert.Equal(t, int64(2), f.bc.commitCalls.Load(),
		"only the shards that prepared may be committed")
}

// A shard that never prepared must not be aborted either. Abort names an epoch
// the node does not hold, so at best it is a wasted round trip and at worst it
// asks a node to discard a generation it is still serving.
func TestDegradedWriteDoesNotAbortTheShardThatNeverLanded(t *testing.T) {
	t.Parallel()

	f := degradedFixture(t, 2, 1, 2)
	body := randomBytes(t, 1<<15)

	_, _, err := f.write(context.Background(), model.ObjectHash("b", "k"),
		bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err)

	assert.Zero(t, f.bc.abortCalls.Load(), "a successful write aborts nothing")
}

// The parity shards used to share one encoder writing into per-shard pipes, so
// a single parity node refusing closed its pipe, failed the encode and took
// every other parity shard down with it. At RS(3,2) that is the difference
// between losing one shard and losing all the redundancy at once.
func TestOneParityNodeDownDoesNotCostTheOtherParityShard(t *testing.T) {
	t.Parallel()

	f := degradedFixture(t, 3, 2, 3)
	want := randomBytes(t, 1<<16)

	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")
	place, written, err := f.write(ctx, objectHash, bytes.NewReader(want), int64(len(want)))
	require.NoError(t, err)

	assert.Equal(t, 4, written.landedCount(), "only the refusing node may be short")
	f.bc.mu.Lock()
	_, held := f.bc.shards[shardID{key: objectHash, index: 4}]
	f.bc.mu.Unlock()
	assert.True(t, held, "the second parity shard was lost with the first")

	// And it is worth what parity is worth: with two more shards gone the
	// object still rebuilds.
	bc := &staleBlob{fakeBlob: f.bc, stale: map[int]bool{0: true}}
	got, _, err := readObject(ctx, bc, f.cfg, "b", "k", place, place.Size)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

// A caller writing something it cannot reproduce deserves to know the write
// landed short of full redundancy. It is not an error and must not read as one.
func TestPutObjectReportsADegradedWriteInAHeader(t *testing.T) {
	t.Parallel()

	f := degradedFixture(t, 2, 1, 2)
	w := httptest.NewRecorder()

	PutObject(f.mc, f.bc, f.ring, testCache(), f.cfg).
		ServeHTTP(w, objectPut("k", randomBytes(t, 1<<15)))

	require.Equal(t, http.StatusOK, w.Code, "a degraded write is durable and must be acknowledged")
	assert.Equal(t, "1", w.Header().Get(degradedWriteHeader))
}

// A full-width write says nothing, so the header means what it says.
func TestPutObjectOmitsTheDegradedHeaderAtFullWidth(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	f.cfg.DegradedWrites = true
	w := httptest.NewRecorder()

	PutObject(f.mc, f.bc, f.ring, testCache(), f.cfg).
		ServeHTTP(w, objectPut("k", randomBytes(t, 1<<15)))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, w.Header().Get(degradedWriteHeader))
}

// An empty object places no shard at all, so it is full width by definition
// rather than degraded by having nothing to compare.
func TestEmptyObjectIsNotADegradedWrite(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	f.cfg.DegradedWrites = true

	_, written, err := f.write(context.Background(), model.ObjectHash("b", "k"),
		bytes.NewReader(nil), 0)

	require.NoError(t, err)
	assert.False(t, written.degraded())
	assert.Equal(t, f.cfg.TotalShards(), written.landedCount())
}
