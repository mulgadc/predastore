package handlers

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/reedsolomon"
	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// hedgeBlobClient serves each shard index from a fixed body, delaying the nodes
// named in stall so a test can make one node slow without making it fail.
type hedgeBlobClient struct {
	shards [][]byte
	stall  map[config.NodeID]time.Duration
	fail   map[config.NodeID]error
	gets   atomic.Int64
	byNode map[config.NodeID]*atomic.Int64
}

func (c *hedgeBlobClient) Get(ctx context.Context, node config.NodeID, req blob.GetRequest) (io.ReadCloser, error) {
	c.gets.Add(1)
	if n, ok := c.byNode[node]; ok {
		n.Add(1)
	}
	if d, ok := c.stall[node]; ok {
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err, ok := c.fail[node]; ok {
		return nil, err
	}
	shard := c.shards[req.Index]
	end := req.RangeEnd + 1
	if req.RangeStart < 0 {
		return io.NopCloser(bytes.NewReader(shard)), nil
	}
	if end > int64(len(shard)) {
		end = int64(len(shard))
	}

	return io.NopCloser(bytes.NewReader(shard[req.RangeStart:end])), nil
}

func (c *hedgeBlobClient) Put(context.Context, config.NodeID, blob.PutRequest, io.Reader) (*blob.PutResponse, error) {
	return nil, errors.New("not implemented")
}
func (c *hedgeBlobClient) Commit(context.Context, config.NodeID, blob.CommitRequest) (bool, error) {
	return false, errors.New("not implemented")
}
func (c *hedgeBlobClient) Release(context.Context, config.NodeID, blob.ReleaseRequest) error {
	return nil
}

func (c *hedgeBlobClient) Abort(context.Context, config.NodeID, blob.CommitRequest) error {
	return errors.New("not implemented")
}
func (c *hedgeBlobClient) Delete(context.Context, config.NodeID, blob.DeleteRequest) (*blob.DeleteResponse, error) {
	return nil, errors.New("not implemented")
}

// rs21 builds an RS(2,1) stripe of shardLen bytes per shard, and the placement
// naming nodes 4, 5 for data and 6 for parity -- the dev-prod topology.
func rs21(t *testing.T, shardLen int) (*hedgeBlobClient, Config, ObjectToShardNodes) {
	t.Helper()
	enc, err := reedsolomon.New(2, 1)
	require.NoError(t, err)

	shards := make([][]byte, 3)
	for i := range shards {
		shards[i] = make([]byte, shardLen)
	}
	for i := range shards[0] {
		shards[0][i] = byte(i % 251)
		shards[1][i] = byte((i * 7) % 241)
	}
	require.NoError(t, enc.Encode(shards))

	return &hedgeBlobClient{
			shards: shards,
			stall:  map[config.NodeID]time.Duration{},
			fail:   map[config.NodeID]error{},
			byNode: map[config.NodeID]*atomic.Int64{4: {}, 5: {}, 6: {}},
		},
		Config{DataShards: 2, ParityShards: 1},
		ObjectToShardNodes{
			Size:             int64(shardLen * 2),
			BlockSize:        int64(shardLen),
			DataShardNodes:   []config.NodeID{4, 5},
			ParityShardNodes: []config.NodeID{6},
		}
}

// The hedge must not fire when the owner is prompt, because it doubles the
// cluster's read load for every request it fires on.
func TestReadRangeHedged_FastOwnerIsNotHedged(t *testing.T) {
	shardLatency = &nodeLatency{}
	bc, cfg, place := rs21(t, 4096)

	got, err := readRangeHedged(t.Context(), bc, cfg, [32]byte{}, place, 0, 0, 128)
	require.NoError(t, err)
	assert.Equal(t, bc.shards[0][:128], got)
	assert.Equal(t, int64(1), bc.gets.Load(), "a prompt owner must be served by exactly one read")
}

// The point of the change: a node that is slow but not failing must not cost
// the caller the blob client's ten-second envelope.
func TestReadRangeHedged_StalledOwnerIsAnsweredByReconstruction(t *testing.T) {
	shardLatency = &nodeLatency{}
	bc, cfg, place := rs21(t, 4096)
	bc.stall[4] = 10 * time.Second

	began := time.Now()
	got, err := readRangeHedged(t.Context(), bc, cfg, [32]byte{}, place, 0, 0, 128)
	took := time.Since(began)

	require.NoError(t, err)
	assert.Equal(t, bc.shards[0][:128], got, "the hedge must reconstruct the same bytes")
	assert.Less(t, took, 2*time.Second, "a stalled owner must not be waited out")
	assert.Positive(t, bc.byNode[5].Load(), "reconstruction reads the other data shard")
	assert.Positive(t, bc.byNode[6].Load(), "reconstruction reads parity")
}

// A failing owner has nothing left in flight, so the hedge must start at once
// rather than waiting out a timer for a read that has already lost.
func TestReadRangeHedged_FailedOwnerHedgesImmediately(t *testing.T) {
	shardLatency = &nodeLatency{}
	bc, cfg, place := rs21(t, 4096)
	bc.fail[4] = errors.New("transport")

	began := time.Now()
	got, err := readRangeHedged(t.Context(), bc, cfg, [32]byte{}, place, 0, 0, 128)

	require.NoError(t, err)
	assert.Equal(t, bc.shards[0][:128], got)
	assert.Less(t, time.Since(began), hedgeFloor, "a failed owner must not wait for the hedge timer")
}

// Losing the owner and one more shard leaves fewer than DataShards, which is a
// real failure and has to be reported rather than served as wrong bytes.
func TestReadRangeHedged_BothArmsFailingIsAnError(t *testing.T) {
	shardLatency = &nodeLatency{}
	bc, cfg, place := rs21(t, 4096)
	bc.fail[4] = errors.New("transport")
	bc.fail[6] = errors.New("transport")

	_, err := readRangeHedged(t.Context(), bc, cfg, [32]byte{}, place, 0, 0, 128)
	require.Error(t, err)
}

// The reconstruction path has to agree with the direct read at an offset, not
// only at the start of a shard -- blocks sit at the same offset in every shard
// and that alignment is what makes a partial decode valid.
func TestReconstructRange_MatchesTheDirectReadAtAnOffset(t *testing.T) {
	shardLatency = &nodeLatency{}
	bc, cfg, place := rs21(t, 8192)

	for _, at := range []int64{0, 1, 4095, 4096, 7000} {
		direct, err := readRangeFromSingleShard(t.Context(), bc, [32]byte{}, place, 1, at, 64)
		require.NoError(t, err)
		rebuilt, err := reconstructRange(t.Context(), bc, cfg, [32]byte{}, place, 1, at, 64)
		require.NoError(t, err)
		assert.Equal(t, direct, rebuilt, "reconstruction disagreed with the direct read at offset %d", at)
	}
}

// The delay is derived, so a node with no history must still hedge, and a node
// answering quickly must not drag the delay below the jitter floor.
func TestNodeLatency_DelayIsBoundedAtBothEnds(t *testing.T) {
	n := &nodeLatency{}
	assert.Equal(t, hedgeFloor, n.delay(4), "an unmeasured node hedges at the floor")

	for range 500 {
		n.observe(4, 200*time.Microsecond)
	}
	assert.Equal(t, hedgeFloor, n.delay(4), "a fast node must not hedge below the floor")

	for range 500 {
		n.observe(5, 5*time.Second)
	}
	assert.Equal(t, hedgeCeiling, n.delay(5), "a slow node must still hedge, at the ceiling")

	for range 500 {
		n.observe(6, 5*time.Millisecond)
	}
	assert.Greater(t, n.delay(6), hedgeFloor, "a measurably slower node hedges later than the floor")
	assert.Less(t, n.delay(6), hedgeCeiling)
}
