package handlers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// downBlob is a blob client with one node that will not answer reads. It
// wraps the honest fake so writes still land everywhere; only the read side
// of the named node fails, which is the shape of a node that is up but not
// serving.
type downBlob struct {
	*fakeBlob

	down config.NodeID
	// delay makes the down node slow rather than broken, so a test can tell
	// a read that waits for it from one that does not.
	delay   time.Duration
	touched atomic.Int64
}

func (b *downBlob) Get(ctx context.Context, node config.NodeID, req blob.GetRequest) (io.ReadCloser, error) {
	if node == b.down {
		b.touched.Add(1)
		if b.delay > 0 {
			select {
			case <-time.After(b.delay):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return nil, fmt.Errorf("node %d is not answering", node)
	}
	return b.fakeBlob.Get(ctx, node, req)
}

var _ BlobClient = (*downBlob)(nil)

// Losing one node must not make the objects on it unreadable: that is the
// whole point of carrying parity. Before this, a shard read error returned
// 500 without the parity ever being consulted, so RS(2,1) tolerated no
// failures at all on the read path.
func TestReadObjectSurvivesOneNodeDown(t *testing.T) {
	t.Parallel()

	for _, rs := range []struct{ data, parity int }{{2, 1}, {3, 2}} {
		t.Run(fmt.Sprintf("rs-%d-%d", rs.data, rs.parity), func(t *testing.T) {
			t.Parallel()
			f := newWriteFixture(rs.data, rs.parity)
			want := randomBytes(t, 1<<20)

			ctx := context.Background()
			objectHash := model.ObjectHash("b", "k")
			place, _, err := f.write(ctx, objectHash, bytes.NewReader(want), int64(len(want)))
			require.NoError(t, err)

			// Take down whichever node holds the first data shard, so the
			// loss is on the path a read uses rather than a spare.
			broken := &downBlob{fakeBlob: f.bc, down: place.DataShardNodes[0]}

			got, _, err := readObject(ctx, broken, f.cfg, "b", "k", place, place.Size, 0)
			require.NoError(t, err, "an object must still read with one node down")
			assert.Equal(t, want, got, "the reconstructed object must be byte-identical")
			assert.Positive(t, broken.touched.Load(), "the down node should have been tried")
		})
	}
}

// Losing more than the parity covers must fail, and must say so rather than
// returning a short or invented object.
func TestReadObjectFailsBeyondParity(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	want := randomBytes(t, 1<<16)

	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")
	place, _, err := f.write(ctx, objectHash, bytes.NewReader(want), int64(len(want)))
	require.NoError(t, err)

	// RS(2,1) survives one loss; two is past what the parity covers.
	broken := &downBlob{fakeBlob: f.bc, down: place.DataShardNodes[0]}
	worse := &twoDownBlob{downBlob: broken, alsoDown: place.DataShardNodes[1]}

	_, _, err = readObject(ctx, worse, f.cfg, "b", "k", place, place.Size, 0)
	require.Error(t, err, "losing more than the parity covers must fail loudly")
}

type twoDownBlob struct {
	*downBlob

	alsoDown config.NodeID
}

func (b *twoDownBlob) Get(ctx context.Context, node config.NodeID, req blob.GetRequest) (io.ReadCloser, error) {
	if node == b.alsoDown {
		return nil, fmt.Errorf("node %d is not answering", node)
	}
	return b.downBlob.Get(ctx, node, req)
}

var _ BlobClient = (*twoDownBlob)(nil)

// Shards are read concurrently, so one slow node costs the slowest shard
// rather than the sum of them. Read sequentially, this takes data*delay.
func TestReadObjectReadsShardsConcurrently(t *testing.T) {
	t.Parallel()

	const delay = 300 * time.Millisecond
	f := newWriteFixture(3, 2)
	want := randomBytes(t, 1<<16)

	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")
	place, _, err := f.write(ctx, objectHash, bytes.NewReader(want), int64(len(want)))
	require.NoError(t, err)

	slow := &slowBlob{fakeBlob: f.bc, delay: delay}
	start := time.Now()
	got, _, err := readObject(ctx, slow, f.cfg, "b", "k", place, place.Size, 0)
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Lessf(t, elapsed, time.Duration(f.cfg.DataShards)*delay,
		"reading %d shards took %v, which is sequential", f.cfg.DataShards, elapsed)
}

// slowBlob answers every read, but only after a delay.
type slowBlob struct {
	*fakeBlob

	delay time.Duration
}

func (b *slowBlob) Get(ctx context.Context, node config.NodeID, req blob.GetRequest) (io.ReadCloser, error) {
	select {
	case <-time.After(b.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return b.fakeBlob.Get(ctx, node, req)
}

var _ BlobClient = (*slowBlob)(nil)

// countingBlob records how many reads each node was asked for, so a test can
// prove a shard was fetched once rather than once per pass.
type countingBlob struct {
	*fakeBlob

	mu   sync.Mutex
	gets map[config.NodeID]int
}

func newCountingBlob(inner *fakeBlob) *countingBlob {
	return &countingBlob{fakeBlob: inner, gets: make(map[config.NodeID]int)}
}

func (b *countingBlob) Get(ctx context.Context, node config.NodeID, req blob.GetRequest) (io.ReadCloser, error) {
	b.mu.Lock()
	b.gets[node]++
	b.mu.Unlock()
	return b.fakeBlob.Get(ctx, node, req)
}

func (b *countingBlob) count(node config.NodeID) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.gets[node]
}

var _ BlobClient = (*countingBlob)(nil)

// A node that accepts a read and then sits on it must not set the latency of
// every object placed on it. With RS(2,1) the parity shard is a complete
// answer, so the read should finish in about the hedge delay rather than
// waiting out the stall — twice, which is what it used to do.
func TestReadObjectDoesNotWaitOutAStalledNode(t *testing.T) {
	t.Parallel()

	const stall = 4 * time.Second
	f := newWriteFixture(2, 1)
	want := randomBytes(t, 1<<16)

	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")
	place, _, err := f.write(ctx, objectHash, bytes.NewReader(want), int64(len(want)))
	require.NoError(t, err)

	stalled := &downBlob{fakeBlob: f.bc, down: place.DataShardNodes[0], delay: stall}

	start := time.Now()
	got, _, err := readObject(ctx, stalled, f.cfg, "b", "k", place, place.Size, 0)
	elapsed := time.Since(start)

	require.NoError(t, err, "the parity shard makes this object readable")
	assert.Equal(t, want, got, "the reconstructed object must be byte-identical")
	assert.Lessf(t, elapsed, stall,
		"the read took %v, so it waited for the stalled node", elapsed)
	assert.Equal(t, int64(1), stalled.touched.Load(),
		"the stalled node must be asked once, not once per read pass")
}

// A healthy read has no reason to fetch the parity shard, which at RS(2,1)
// would be half again as much blob traffic for nothing.
func TestReadObjectDoesNotFetchParityWhenTheDataShardsAnswer(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	want := randomBytes(t, 1<<16)

	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")
	place, _, err := f.write(ctx, objectHash, bytes.NewReader(want), int64(len(want)))
	require.NoError(t, err)

	counting := newCountingBlob(f.bc)
	got, _, err := readObject(ctx, counting, f.cfg, "b", "k", place, place.Size, 0)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	for _, node := range place.ParityShardNodes {
		assert.Zerof(t, counting.count(node),
			"parity node %d was read on a healthy object", node)
	}
	for _, node := range place.DataShardNodes {
		assert.Equalf(t, 1, counting.count(node),
			"data node %d was read more than once", node)
	}
}
