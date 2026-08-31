// The write path used to hold every shard whole, so a PUT cost (k+m)/k times
// the object on the heap and a 4 GiB upload peaked at 27 GiB of RSS. These
// hold the property that replaced it: the working set is a fixed number of
// blocks, and it does not grow with the object.

package handlers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/gate/placement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// patternReader synthesises a body without holding one. A test that allocated
// its own object could not tell the gate's allocations from its own.
type patternReader struct {
	remaining int64
	seed      byte
}

func (p *patternReader) Read(b []byte) (int, error) {
	if p.remaining <= 0 {
		return 0, io.EOF
	}
	n := min(int64(len(b)), p.remaining)
	for i := range b[:n] {
		p.seed = p.seed*31 + 17
		b[i] = p.seed
	}
	p.remaining -= n

	return int(n), nil
}

// discardBlob accepts shards without keeping them, so the only heap the test
// measures is the gate's. It counts the largest single write it was handed,
// which is what says whether the caller streamed or staged.
type discardBlob struct {
	maxWrite atomic.Int64
	putCalls atomic.Int64
	// down is keyed by node rather than by shard index, so a shard re-aimed at
	// the spare is judged by where it went. Keying on the index would fail the
	// handoff too and prove nothing.
	down      map[config.NodeID]bool
	failAfter int64 // bytes a failing node accepts before it gives up
}

func (d *discardBlob) Put(_ context.Context, node config.NodeID, req blob.PutRequest, body io.Reader) (*blob.PutResponse, error) {
	d.putCalls.Add(1)
	if d.down[node] {
		if d.failAfter == 0 {
			return nil, errors.New("node is not answering")
		}
		if _, err := io.CopyN(io.Discard, body, d.failAfter); err != nil {
			return nil, err
		}

		return nil, errors.New("node stopped part way through the shard")
	}

	buf := make([]byte, 64<<10)
	var total int64
	for {
		n, err := body.Read(buf)
		if n > 0 {
			total += int64(n)
			for {
				prev := d.maxWrite.Load()
				if int64(n) <= prev || d.maxWrite.CompareAndSwap(prev, int64(n)) {
					break
				}
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if total >= req.Size {
			break
		}
	}

	return &blob.PutResponse{Size: total, Epoch: req.Epoch}, nil
}

func (d *discardBlob) Commit(context.Context, config.NodeID, blob.CommitRequest) (bool, error) {
	return false, nil
}
func (d *discardBlob) Abort(context.Context, config.NodeID, blob.CommitRequest) error { return nil }

func (d *discardBlob) Get(context.Context, config.NodeID, blob.GetRequest) (io.ReadCloser, error) {
	return nil, blob.ErrNotFound
}

func (d *discardBlob) Delete(context.Context, config.NodeID, blob.DeleteRequest) (*blob.DeleteResponse, error) {
	return &blob.DeleteResponse{}, nil
}

var _ BlobClient = (*discardBlob)(nil)

// syntheticPlace resolves where a synthetic object of this size would go, so
// a test can name the node holding a shard before it writes one.
func syntheticPlace(t *testing.T, f writeFixture, size int64) ([32]byte, ObjectToShardNodes) {
	t.Helper()
	objectHash := model.ObjectHash("b", "k")
	place, err := placeShards(f.ring, f.cfg, objectHash, size)
	require.NoError(t, err)

	return objectHash, place
}

// writeSynthetic puts an object of the given size without ever holding it.
func writeSynthetic(t *testing.T, f writeFixture, bc BlobClient, size int64) (writeResult, error) {
	t.Helper()
	objectHash, place := syntheticPlace(t, f, size)

	return writeObject(context.Background(), bc, f.cfg, f.ring,
		&patternReader{remaining: size}, size, objectHash, place)
}

// ringOf builds a ring of n nodes, which is how a test gets one node more than
// the stripe is wide and therefore a handoff holder to aim at.
func ringOf(n int) *placement.Ring {
	nodes := make([]config.NodeID, n)
	for i := range nodes {
		nodes[i] = config.NodeID(i + 1)
	}

	return placement.NewRing(nodes)
}

// The property the whole phase exists for. A buffered write allocates at least
// (k+m)/k times the object; a streaming one allocates its blocks once and
// reuses them, so what it allocates does not follow the size.
func TestWriteAllocationDoesNotFollowObjectSize(t *testing.T) {
	const (
		small = 8 << 20
		large = 256 << 20
	)

	allocatedFor := func(size int64) uint64 {
		f := newWriteFixture(2, 1)
		bc := &discardBlob{}

		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		_, err := writeSynthetic(t, f, bc, size)
		require.NoError(t, err)
		runtime.ReadMemStats(&after)

		return after.TotalAlloc - before.TotalAlloc
	}

	smallAlloc := allocatedFor(small)
	largeAlloc := allocatedFor(large)

	// The object is 32x bigger. Buffering would make the allocation 32x too;
	// streaming leaves it dominated by the fixed block set. The budget is
	// deliberately loose — the assertion is the shape, not a byte count.
	assert.Less(t, largeAlloc, uint64(large),
		"a %d-byte object allocated %d bytes, so the write is still holding it", large, largeAlloc)
	assert.Less(t, largeAlloc, smallAlloc*4,
		"allocation grew %dx for a 32x object, so it still follows the size", largeAlloc/max(smallAlloc, 1))
}

// The shards have to reach their nodes in blocks. Arriving in one write is
// what a staged object looks like from the node's side.
func TestWriteReachesNodesInBlocks(t *testing.T) {
	t.Parallel()

	const size = 64 << 20
	f := newWriteFixture(2, 1)
	bc := &discardBlob{}

	_, err := writeSynthetic(t, f, bc, size)
	require.NoError(t, err)

	assert.LessOrEqual(t, bc.maxWrite.Load(), int64(streamBlockSize),
		"a shard arrived in a single write larger than one block")
	assert.Equal(t, int64(3), bc.putCalls.Load(), "one put per shard, no retries")
}

// Streaming must not change what is stored. Sizes are chosen around the block
// boundary, which is where a stripe loop gets its arithmetic wrong: one block
// exactly, one byte under, one byte over, and a size that does not divide by k.
func TestStreamedWriteRoundTripsAtBlockBoundaries(t *testing.T) {
	t.Parallel()

	sizes := []int{
		1,
		1023,
		streamBlockSize * 2,
		streamBlockSize*2 - 1,
		streamBlockSize*2 + 1,
		streamBlockSize*2 + 12345,
	}
	for _, rs := range []struct{ data, parity int }{{2, 1}, {3, 2}} {
		for _, size := range sizes {
			t.Run(fmt.Sprintf("rs-%d-%d/size-%d", rs.data, rs.parity, size), func(t *testing.T) {
				t.Parallel()
				f := newWriteFixture(rs.data, rs.parity)
				want := randomBytes(t, size)
				assert.Equal(t, want, f.roundTrip(t, "b", "k", want))
			})
		}
	}
}

// A node that will not take the shard at all is refused on the first block,
// which is the only point at which the shard can still be re-aimed. The write
// stays at full width because the spare took it.
func TestAShardRefusedAtTheFirstBlockIsHandedOff(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	f.cfg.HintedHandoff = true
	f.ring = ringOf(4)
	_, place := syntheticPlace(t, f, 16<<20)
	bc := &discardBlob{down: map[config.NodeID]bool{place.AllNodes()[0]: true}}

	written, err := writeSynthetic(t, f, bc, 16<<20)
	require.NoError(t, err)

	assert.Equal(t, []int{0}, written.handoff, "the refused shard was not handed off")
	assert.Empty(t, written.missing, "the stripe is complete, so nothing is missing")
	assert.Equal(t, 3, written.landedCount())
}

// The behaviour change streaming forces. Nothing is buffered, so a shard whose
// node stops half way cannot be started again anywhere: the blocks already
// sent are gone. That position is missing, and repair is what restores it.
func TestAShardLostMidObjectIsMissingRatherThanHandedOff(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	f.cfg.HintedHandoff = true
	f.cfg.DegradedWrites = true
	f.ring = ringOf(4)
	_, place := syntheticPlace(t, f, 16<<20)
	bc := &discardBlob{
		down:      map[config.NodeID]bool{place.AllNodes()[0]: true},
		failAfter: streamBlockSize,
	}

	written, err := writeSynthetic(t, f, bc, 16<<20)
	require.NoError(t, err)

	assert.Empty(t, written.handoff, "a shard already part-sent cannot be handed off")
	assert.Len(t, written.missing, 1, "the position it was sent to is missing")
	assert.False(t, written.landed[0])
	assert.Equal(t, 2, written.landedCount(), "the other shards were not taken down with it")
}

// One node refusing must not fail the encode. That coupling is why the
// buffered version held every shard whole, and it is the reason a single
// parity node could take all the redundancy with it.
func TestOneRefusingNodeDoesNotFailTheOthers(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(3, 2)
	f.cfg.DegradedWrites = true
	_, place := syntheticPlace(t, f, 32<<20)
	bc := &discardBlob{down: map[config.NodeID]bool{place.AllNodes()[4]: true}}

	written, err := writeSynthetic(t, f, bc, 32<<20)
	require.NoError(t, err)

	assert.Equal(t, 4, written.landedCount(), "the surviving shards all landed")
	assert.Len(t, written.missing, 1)
}

// A body that stops short of its declared length must not be stored as an
// object with zeros on the end. The padding that squares the last stripe is
// not a licence to invent data the client never sent.
func TestAShortBodyIsRefused(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	bc := &discardBlob{}
	objectHash := model.ObjectHash("b", "k")
	place, err := placeShards(f.ring, f.cfg, objectHash, 8<<20)
	require.NoError(t, err)

	_, err = writeObject(context.Background(), bc, f.cfg, f.ring,
		&patternReader{remaining: 4 << 20}, 8<<20, objectHash, place)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "declared")
}
