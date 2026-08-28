// Striping is a change to how bytes sit on the nodes, so the risk it carries
// is not that a new object fails loudly but that an old one comes back
// scrambled. These pin both directions: the contiguous layout still assembles
// exactly as the encoder's own Join did, and the striped one round-trips.

package handlers

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/klauspost/reedsolomon"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// split lays an object out the way the layout says, which is the write path's
// arithmetic expressed once more so the join is checked against something.
func split(t *testing.T, lay layout, data []byte) [][]byte {
	t.Helper()
	shards := make([][]byte, lay.dataShards)
	for i := range shards {
		shards[i] = make([]byte, lay.shardSize)
	}

	var read int64
	for offset := int64(0); offset < lay.shardSize; offset += lay.blockSize {
		n := min(lay.blockSize, lay.shardSize-offset)
		for i := range lay.dataShards {
			take := min(n, int64(len(data))-read)
			if take > 0 {
				copy(shards[i][offset:], data[read:read+take])
				read += take
			}
		}
	}

	return shards
}

// The compatibility guarantee. A record with no block size describes an object
// whose shards are contiguous, and it has to assemble byte for byte the way
// the encoder's Join assembled it before the layout existed.
func TestTheContiguousLayoutMatchesTheEncodersJoin(t *testing.T) {
	t.Parallel()

	for _, size := range []int{1, 1023, 1 << 20, (1 << 20) + 7} {
		t.Run(fmt.Sprintf("size-%d", size), func(t *testing.T) {
			t.Parallel()
			const k, m = 3, 2
			data := randomBytes(t, size)

			enc, err := reedsolomon.New(k, m)
			require.NoError(t, err)
			shards, err := enc.Split(data)
			require.NoError(t, err)

			stream, err := reedsolomon.NewStream(k, m)
			require.NoError(t, err)
			var want bytes.Buffer
			require.NoError(t, stream.Join(&want, shardReadersOf(shards[:k]), int64(size)))

			// BlockSize 0 is what a version 1 placement record decodes to.
			var got bytes.Buffer
			lay := newLayout(k, int64(size), 0)
			require.NoError(t, lay.join(&got, shards[:k], int64(size)))

			assert.Equal(t, want.Bytes(), got.Bytes())
		})
	}
}

// The striped layout has to be its own inverse across the boundaries where a
// block loop gets the arithmetic wrong: exactly one block, either side of it,
// and a size that leaves the last stripe short.
func TestTheStripedLayoutJoinsWhatItSplit(t *testing.T) {
	t.Parallel()

	const block = 4096
	for _, k := range []int{2, 3, 5} {
		for _, size := range []int{1, block - 1, block, block + 1,
			k * block, k*block + 1, 3*k*block + 12345} {
			t.Run(fmt.Sprintf("k-%d/size-%d", k, size), func(t *testing.T) {
				t.Parallel()
				data := randomBytes(t, size)
				lay := newLayout(k, int64(size), block)

				var got bytes.Buffer
				require.NoError(t, lay.join(&got, split(t, lay, data), int64(size)))
				assert.Equal(t, data, got.Bytes())
			})
		}
	}
}

// locate is what a ranged read trusts to find a byte, so it has to agree with
// the join about where every byte ended up.
func TestLocateFindsTheByteTheJoinEmits(t *testing.T) {
	t.Parallel()

	const (
		k     = 3
		block = 512
		size  = 7*block + 91
	)
	data := randomBytes(t, size)
	lay := newLayout(k, size, block)
	shards := split(t, lay, data)

	for offset := range int64(size) {
		shard, at := lay.locate(offset)
		require.Less(t, shard, k, "offset %d located outside the data shards", offset)
		assert.Equal(t, data[offset], shards[shard][at],
			"offset %d located to shard %d at %d, which holds a different byte", offset, shard, at)
	}
}

// A range served from one shard is only correct if it is one run of bytes in
// that shard. contiguous is the guard on that fast path, so it must not claim
// a range that crosses a block.
func TestContiguousOnlyClaimsRangesInsideOneBlock(t *testing.T) {
	t.Parallel()

	lay := newLayout(3, 10<<10, 512)

	assert.True(t, lay.contiguous(0, 511), "a whole block is contiguous")
	assert.True(t, lay.contiguous(10, 200), "a range inside one block is contiguous")
	assert.False(t, lay.contiguous(511, 512), "a range crossing a block boundary is not")
	assert.False(t, lay.contiguous(0, 5000), "a range spanning blocks is not")
}

// A shard shorter than the layout expects means a read assembled from partial
// data. Returning it as an object would be silent corruption, so the join has
// to refuse rather than emit what it has.
func TestJoinRefusesAShortShard(t *testing.T) {
	t.Parallel()

	lay := newLayout(2, 8192, 1024)
	shards := split(t, lay, randomBytes(t, 8192))
	shards[1] = shards[1][:100]

	err := lay.join(&bytes.Buffer{}, shards, 8192)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "want at least")
}

// Ranged reads are where a layout change does its quietest damage: the fast
// path serves bytes straight off one shard without ever joining the object, so
// a wrong offset returns a plausible-looking wrong answer rather than an error.
func TestRangedReadsAgreeWithTheWholeObject(t *testing.T) {
	t.Parallel()

	const size = 3*streamBlockSize + 7919
	f := newWriteFixture(2, 1)
	body := randomBytes(t, size)

	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")
	place, _, err := f.write(ctx, objectHash, bytes.NewReader(body), size)
	require.NoError(t, err)

	ranges := []struct{ start, end int64 }{
		{0, 0},
		{0, 1023},
		{streamBlockSize - 1, streamBlockSize},   // across a block boundary
		{streamBlockSize, 2*streamBlockSize - 1}, // exactly the second block
		{2*streamBlockSize + 5, 3 * streamBlockSize},
		{3 * streamBlockSize, size - 1}, // the short last stripe
		{size - 1, size - 1},
		{0, size - 1},
	}
	for _, r := range ranges {
		t.Run(fmt.Sprintf("%d-%d", r.start, r.end), func(t *testing.T) {
			t.Parallel()
			got, contentRange, _, rangeErr := readRange(
				ctx, f.bc, f.cfg, "b", "k", place, size, r.start, r.end, 0)
			require.NoError(t, rangeErr)
			assert.Equal(t, fmt.Sprintf("bytes %d-%d/%d", r.start, r.end, size), contentRange)
			assert.Equal(t, body[r.start:r.end+1], got)
		})
	}
}
