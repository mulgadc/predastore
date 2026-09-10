// Striping is a change to how bytes sit on the nodes, so the risk it carries
// is not that a new object fails loudly but that an old one comes back
// scrambled. These pin both directions: the contiguous layout still assembles
// exactly as the encoder's own Join did, and the striped one round-trips.

package handlers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/klauspost/reedsolomon"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// shardReadersOf feeds shards to the stream encoder, which is the only thing
// left that wants them as readers: the reference Join this file checks against.
func shardReadersOf(shards [][]byte) []io.Reader {
	readers := make([]io.Reader, len(shards))
	for i, s := range shards {
		readers[i] = bytes.NewReader(s)
	}

	return readers
}

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
	f.publish(t, objectHash, place)

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
			req := objectRequest(http.MethodGet, "k", "")
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", r.start, r.end))

			w := httptest.NewRecorder()
			GetObject(f.mc, f.bc, f.ring, testCache(), f.cfg).ServeHTTP(w, req)

			require.Equal(t, http.StatusPartialContent, w.Code)
			assert.Equal(t, fmt.Sprintf("bytes %d-%d/%d", r.start, r.end, size),
				w.Header().Get("Content-Range"))
			assert.Equal(t, body[r.start:r.end+1], w.Body.Bytes())
		})
	}
}

// A range that crosses a block cannot be served off one shard, and the read
// that replaces it used to start every shard at byte zero and run to the end of
// the object. The bytes came back correct, so only a count of what the nodes
// were made to deliver catches it: a two byte range cost a whole object of
// shard reads, transfer and decryption.
//
// The bound is the stripes the range touches, because a stripe is the unit
// parity rebuilds and so the smallest thing a degradable read can fetch.
func TestARangedReadCostsOnlyTheStripesItTouches(t *testing.T) {
	t.Parallel()

	const size = 3*streamBlockSize + 7919
	f := newWriteFixture(2, 1)
	body := randomBytes(t, size)

	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")
	place, _, err := f.write(ctx, objectHash, bytes.NewReader(body), size)
	require.NoError(t, err)
	f.publish(t, objectHash, place)

	lay := newLayout(f.cfg.DataShards, size, place.BlockSize)

	// touched is what the range ought to cost: every data shard, from the start
	// of the stripe holding start to the end of the block holding end.
	touched := func(start, end int64) int64 {
		_, from := lay.stripeStart(start)
		_, last := lay.stripeStart(end)
		to := last + min(lay.blockSize, lay.shardSize-last)

		return int64(lay.dataShards) * (to - from)
	}

	// The last stripe is short, so its offsets are spaced by the remainder
	// rather than by the block. A range crossing inside it is what catches an
	// alignment that rounded by the block size instead of asking the layout.
	head := int64(lay.dataShards) * lay.blockSize * (lay.shardSize / lay.blockSize)
	tailCross := head + lay.shardSize%lay.blockSize
	require.Less(t, tailCross, int64(size), "fixture has no short last stripe to cross")

	ranges := []struct{ start, end int64 }{
		{streamBlockSize - 1, streamBlockSize},       // across the first boundary
		{streamBlockSize - 1, 2 * streamBlockSize},   // across two
		{2*streamBlockSize + 5, 3 * streamBlockSize}, // into the short last stripe
		{tailCross - 1, tailCross},                   // across a boundary in the tail
		{size - 2, size - 1},                         // one block: the fast path
		{0, size - 1},                                // the whole object
	}
	// One tally over one object, so the ranges run in sequence rather than as
	// parallel subtests: a shared counter cannot attribute concurrent reads.
	for _, r := range ranges {
		req := objectRequest(http.MethodGet, "k", "")
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", r.start, r.end))

		f.bc.getBytes.Store(0)
		w := httptest.NewRecorder()
		GetObject(f.mc, f.bc, f.ring, testCache(), f.cfg).ServeHTTP(w, req)

		require.Equal(t, http.StatusPartialContent, w.Code, "range %d-%d", r.start, r.end)
		require.Equal(t, body[r.start:r.end+1], w.Body.Bytes(), "range %d-%d", r.start, r.end)
		assert.LessOrEqualf(t, f.bc.getBytes.Load(), touched(r.start, r.end),
			"range %d-%d of %d bytes read %d from the nodes, more than the %d its stripes hold",
			r.start, r.end, r.end-r.start+1, f.bc.getBytes.Load(), touched(r.start, r.end))
	}
}
