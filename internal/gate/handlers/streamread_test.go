// The streaming read gives up the ability to retry an object wholesale: once
// the header is out, the bytes already sent cannot be taken back. What it buys
// is recovery part way through, and these pin that -- a shard that dies mid
// object is replaced from parity opened at the offset the read has reached.

package handlers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
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

// dyingBlob serves one node's shard normally for a while and then fails the
// stream, which is the failure a whole-object read could not see: the shard
// opened, so nothing was wrong until the bytes ran out.
type dyingBlob struct {
	*fakeBlob

	// node -> how many bytes it serves before the stream fails.
	dying map[config.NodeID]int64
}

type dyingReader struct {
	io.ReadCloser

	left int64
}

func (r *dyingReader) Read(p []byte) (int, error) {
	if r.left <= 0 {
		return 0, errors.New("shard stream died")
	}
	if int64(len(p)) > r.left {
		p = p[:r.left]
	}
	n, err := r.ReadCloser.Read(p)
	r.left -= int64(n)

	return n, err
}

func (b *dyingBlob) Get(ctx context.Context, node config.NodeID, req blob.GetRequest) (io.ReadCloser, error) {
	rc, err := b.fakeBlob.Get(ctx, node, req)
	after, dying := b.dying[node]
	if err != nil || !dying {
		return rc, err
	}

	return &dyingReader{ReadCloser: rc, left: after}, nil
}

var _ BlobClient = (*dyingBlob)(nil)

// The whole point of streaming: a shard lost after the response has begun is
// still recoverable, because the bytes it would have supplied have not been
// sent yet.
func TestAShardLostPartWayThroughIsRebuiltFromParity(t *testing.T) {
	t.Parallel()

	const size = 5 * streamBlockSize
	f := newWriteFixture(2, 1)
	want := randomBytes(t, size)

	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")
	place, _, err := f.write(ctx, objectHash, bytes.NewReader(want), size)
	require.NoError(t, err)
	f.publish(t, objectHash, place)

	// One block through, so the first stripe is healthy and the header has
	// already been sent by the time the shard goes.
	dying := &dyingBlob{fakeBlob: f.bc,
		dying: map[config.NodeID]int64{place.DataShardNodes[0]: streamBlockSize}}

	w := httptest.NewRecorder()
	GetObject(f.mc, dying, f.ring, testCache(), f.cfg).ServeHTTP(w, objectRequest(http.MethodGet, "k", ""))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, want, w.Body.Bytes(), "the object must be complete despite the shard dying mid read")

	// The header went out before the loss, so it reports the healthy first
	// stripe and says nothing. The count there is a floor, not a total.
	assert.Empty(t, w.Header().Get(degradedHeader))
}

// openWatcher records the requests that went to the blob client, so a test can
// tell a parity read that started at the loss from one that re-read the shard
// from the beginning.
type openWatcher struct {
	BlobClient

	mu   sync.Mutex
	seen []blob.GetRequest
}

func (b *openWatcher) Get(ctx context.Context, node config.NodeID, req blob.GetRequest) (io.ReadCloser, error) {
	b.mu.Lock()
	b.seen = append(b.seen, req)
	b.mu.Unlock()

	return b.BlobClient.Get(ctx, node, req)
}

var _ BlobClient = (*openWatcher)(nil)

// Parity is opened at the offset the read has reached. Opening it from zero
// would make a shard lost at the end of a large object cost a second full
// stream of it, which is the cost the old whole-object read paid every time.
func TestParityIsOpenedAtTheOffsetTheReadReached(t *testing.T) {
	t.Parallel()

	const size = 5 * streamBlockSize
	f := newWriteFixture(2, 1)
	body := randomBytes(t, size)

	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")
	place, _, err := f.write(ctx, objectHash, bytes.NewReader(body), size)
	require.NoError(t, err)

	dying := &dyingBlob{fakeBlob: f.bc,
		dying: map[config.NodeID]int64{place.DataShardNodes[0]: streamBlockSize}}
	watched := &openWatcher{BlobClient: dying}

	got, degraded, err := readObject(ctx, watched, f.cfg, "b", "k", place, size, 0)
	require.NoError(t, err)
	assert.Equal(t, body, got)
	assert.Positive(t, degraded, "the dying shard should have been rebuilt")

	lay := newLayout(f.cfg.DataShards, size, place.BlockSize)
	parityOpens := 0
	for _, req := range watched.seen {
		if int(req.Index) < f.cfg.DataShards {
			continue
		}
		parityOpens++
		assert.EqualValues(t, streamBlockSize, req.RangeStart,
			"parity was opened at %d rather than at the block the read had reached", req.RangeStart)
		assert.Equal(t, lay.shardSize-1, req.RangeEnd)
	}
	assert.Equal(t, 1, parityOpens, "parity should be opened once and then followed")
}

// uniformlySlowBlob delays every node equally, which is a slow cluster rather
// than a straggler.
type uniformlySlowBlob struct {
	*fakeBlob

	delay time.Duration
	gets  atomic.Int64
}

func (b *uniformlySlowBlob) Get(ctx context.Context, node config.NodeID, req blob.GetRequest) (io.ReadCloser, error) {
	b.gets.Add(1)
	time.Sleep(b.delay)

	return b.fakeBlob.Get(ctx, node, req)
}

var _ BlobClient = (*uniformlySlowBlob)(nil)

// A straggler is only a straggler next to a peer that has already answered.
// Timed from the request instead, the same delay abandons every shard of a
// uniformly slow cluster at once and fails a read that was only going to be
// slow -- and there is no parity left to rebuild what it gave up on.
func TestAUniformlySlowClusterIsWaitedOutRatherThanAbandoned(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(3, 2)
	want := randomBytes(t, 1<<16)

	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")
	place, _, err := f.write(ctx, objectHash, bytes.NewReader(want), int64(len(want)))
	require.NoError(t, err)

	slow := &uniformlySlowBlob{fakeBlob: f.bc, delay: 2 * hedgeDelay}
	got, degraded, err := readObject(ctx, slow, f.cfg, "b", "k", place, place.Size, 0)

	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Zero(t, degraded, "nothing was lost, so nothing should have been rebuilt")
	assert.LessOrEqual(t, slow.gets.Load(), int64(f.cfg.DataShards),
		"parity was read for a cluster that was merely slow")
}

// An object that will not assemble has to fail, not return short. The read has
// no header to take back here, so the caller gets an error rather than bytes.
func TestAReadThatCannotAssembleFails(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	want := randomBytes(t, 3*streamBlockSize)

	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")
	place, _, err := f.write(ctx, objectHash, bytes.NewReader(want), int64(len(want)))
	require.NoError(t, err)

	// One data shard dies part way through and the parity that would replace it
	// will not serve at all, which is one loss more than RS(2,1) can carry.
	dying := &dyingBlob{fakeBlob: f.bc, dying: map[config.NodeID]int64{
		place.DataShardNodes[0]:   streamBlockSize,
		place.ParityShardNodes[0]: 0,
	}}

	_, _, err = readObject(ctx, dying, f.cfg, "b", "k", place, place.Size, 0)
	require.Error(t, err, "an object that cannot be assembled must not come back short")
}

// The reader is the only thing that knows how much of an object is resident at
// once, so this is where the streaming property can be stated exactly: the
// working set is a block per shard, whatever the object's size.
func TestTheWorkingSetIsAFixedNumberOfBlocks(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	for _, size := range []int64{1 << 16, 3*streamBlockSize + 11, 9 * streamBlockSize} {
		t.Run(fmt.Sprintf("size-%d", size), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			objectHash := model.ObjectHash("b", strconv.FormatInt(size, 10))
			place, _, err := f.write(ctx, objectHash, bytes.NewReader(randomBytes(t, int(size))), size)
			require.NoError(t, err)

			r, err := newStripeReader(ctx, f.bc, f.cfg, objectHash, place, 0)
			require.NoError(t, err)
			defer r.close(ctx)

			var resident int64
			for _, b := range r.blocks.held {
				if b != nil {
					resident += int64(cap(*b))
				}
			}
			assert.LessOrEqualf(t, resident, int64(f.cfg.TotalShards())*streamBlockSize,
				"a %d byte object holds %d bytes resident", size, resident)

			// Tighter than the bound above, and the reason for it: a healthy
			// read opens k shards, so it must not be holding a parity block.
			assert.LessOrEqualf(t, resident, int64(f.cfg.DataShards)*streamBlockSize,
				"a healthy %d byte read took a parity block it never opened", size)
		})
	}
}

// pacedReader delivers a shard in chunks with a gap between them, and can be
// told to stop delivering entirely. Closing it unblocks a read waiting on the
// gap, which is what abandoning the shard has to be able to do.
type pacedReader struct {
	io.Reader

	gap    time.Duration
	after  int64 // bytes delivered before it stops entirely; -1 for never
	sent   int64
	closed chan struct{}
	once   sync.Once
}

func (r *pacedReader) Read(p []byte) (int, error) {
	if len(p) > pacedChunk {
		p = p[:pacedChunk]
	}
	if r.after >= 0 && r.sent >= r.after {
		<-r.closed

		return 0, errors.New("shard stopped delivering")
	}
	select {
	case <-r.closed:
		return 0, errors.New("shard closed")
	case <-time.After(r.gap):
	}
	n, err := r.Reader.Read(p)
	r.sent += int64(n)

	return n, err
}

func (r *pacedReader) Close() error {
	r.once.Do(func() { close(r.closed) })

	return nil
}

const pacedChunk = 64 << 10

// pacingBlob paces one node's shard.
type pacingBlob struct {
	*fakeBlob

	slow  config.NodeID
	gap   time.Duration
	after int64
}

func (b *pacingBlob) Get(ctx context.Context, node config.NodeID, req blob.GetRequest) (io.ReadCloser, error) {
	rc, err := b.fakeBlob.Get(ctx, node, req)
	if err != nil || node != b.slow {
		return rc, err
	}

	return &pacedReader{Reader: rc, gap: b.gap, after: b.after, closed: make(chan struct{})}, nil
}

var _ BlobClient = (*pacingBlob)(nil)

// The case the whole hedge policy exists to protect. A shard delivering
// steadily but slowly takes far longer than hedgeDelay to finish a block, and
// must not be given up on: judged on elapsed time, every shard of a large
// object looks late and parity is fetched on every read of a healthy cluster.
func TestASlowButProgressingShardIsNotHedged(t *testing.T) {
	t.Parallel()

	const size = 2 * streamBlockSize
	f := newWriteFixture(2, 1)
	want := randomBytes(t, size)

	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")
	place, _, err := f.write(ctx, objectHash, bytes.NewReader(want), size)
	require.NoError(t, err)

	// A 4 MiB block in 64 KiB chunks 5 ms apart takes 320 ms, comfortably past
	// hedgeDelay, while never going idle for more than 5 ms.
	paced := &pacingBlob{fakeBlob: f.bc, slow: place.DataShardNodes[1], gap: 5 * time.Millisecond, after: -1}
	watched := &openWatcher{BlobClient: paced}

	start := time.Now()
	got, degraded, err := readObject(ctx, watched, f.cfg, "b", "k", place, size, 0)
	elapsed := time.Since(start)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	require.Greater(t, elapsed, hedgeDelay,
		"the read finished inside hedgeDelay, so it never entered the regime this is about")
	assert.Zero(t, degraded, "a shard that kept delivering was given up on")

	for _, req := range watched.seen {
		assert.Less(t, int(req.Index), f.cfg.DataShards,
			"parity shard %d was fetched for a read that was only slow", req.Index)
	}
}

// A stream that opened and then said nothing is not slow, it is not working.
// Its peer has already answered, so there is something to judge it against.
func TestAShardThatDeliversNothingIsHedged(t *testing.T) {
	t.Parallel()

	const size = 2 * streamBlockSize
	f := newWriteFixture(2, 1)
	want := randomBytes(t, size)

	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")
	place, _, err := f.write(ctx, objectHash, bytes.NewReader(want), size)
	require.NoError(t, err)

	silent := &pacingBlob{fakeBlob: f.bc, slow: place.DataShardNodes[1], after: 0}

	start := time.Now()
	got, degraded, err := readObject(ctx, silent, f.cfg, "b", "k", place, size, 0)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Positive(t, degraded, "a silent shard should have been replaced by parity")
	assert.Less(t, time.Since(start), shardStallWindow,
		"a silent shard was waited on for the stall window rather than the first-byte budget")
}

// A stream that was delivering and then stopped is the case an elapsed-time
// hedge and a progress hedge disagree about most sharply: it looks healthy
// right up until it does not.
func TestAShardThatStopsMidBlockIsHedged(t *testing.T) {
	t.Parallel()

	const size = 2 * streamBlockSize
	f := newWriteFixture(2, 1)
	want := randomBytes(t, size)

	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")
	place, _, err := f.write(ctx, objectHash, bytes.NewReader(want), size)
	require.NoError(t, err)

	stalling := &pacingBlob{fakeBlob: f.bc, slow: place.DataShardNodes[1], after: pacedChunk}

	got, degraded, err := readObject(ctx, stalling, f.cfg, "b", "k", place, size, 0)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Positive(t, degraded, "a shard that stopped delivering should have been replaced")
}
