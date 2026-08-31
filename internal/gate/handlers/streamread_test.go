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

	got, degraded, err := readObject(ctx, watched, f.cfg, "b", "k", place, size, 0, frozenClock())
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

// uniformlySlowBlob holds every open until all of them have arrived, then bills
// the whole cluster one delay and releases them together. That is what uniform
// slowness is: a wait every node spends at the same time, not one after another.
type uniformlySlowBlob struct {
	*fakeBlob

	clk   *testClock
	delay time.Duration
	want  int

	mu      sync.Mutex
	arrived int
	release chan struct{}
	gets    atomic.Int64
}

func newUniformlySlowBlob(inner *fakeBlob, clk *testClock, want int, delay time.Duration) *uniformlySlowBlob {
	return &uniformlySlowBlob{
		fakeBlob: inner, clk: clk, delay: delay, want: want, release: make(chan struct{}),
	}
}

func (b *uniformlySlowBlob) Get(ctx context.Context, node config.NodeID, req blob.GetRequest) (io.ReadCloser, error) {
	b.gets.Add(1)
	b.mu.Lock()
	b.arrived++
	last := b.arrived == b.want
	b.mu.Unlock()
	if last {
		b.clk.Advance(b.delay)
		close(b.release)
	}
	select {
	case <-b.release:
	case <-time.After(pacerWait):
	}

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

	// Two hedge delays are spent before any shard answers, so a hedge armed at
	// the request rather than at the first answer would have fired on all of them.
	clk := newTestClock()
	slow := newUniformlySlowBlob(f.bc, clk, f.cfg.DataShards, 2*hedgeDelay)
	got, degraded, err := readObject(ctx, slow, f.cfg, "b", "k", place, place.Size, 0, withClock(clk))

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

	_, _, err = readObject(ctx, dying, f.cfg, "b", "k", place, place.Size, 0, frozenClock())
	require.Error(t, err, "an object that cannot be assembled must not come back short")
}

// The reader is the only thing that knows how much of an object is resident at
// once, so this is where the streaming property can be stated exactly: the
// working set is a block per shard, whatever the object's size.
func TestTheWorkingSetIsAFixedNumberOfBlocks(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	sizes := []int64{1 << 16, 1 << 20, 3*streamBlockSize + 11, 9 * streamBlockSize}
	for _, size := range sizes {
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

			// Exactly a block per data shard, and the block is the object's own,
			// not the pooled one. A read that took a pooled buffer for a small
			// object would hold 4 MiB a shard for a few KiB of data, which is how
			// a hundred concurrent small GETs come to hold a gigabyte.
			want := int64(f.cfg.DataShards) * r.lay.blockSize
			assert.Equalf(t, want, resident,
				"a %d byte object with a %d byte block holds %d bytes resident",
				size, r.lay.blockSize, resident)
		})
	}
}

// The pool is what makes a small object dangerous: a buffer sized for the
// largest block, taken per shard per request, held for the life of the request.
// Concurrency multiplies it, so the bound has to hold across requests too.
func TestConcurrentSmallReadsDoNotHoldPooledBlocks(t *testing.T) {
	t.Parallel()

	const readers = 64
	f := newWriteFixture(2, 1)
	ctx := context.Background()

	for _, size := range []int64{64 << 10, 1 << 20} {
		t.Run(fmt.Sprintf("size-%d", size), func(t *testing.T) {
			t.Parallel()
			objectHash := model.ObjectHash("c", strconv.FormatInt(size, 10))
			place, _, err := f.write(ctx, objectHash, bytes.NewReader(randomBytes(t, int(size))), size)
			require.NoError(t, err)

			open := make([]*stripeReader, readers)
			for i := range open {
				r, rErr := newStripeReader(ctx, f.bc, f.cfg, objectHash, place, 0)
				require.NoError(t, rErr)
				defer r.close(ctx)
				open[i] = r
			}

			var resident int64
			for _, r := range open {
				for _, b := range r.blocks.held {
					if b != nil {
						resident += int64(cap(*b))
					}
				}
			}
			want := int64(readers*f.cfg.DataShards) * open[0].lay.blockSize
			assert.Equalf(t, want, resident,
				"%d concurrent %d byte reads hold %d bytes, not %d",
				readers, size, resident, want)
		})
	}
}

// holdingBlob answers one node and holds the others open, so a test can cancel
// a read while it is still gathering its shards.
type holdingBlob struct {
	*fakeBlob

	answer config.NodeID
	opened chan struct{}
	block  chan struct{}
	closes atomic.Int64
}

func (b *holdingBlob) Get(ctx context.Context, node config.NodeID, req blob.GetRequest) (io.ReadCloser, error) {
	// Held nodes ignore cancellation deliberately: if they gave up when the
	// context did, the constructor could collect every open and return a reader
	// before it noticed, and the path under test would not be taken.
	if node != b.answer {
		<-b.block

		return nil, errors.New("node never answered")
	}
	rc, err := b.fakeBlob.Get(ctx, node, req)
	if err != nil {
		return nil, err
	}
	select {
	case b.opened <- struct{}{}:
	default:
	}

	return &countingCloser{ReadCloser: rc, closes: &b.closes}, nil
}

var _ BlobClient = (*holdingBlob)(nil)

type countingCloser struct {
	io.ReadCloser

	closes *atomic.Int64
}

func (c *countingCloser) Close() error {
	c.closes.Add(1)

	return c.ReadCloser.Close()
}

// A read cancelled while it is still opening owns streams nobody else will ever
// see. They have to be closed on the way out, or a client that walked away
// leaves a shard stream open on a node for as long as the node tolerates it.
func TestAnOpenCancelledMidFlightClosesWhatItOpened(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(3, 2)
	want := randomBytes(t, 1<<16)

	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")
	place, _, err := f.write(ctx, objectHash, bytes.NewReader(want), int64(len(want)))
	require.NoError(t, err)

	held := &holdingBlob{
		fakeBlob: f.bc, answer: place.DataShardNodes[0],
		opened: make(chan struct{}, 1), block: make(chan struct{}),
	}

	// The clock never moves, so the straggler timer cannot fire and let the
	// construction succeed: cancellation is the only way this read ends.
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-held.opened
		cancel()
	}()

	_, err = newStripeReader(rctx, held, f.cfg, objectHash, place, 0, withClock(newTestClock()))
	require.ErrorIs(t, err, context.Canceled)
	close(held.block)

	assert.Eventually(t, func() bool { return held.closes.Load() == 1 }, time.Second, time.Millisecond,
		"the one stream that opened was left behind by the cancelled read")
}

// The other half of giving up: the working set goes back to the pool rather
// than waiting on a collection, and nothing is left pointing at it.
func TestAbandoningAReaderGivesBackItsWorkingSet(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	want := randomBytes(t, 1<<16)

	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")
	place, _, err := f.write(ctx, objectHash, bytes.NewReader(want), int64(len(want)))
	require.NoError(t, err)

	r, err := newStripeReader(ctx, f.bc, f.cfg, objectHash, place, 0)
	require.NoError(t, err)
	require.NotNil(t, r.blocks.held[0], "the data blocks are taken up front")

	r.abandon(0, make(chan openResult))

	for i, b := range r.blocks.held {
		assert.Nilf(t, b, "block %d was still held after abandoning the reader", i)
	}
	for i, rc := range r.src {
		assert.Nilf(t, rc, "stream %d was still open after abandoning the reader", i)
	}
}

// pacedReader delivers a shard in chunks against the virtual clock, and can be
// told to stop delivering entirely. Each chunk costs gap, so the shard's
// slowness is stated by the test; closing it unblocks a read waiting on a gap,
// which is what abandoning the shard has to be able to do.
type pacedReader struct {
	io.Reader

	pacer *shardPacer
	index int
	gap   time.Duration
	after int64 // bytes delivered before it stops entirely; -1 for never

	// budget caps the virtual time this shard may run the clock forward once it
	// has stopped, so the move that convicts it can never reach a peer.
	budget time.Duration

	sent   int64
	closed chan struct{}
	once   sync.Once
	stalls sync.Once
}

func (r *pacedReader) Read(p []byte) (int, error) {
	if len(p) > pacedChunk {
		p = p[:pacedChunk]
	}
	if r.after >= 0 && r.sent >= r.after {
		r.stalls.Do(r.runOutTheClock)
		<-r.closed

		return 0, errors.New("shard stopped delivering")
	}
	select {
	case <-r.closed:
		return 0, errors.New("shard closed")
	default:
	}
	r.pacer.awaitPeersFed(r.index)
	r.pacer.clk.Advance(r.gap)
	n, err := r.Reader.Read(p)
	r.sent += int64(n)

	return n, err
}

// runOutTheClock moves time forward while this shard delivers nothing, so the
// stripe loop's probe fires and judges it. Each step waits for every peer to be
// between blocks, so the only shard a step can convict is this one.
func (r *pacedReader) runOutTheClock() {
	go func() {
		for spent := time.Duration(0); spent < r.budget; spent += hedgeProbeInterval {
			select {
			case <-r.closed:
				return
			default:
			}
			r.pacer.awaitPeersIdle(r.index)
			r.pacer.clk.Advance(hedgeProbeInterval)
			time.Sleep(time.Millisecond)
		}
	}()
}

func (r *pacedReader) Close() error {
	r.once.Do(func() { close(r.closed) })

	return nil
}

const pacedChunk = 64 << 10

// pacingBlob paces one node's shard. Every other node answers at once, so the
// only thing that moves the clock is the shard the test is about.
type pacingBlob struct {
	*fakeBlob

	pacer  *shardPacer
	slow   config.NodeID
	gap    time.Duration
	after  int64
	budget time.Duration
}

func (b *pacingBlob) Get(ctx context.Context, node config.NodeID, req blob.GetRequest) (io.ReadCloser, error) {
	rc, err := b.fakeBlob.Get(ctx, node, req)
	if err != nil || node != b.slow {
		return rc, err
	}

	return &pacedReader{
		Reader: rc, pacer: b.pacer, index: int(req.Index),
		gap: b.gap, after: b.after, budget: b.budget, closed: make(chan struct{}),
	}, nil
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
	pacer := newShardPacer()
	paced := &pacingBlob{
		fakeBlob: f.bc, pacer: pacer, slow: place.DataShardNodes[1],
		gap: 5 * time.Millisecond, after: -1,
	}
	watched := &openWatcher{BlobClient: paced}

	start := pacer.clk.Now()
	got, degraded, err := readObject(ctx, watched, f.cfg, "b", "k", place, size, 0,
		withClock(pacer.clk), pacer.bind())
	elapsed := pacer.clk.Now().Sub(start)
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
//
// RS(3,2), because a hedge never spends the last parity unit and RS(2,1) has
// only one — see TestTheLastParityUnitIsNotSpentOnAHedge.
func TestAShardThatDeliversNothingIsHedged(t *testing.T) {
	t.Parallel()

	const size = 2 * streamBlockSize
	f := newWriteFixture(3, 2)
	want := randomBytes(t, size)

	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")
	place, _, err := f.write(ctx, objectHash, bytes.NewReader(want), size)
	require.NoError(t, err)

	// The budget is the whole point of the case: it is under the stall window,
	// so the read can only finish if the silent shard was convicted on the
	// first-byte budget instead.
	pacer := newShardPacer()
	silent := &pacingBlob{
		fakeBlob: f.bc, pacer: pacer, slow: place.DataShardNodes[1],
		after: 0, budget: shardStallWindow - hedgeProbeInterval,
	}

	start := pacer.clk.Now()
	got, degraded, err := readObject(ctx, silent, f.cfg, "b", "k", place, size, 0,
		withClock(pacer.clk), pacer.bind())
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Positive(t, degraded, "a silent shard should have been replaced by parity")
	assert.Less(t, pacer.clk.Now().Sub(start), shardStallWindow,
		"a silent shard was waited on for the stall window rather than the first-byte budget")
}

// A stream that was delivering and then stopped is the case an elapsed-time
// hedge and a progress hedge disagree about most sharply: it looks healthy
// right up until it does not.
func TestAShardThatStopsMidBlockIsHedged(t *testing.T) {
	t.Parallel()

	const size = 2 * streamBlockSize
	f := newWriteFixture(3, 2)
	want := randomBytes(t, size)

	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")
	place, _, err := f.write(ctx, objectHash, bytes.NewReader(want), size)
	require.NoError(t, err)

	// This one has delivered, so only the stall window can convict it and the
	// budget has to clear that.
	pacer := newShardPacer()
	stalling := &pacingBlob{
		fakeBlob: f.bc, pacer: pacer, slow: place.DataShardNodes[1],
		after: pacedChunk, budget: shardStallWindow + 4*hedgeProbeInterval,
	}

	got, degraded, err := readObject(ctx, stalling, f.cfg, "b", "k", place, size, 0,
		withClock(pacer.clk), pacer.bind())
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Positive(t, degraded, "a shard that stopped delivering should have been replaced")
}

// TestHedgeNeverSpendsTheLastParityUnit pins the rule that decides whether a
// slow read can become a failed one. A hedge is a guess about latency and a
// genuine failure is not, so the last unit is never spent on the guess.
//
// RS(2,1) is the case that matters: its only parity unit is also its last, so
// no hedge is affordable and a stalled shard falls to the blob client's idle
// guard instead. Every shipped multi-host config but 5host is RS(2,1).
func TestHedgeNeverSpendsTheLastParityUnit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		data, parity int
		want         int
	}{
		{"RS(2,1) can afford none", 2, 1, 0},
		{"RS(3,2) can afford one", 3, 2, 1},
		{"RS(7,3) can afford two", 7, 3, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := &stripeReader{
				total: tt.data + tt.parity,
				lay:   layout{dataShards: tt.data},
				dead:  make([]bool, tt.data+tt.parity),
				src:   make([]io.ReadCloser, tt.data+tt.parity),
			}
			assert.Equal(t, tt.parity, r.spareParity(), "every parity unit starts spare")
			assert.Equal(t, tt.want, r.hedgeBudget())
		})
	}

	// A parity unit already consumed by a real loss is not available to hedge
	// with either, which is what keeps the reserve a reserve.
	t.Run("a lost parity unit is not lent back to the hedge", func(t *testing.T) {
		t.Parallel()
		r := &stripeReader{
			total: 5, lay: layout{dataShards: 3},
			dead: make([]bool, 5), src: make([]io.ReadCloser, 5),
		}
		r.dead[4] = true
		assert.Equal(t, 1, r.spareParity())
		assert.Equal(t, 0, r.hedgeBudget(), "the survivor is the reserve, not a hedge budget")
	})
}

// A shard silent past the conviction window has stopped being slow and started
// being gone, so it is replaced even where no hedge was affordable. RS(2,1) is
// the case with no hedge budget at all, and it is the one that has to work:
// without this the read waits on the blob client's 30s idle timeout instead.
func TestASilentShardIsConvictedEvenWithNoHedgeBudget(t *testing.T) {
	t.Parallel()

	const size = 2 * streamBlockSize
	f := newWriteFixture(2, 1)
	want := randomBytes(t, size)

	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")
	place, _, err := f.write(ctx, objectHash, bytes.NewReader(want), size)
	require.NoError(t, err)

	// Enough budget to carry the silent shard past the conviction window; a
	// hedge would have fired far earlier and is deliberately not available.
	pacer := newShardPacer()
	silent := &pacingBlob{
		fakeBlob: f.bc, pacer: pacer, slow: place.DataShardNodes[1],
		after: 0, budget: shardConvictionWindow + 4*hedgeProbeInterval,
	}

	start := pacer.clk.Now()
	got, degraded, err := readObject(ctx, silent, f.cfg, "b", "k", place, size, 0,
		withClock(pacer.clk), pacer.bind())
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Positive(t, degraded, "a shard silent past the conviction window should have been replaced")
	assert.GreaterOrEqual(t, pacer.clk.Now().Sub(start), shardConvictionWindow,
		"it was replaced before the conviction window, so the reserve was spent on a hedge")
}
