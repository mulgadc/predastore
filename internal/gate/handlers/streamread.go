// The read used to buffer every shard and then join into a second buffer, so a
// GET peaked near twice the object and a large one took the process out. This
// reads a stripe at a time and reconstructs it before emitting it, so the
// working set is a fixed number of blocks and a shard lost part way through is
// still recoverable — the bytes it would have supplied have not been sent yet.

package handlers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/klauspost/reedsolomon"
	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
)

// stripeReader serves an object one stripe at a time from its shard streams.
//
// Parity is opened only when a data shard is missing, and opened at the offset
// the read has reached rather than from the start, so a healthy read costs k
// shards and a degraded one pays for parity only from the point it went wrong.
type stripeReader struct {
	bc         BlobClient
	objectHash [32]byte
	place      ObjectToShardNodes
	handoff    config.NodeID
	lay        layout
	enc        reedsolomon.Encoder
	total      int

	block  [][]byte // the whole working set: total buffers of one block
	stripe [][]byte // per-stripe view, zero length where a shard is missing
	src    []io.ReadCloser
	dead   []bool

	offset        int64
	failures      []shardFailure
	reconstructed int
}

func newStripeReader(
	ctx context.Context, bc BlobClient, cfg Config,
	objectHash [32]byte, place ObjectToShardNodes, handoff config.NodeID,
) (*stripeReader, error) {
	enc, err := reedsolomon.New(cfg.DataShards, cfg.ParityShards)
	if err != nil {
		return nil, err
	}

	total := cfg.TotalShards()
	lay := newLayout(cfg.DataShards, place.Size, place.BlockSize)
	r := &stripeReader{
		bc: bc, objectHash: objectHash, place: place, handoff: handoff,
		lay: lay, enc: enc, total: total,
		block:  make([][]byte, total),
		stripe: make([][]byte, total),
		src:    make([]io.ReadCloser, total),
		dead:   make([]bool, total),
	}
	for i := range r.block {
		r.block[i] = make([]byte, lay.blockSize)
	}

	// Only the data shards are opened. Opening parity here would double the
	// traffic of every healthy read for a fallback that is usually not needed.
	//
	// The opens are hedged like the reads that follow them. A node that accepts
	// a connection and then says nothing costs shardOpenTimeout, and paying five
	// seconds of that on every GET is exactly the availability loss parity is
	// there to prevent.
	nodes := place.AllNodes()
	opens := make(chan openResult, cfg.DataShards)
	for i := range cfg.DataShards {
		go func() {
			rc, openErr := r.open(ctx, i, nodes[i], 0)
			opens <- openResult{index: i, rc: rc, err: openErr}
		}()
	}

	// The clock starts at the first shard that answers, not at the request. A
	// straggler is only a straggler next to a peer that has already landed;
	// started at the request, the same delay would abandon every shard of a
	// uniformly slow cluster and fail a read that was merely going to be slow.
	var hedge *time.Timer
	var straggling <-chan time.Time
	opened := make([]bool, cfg.DataShards)
	waiting := cfg.DataShards
	for waiting > 0 {
		select {
		case res := <-opens:
			waiting--
			opened[res.index] = true
			if res.err != nil {
				r.fail(ctx, res.index, nodes[res.index], res.err, 0)

				continue
			}
			r.src[res.index] = res.rc
			if hedge == nil {
				hedge = time.NewTimer(hedgeDelay)
				straggling = hedge.C
				defer hedge.Stop()
			}
		case <-straggling:
			straggling = nil
			// Giving up on more shards than parity can replace would turn a slow
			// read into a failed one, so past that point waiting is the only move.
			if countFalse(opened) <= cfg.ParityShards-countTrue(r.dead[:cfg.DataShards]) {
				waiting = 0
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// An abandoned open still lands eventually, and the stream it returns is
	// nobody's to close by then, so a drain closes it rather than leaking it.
	if late := countFalse(opened); late > 0 {
		for i, ok := range opened {
			if !ok {
				r.fail(ctx, i, nodes[i], errShardTooSlow, hedgeDelay)
			}
		}
		go func() {
			for range late {
				if res := <-opens; res.rc != nil {
					_ = res.rc.Close()
				}
			}
		}()
	}

	return r, nil
}

// openResult is one shard's stream, or the reason there is not one.
type openResult struct {
	index int
	rc    io.ReadCloser
	err   error
}

func countFalse(b []bool) int {
	return len(b) - countTrue(b)
}

func countTrue(b []bool) int {
	n := 0
	for _, v := range b {
		if v {
			n++
		}
	}

	return n
}

// spareParity counts the parity shards still available to stand in for a data
// shard: neither already lost nor already carrying one.
func (r *stripeReader) spareParity() int {
	n := 0
	for i := r.lay.dataShards; i < r.total; i++ {
		if !r.dead[i] && r.src[i] == nil {
			n++
		}
	}

	return n
}

// open dials one shard from a byte offset. Only the open is bounded by a
// deadline; the body that follows is bounded by progress in the blob client's
// idle guard, because a total cap cannot tell a stalled node from a large
// shard. A shard its owner will not serve is looked for on the handoff node,
// since a write that could not reach the owner will have left it there.
func (r *stripeReader) open(ctx context.Context, index int, node config.NodeID, from int64) (io.ReadCloser, error) {
	dial := func(at config.NodeID) (io.ReadCloser, error) {
		ctx, cancel := context.WithCancel(ctx)
		opening := time.AfterFunc(shardOpenTimeout, cancel)
		req := blob.GetRequest{
			Key:        r.objectHash,
			RangeStart: -1, // -1 for both means the whole shard
			RangeEnd:   -1,
			Index:      uint32(index), //nolint:gosec // G115: index bounded by shard count.
			Epoch:      r.place.WriteEpoch,
		}
		if from > 0 {
			req.RangeStart, req.RangeEnd = from, r.lay.shardSize-1
		}
		rc, err := r.bc.Get(ctx, at, req)
		opening.Stop()
		if err != nil {
			cancel()
			return nil, err
		}

		return &cancelOnClose{ReadCloser: rc, cancel: cancel}, nil
	}

	rc, err := dial(node)
	if err == nil {
		return rc, nil
	}
	recordShardReadError(ctx, err)
	if r.handoff != 0 && r.handoff != node {
		if held, hErr := dial(r.handoff); hErr == nil {
			slog.InfoContext(ctx, "Shard served from its handoff holder",
				"owner", node, "holder", r.handoff, "index", index)

			return held, nil
		}
	}

	return nil, err
}

// cancelOnClose releases the context that bounded the open when the stream is
// closed, so abandoning a shard does not leak its cancel func.
type cancelOnClose struct {
	io.ReadCloser

	cancel context.CancelFunc
}

func (c *cancelOnClose) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()

	return err
}

func (r *stripeReader) fail(ctx context.Context, index int, node config.NodeID, err error, took time.Duration) {
	if r.dead[index] {
		return
	}
	r.dead[index] = true
	if r.src[index] != nil {
		_ = r.src[index].Close()
		r.src[index] = nil
	}
	logShardFailure(ctx, node, index, err, took)
	r.failures = append(r.failures, shardFailure{index: index, node: node, reason: shardErrorReason(err)})
}

// blockResult is one shard's contribution to a stripe.
type blockResult struct {
	index int
	err   error
	took  time.Duration
}

// next returns the data blocks of the next stripe. It reports io.EOF once the
// object is exhausted. The returned slices are reused by the following call.
//
// The stripe is hedged: if the data blocks have not all arrived within
// hedgeDelay, parity is opened to cover the shortfall. At block granularity an
// absolute delay is the right bound -- a 4 MiB block is tens of milliseconds on
// a healthy node whatever the object's size -- which is not true of the same
// constant applied to a whole shard.
//
// A hedged shard is abandoned, not raced. A shard stream is sequential, so
// skipping one stripe's block would leave it at the wrong offset for the next.
// It is closed and left out of the rest of this read, and parity carries the
// object from here. A node that stalls once is demoted for one object, which is
// the price of not holding the shard in order to be able to rewind it.
func (r *stripeReader) next(ctx context.Context) ([][]byte, int64, error) {
	if r.offset >= r.lay.shardSize {
		return nil, 0, io.EOF
	}
	n := min(r.lay.blockSize, r.lay.shardSize-r.offset)
	k := r.lay.dataShards
	nodes := r.place.AllNodes()
	for i := range r.stripe {
		r.stripe[i] = nil
	}

	// Parity opened for an earlier stripe is read again here rather than opened
	// again: the stream is already at the right offset, and skipping it would
	// leave the stripe short of the shard that is standing in for a lost one.
	results := make(chan blockResult, r.total)
	inflight := make([]bool, r.total)
	pending, have := 0, 0
	for i := range r.total {
		if r.src[i] == nil {
			continue
		}
		inflight[i] = true
		pending++
		r.readBlock(ctx, i, n, results)
	}
	// A shard already lost, either at open or on an earlier stripe, is covered
	// before the stripe starts rather than after waiting out a read for it.
	pending += r.openParity(ctx, n, nodes, inflight, results, k-pending)

	// As with the opens, the hedge is armed by the first block to land, so a
	// stripe that is slow everywhere is waited out rather than abandoned.
	var hedge *time.Timer
	var straggling <-chan time.Time

	for pending > 0 {
		select {
		case res := <-results:
			pending--
			inflight[res.index] = false
			if res.err != nil {
				r.fail(ctx, res.index, nodes[res.index], res.err, res.took)
				pending += r.openParity(ctx, n, nodes, inflight, results, k-have-pending)

				continue
			}
			if res.took > slowShardThreshold {
				slog.WarnContext(ctx, "Shard block was slow but landed",
					"node", nodes[res.index], "index", res.index,
					"offset", r.offset, "read_ms", res.took.Milliseconds())
			}
			r.stripe[res.index] = r.block[res.index][:n]
			have++
			if hedge == nil {
				hedge = time.NewTimer(hedgeDelay)
				straggling = hedge.C
				defer hedge.Stop()
			}
		case <-straggling:
			straggling = nil
			// Closing the stream unblocks its read, so a straggler arrives back
			// through the error path and is replaced there like any other loss.
			// Only as many as parity can still replace are given up on.
			budget := r.spareParity()
			for i := range k {
				if inflight[i] && budget > 0 {
					r.fail(ctx, i, nodes[i], errShardTooSlow, hedgeDelay)
					budget--
				}
			}
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		}
	}

	// A zero-length entry is how the encoder is told a shard is absent, and it
	// keeps the buffer's capacity so the reconstruction writes back into the
	// working set instead of allocating a block per stripe.
	missing := 0
	for i := range r.total {
		if r.stripe[i] == nil {
			r.stripe[i] = r.block[i][:0]
			if i < k {
				missing++
			}
		}
	}
	if missing > 0 {
		if err := r.enc.ReconstructData(r.stripe); err != nil {
			return nil, 0, fmt.Errorf("reconstruct stripe at %d: %w", r.offset, err)
		}
		r.reconstructed = max(r.reconstructed, missing)
	}

	r.offset += n

	return r.stripe[:k], n, nil
}

// readBlock pulls one block from a shard. The stream is captured before the
// goroutine starts, so abandoning the shard closes a reader this call already
// holds rather than racing a nil.
func (r *stripeReader) readBlock(ctx context.Context, index int, n int64, out chan<- blockResult) {
	src, buf := r.src[index], r.block[index][:n]
	go func() {
		start := time.Now()
		_, err := io.ReadFull(src, buf)
		select {
		case out <- blockResult{index: index, err: err, took: time.Since(start)}:
		case <-ctx.Done():
		}
	}()
}

// openParity starts up to short parity reads for the stripe in hand. Parity is
// opened at the offset the read has reached, so a shard lost late in an object
// costs a stream from there rather than from the beginning.
func (r *stripeReader) openParity(
	ctx context.Context, n int64, nodes []config.NodeID,
	inflight []bool, results chan<- blockResult, short int,
) int {
	started := 0
	for i := r.lay.dataShards; i < r.total && started < short; i++ {
		if r.src[i] != nil || r.dead[i] {
			continue
		}
		rc, err := r.open(ctx, i, nodes[i], r.offset)
		if err != nil {
			r.fail(ctx, i, nodes[i], err, 0)

			continue
		}
		r.src[i] = rc
		inflight[i] = true
		started++
		r.readBlock(ctx, i, n, results)
	}

	return started
}

// errShardTooSlow names a shard that was still answering but had not delivered
// its block in time. It is not the same fault as a refused connection, which is
// why it is classified apart from one.
var errShardTooSlow = errors.New("shard did not deliver its block within the hedge delay")

func (r *stripeReader) close() {
	for i, rc := range r.src {
		if rc != nil {
			_ = rc.Close()
			r.src[i] = nil
		}
	}
}

// windowWriter serves a byte range out of a whole-object stream: it drops the
// first skip bytes and stops after limit. A negative limit means no limit.
//
// It reports the full length as written even where it dropped bytes, so the
// caller's accounting stays in object offsets rather than emitted ones.
type windowWriter struct {
	dst   io.Writer
	skip  int64
	limit int64
}

func (w *windowWriter) Write(p []byte) (int, error) {
	full := len(p)
	if w.skip > 0 {
		drop := min(int64(len(p)), w.skip)
		p = p[drop:]
		w.skip -= drop
	}
	if w.limit >= 0 && int64(len(p)) > w.limit {
		p = p[:w.limit]
	}
	if len(p) == 0 {
		return full, nil
	}
	if _, err := w.dst.Write(p); err != nil {
		return 0, err
	}
	if w.limit >= 0 {
		w.limit -= int64(len(p))
	}

	return full, nil
}

// drain emits the object from the stripe already in hand and then the rest,
// stopping at the object size so the padding that squares the last stripe is
// never served. The first stripe is passed in because the caller has to read it
// before it can send a header it cannot take back.
func drain(ctx context.Context, r *stripeReader, dst io.Writer, first [][]byte, n int64, size int64) error {
	var pos int64
	blocks, count := first, n
	for {
		for i := range blocks {
			if pos >= size {
				break
			}
			chunk := blocks[i][:count]
			if pos+int64(len(chunk)) > size {
				chunk = chunk[:size-pos]
			}
			if _, wErr := dst.Write(chunk); wErr != nil {
				return wErr
			}
			pos += int64(len(chunk))
		}
		if pos >= size {
			return nil
		}
		next, n, err := r.next(ctx)
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("object ended after %d of %d bytes", pos, size)
		}
		if err != nil {
			return err
		}
		blocks, count = next, n
	}
}
