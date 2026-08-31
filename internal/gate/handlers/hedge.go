package handlers

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/reedsolomon"
	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/telemetry"
)

// A ranged read that lands inside one block is served by exactly one node, so
// that node's slowest response is the client's response. There is no failover
// for slowness -- only for failure -- and a blob read that stalls is bounded
// only by blob.DefaultEnvelopeTimeout, ten seconds.
//
// That is long enough to be fatal upstream rather than merely slow. A guest
// volume admits sixteen requests in flight, so fifteen concurrent stalls leave
// one working slot and the volume drops to tens of IOPS while every node
// reports itself healthy.
//
// The redundancy to answer around it is already paid for: any DataShards of a
// stripe reconstruct it, so the same bytes can be had from the shards that are
// not slow. This starts that second read once the first looks stalled and
// takes whichever finishes.
//
// This is not the stripe reader's hedge and does not share its budget. That one
// abandons a shard to hedge, spending redundancy the read may still need, which
// is why it holds a parity unit back and so never fires at RS(2,1). This one
// only ever adds a read: the original stays in flight, nothing is abandoned,
// and the object survives exactly as much loss during the hedge as before it.
// A read costs bandwidth, not durability.

// hedgeFloor is the shortest a read may run before it is treated as stalled.
// Below this the hedge fires on ordinary scheduling jitter and doubles the
// cluster's read load to save nothing.
const hedgeFloor = 15 * time.Millisecond

// hedgeCeiling bounds the delay when recent reads have been slow, so a cluster
// that is uniformly loaded still hedges rather than inferring from its own
// slowness that stalling is normal.
const hedgeCeiling = 250 * time.Millisecond

// hedgeMultiple is how many times the recent mean a read must exceed before a
// second one is worth issuing. Read latency here is tightly clustered -- a
// local shard answers in ~0.2ms and a remote one in ~0.7ms -- while a stall is
// three orders of magnitude out, so a wide multiple still separates them
// cleanly and keeps the hedge on the tail where it belongs.
const hedgeMultiple = 20

// hedgeSmoothing weights each new observation in the per-node mean. Small
// enough that one slow read does not move the delay much, large enough that a
// node recovering is reflected within a few hundred reads.
const hedgeSmoothing = 0.02

// nodeLatency tracks a smoothed mean read duration per node, which is what the
// hedge delay is derived from. There is no configured delay: a fixed one is
// either below the jitter of a loaded cluster or above the stall it exists to
// catch, and which of those it is depends on hardware nobody edits a TOML for.
type nodeLatency struct {
	mean sync.Map // config.NodeID -> *atomic.Uint64, float64 seconds in bits
}

// observe folds one read duration into the node's mean.
func (n *nodeLatency) observe(node config.NodeID, d time.Duration) {
	v, _ := n.mean.LoadOrStore(node, new(atomic.Uint64))
	slot, ok := v.(*atomic.Uint64)
	if !ok {
		return
	}
	for {
		old := slot.Load()
		next := d.Seconds()
		if old != 0 {
			next = math.Float64frombits(old)*(1-hedgeSmoothing) + next*hedgeSmoothing
		}
		if slot.CompareAndSwap(old, math.Float64bits(next)) {
			return
		}
	}
}

// delay reports how long to wait on this node before hedging. A node with no
// history yet gets the floor, which is the conservative end: it hedges sooner
// than a measured node would, and only until a handful of reads have landed.
func (n *nodeLatency) delay(node config.NodeID) time.Duration {
	v, ok := n.mean.Load(node)
	if !ok {
		return hedgeFloor
	}
	slot, ok := v.(*atomic.Uint64)
	if !ok {
		return hedgeFloor
	}
	raw := slot.Load()
	if raw == 0 {
		return hedgeFloor
	}

	return min(max(time.Duration(math.Float64frombits(raw)*hedgeMultiple*float64(time.Second)),
		hedgeFloor), hedgeCeiling)
}

// shardLatency is process-wide. The delay is a property of how the nodes are
// answering, not of one request, and a per-request tracker would spend every
// read's history on that read alone and always return the floor.
var shardLatency = &nodeLatency{}

// rangeResult is one attempt at a range, from either arm of the hedge.
type rangeResult struct {
	data   []byte
	err    error
	hedged bool
}

// readRangeHedged serves a byte range that lives in one data shard, starting a
// reconstruction from the other shards if the owner has not answered within the
// hedge delay. Both arms run to completion or cancellation and the first usable
// answer wins, so a stalled node costs the hedge delay rather than the blob
// client's ten-second envelope.
func readRangeHedged(
	ctx context.Context, bc BlobClient, cfg Config, objectHash [32]byte,
	place ObjectToShardNodes, shardIdx int, at, length int64,
) ([]byte, error) {
	if shardIdx >= len(place.DataShardNodes) {
		return nil, fmt.Errorf("shard index %d out of range", shardIdx)
	}
	owner := place.DataShardNodes[shardIdx]

	// Cancelling releases whichever arm lost, so the blob stream it holds goes
	// back to the pool rather than running to the envelope timeout unread.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Buffered for both arms: the loser must be able to finish writing its
	// result and exit even though nothing reads it again.
	results := make(chan rangeResult, 2)

	go func() {
		data, err := readRangeFromSingleShard(ctx, bc, objectHash, place, shardIdx, at, length)
		results <- rangeResult{data: data, err: err}
	}()

	startHedge := func() {
		go func() {
			data, err := reconstructRange(ctx, bc, cfg, objectHash, place, shardIdx, at, length)
			results <- rangeResult{data: data, err: err, hedged: true}
		}()
	}

	hedge := time.NewTimer(shardLatency.delay(owner))
	defer hedge.Stop()

	// Both arms have to fail before the read does, so the first failure is
	// held: the arm still running may yet answer, and if it is not running
	// yet it is started rather than waited for.
	var (
		running  = 1
		firstErr error
	)
	for running > 0 {
		select {
		case <-hedge.C:
			running++
			startHedge()

		case res := <-results:
			running--
			if res.err == nil {
				if res.hedged {
					telemetry.RecordObjectRead(ctx, telemetry.ReadPathReconstructed)
					slog.DebugContext(ctx, "Hedged range read answered before its owner",
						"owner", owner, "index", shardIdx)
				}

				return res.data, nil
			}
			if firstErr == nil {
				firstErr = res.err
			}
			// The owner failed before the hedge was due. Nothing is in flight,
			// so start it now instead of waiting out a timer for a read that
			// has already lost.
			if running == 0 && !res.hedged && hedge.Stop() {
				running++
				startHedge()
			}

		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return nil, fmt.Errorf("range read failed on the owner and on reconstruction: %w", firstErr)
}

// reconstructRange rebuilds a range of one data shard from the others. Blocks
// sit at the same offset in every shard, so the same byte range read from each
// of them is one aligned slice of the code and decodes on its own -- there is
// no need to read, or hold, a whole stripe to answer four kilobytes.
func reconstructRange(
	ctx context.Context, bc BlobClient, cfg Config, objectHash [32]byte,
	place ObjectToShardNodes, shardIdx int, at, length int64,
) ([]byte, error) {
	nodes := place.AllNodes()
	if len(nodes) != cfg.TotalShards() {
		return nil, fmt.Errorf("placement names %d nodes, want %d", len(nodes), cfg.TotalShards())
	}

	enc, err := reedsolomon.New(cfg.DataShards, cfg.ParityShards)
	if err != nil {
		return nil, fmt.Errorf("erasure coder: %w", err)
	}

	var (
		mu     sync.Mutex
		wg     sync.WaitGroup
		shards = make([][]byte, len(nodes))
	)
	for i, node := range nodes {
		if i == shardIdx {
			continue
		}
		wg.Go(func() {
			data, rErr := readShardRange(ctx, bc, objectHash, node, i, at, length, place.WriteEpoch)
			if rErr != nil {
				return
			}
			mu.Lock()
			shards[i] = data
			mu.Unlock()
		})
	}
	wg.Wait()

	// Short reads cannot be decoded against full-length ones, and a shard whose
	// range ran past its end is one of those rather than a usable column.
	have := 0
	for i, s := range shards {
		if s == nil {
			continue
		}
		if int64(len(s)) != length {
			shards[i] = nil
			continue
		}
		have++
	}
	if have < cfg.DataShards {
		return nil, fmt.Errorf("reconstruct range: %d of %d shards answered", have, cfg.DataShards)
	}

	if err := enc.ReconstructData(shards); err != nil {
		return nil, fmt.Errorf("reconstruct range: %w", err)
	}

	return shards[shardIdx], nil
}

// readShardRange reads a byte range from one shard on one node. Unlike the
// data-shard read it takes the shard index and node directly, because a
// reconstruction reads parity shards, which the placement's data list does not
// name.
func readShardRange(
	ctx context.Context, bc BlobClient, objectHash [32]byte, node config.NodeID,
	index int, at, length int64, epoch uint64,
) (data []byte, err error) {
	start := time.Now()
	defer func() {
		recordShardOutcome(ctx, telemetry.ShardOpRead, node, start, err)
		if err == nil {
			shardLatency.observe(node, time.Since(start))
		}
	}()

	reader, err := bc.Get(ctx, node, blob.GetRequest{
		Key:        objectHash,
		Index:      uint32(index), //nolint:gosec // G115: index bounded by shard count (small uint).
		RangeStart: at,
		RangeEnd:   at + length - 1,
		Epoch:      epoch,
	})
	if err != nil {
		return nil, fmt.Errorf("get range from node %d: %w", node, err)
	}
	defer reader.Close() // CRITICAL: Close to release the stream back to the pool

	data, err = io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read range data: %w", err)
	}

	return data, nil
}
