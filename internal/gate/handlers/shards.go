package handlers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/reedsolomon"
	"github.com/mulgadc/bluebottle/pkg/sigv4"
	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/blob/engine"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/chunked"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/gate/placement"
	"github.com/mulgadc/predastore/internal/telemetry"
)

// ObjectToShardNodes maps an object to its shard locations. The node ids are
// the record of where the shards physically went, not a cache of what the ring
// would derive today: the read path dials these, so they are what survives a
// ring whose tuning changed under them.
type ObjectToShardNodes struct {
	Size       int64
	WriteEpoch uint64
	BlockSize  int64

	// Timestamped reports whether WriteEpoch carries a time. Version 1 records
	// hold a random epoch instead, so they have no modification time and never
	// will.
	Timestamped bool

	// Digest is the object's content MD5, or nil. A record written before
	// version 3 carries none, and neither does a multipart part's: the part's
	// ETag lives in the parts table instead.
	Digest []byte

	// PartCount is how many multipart parts Digest was composed from. Zero
	// means Digest is a plain content digest rather than a composite one; it
	// is meaningless when Digest is nil.
	PartCount int

	DataShardNodes   []config.NodeID
	ParityShardNodes []config.NodeID
}

// AllNodes returns the shard nodes in shard order: data first, then parity, so
// index i in the result is the node holding shard i.
func (p ObjectToShardNodes) AllNodes() []config.NodeID {
	nodes := make([]config.NodeID, 0, len(p.DataShardNodes)+len(p.ParityShardNodes))
	nodes = append(nodes, p.DataShardNodes...)

	return append(nodes, p.ParityShardNodes...)
}

// shardWriteOutcome captures the result of writing a shard to a blob node.
type shardWriteOutcome struct {
	shardIndex   int
	poolNearFull bool // mirrors PutResponse.PoolNearFull for this shard's node.
	err          error
}

// writeResult is what a write left on the cluster. It carries the landed set
// rather than a count because commit, abort and the degraded signal each need
// to know which positions, not how many: committing a shard that was never
// prepared is noise, and aborting one is a request to discard something else's
// work.
type writeResult struct {
	poolNearFull bool
	landed       []bool

	// holders is where shard i actually went — its placement node, or the
	// handoff node when the owner would not take it. Commit and abort follow
	// this rather than the record, because a shard handed off is not on the
	// node the record names.
	holders []config.NodeID

	// missing names the owners whose shard landed nowhere at all, and handoff
	// the positions that landed away from home. A position is in one or the
	// other or neither, never both.
	missing []config.NodeID
	handoff []int

	// ambiguous names the positions whose node took the whole body and then
	// did not report. The shard may be prepared there or may not, so it counts
	// toward neither the floor nor the degraded signal, and the commit that
	// follows the record settles it either way.
	ambiguous []int
}

func (r writeResult) landedCount() int {
	n := 0
	for _, ok := range r.landed {
		if ok {
			n++
		}
	}

	return n
}

// degraded reports whether the write went out at less than full width. The
// object is durable either way; what is reduced is how many further losses it
// survives until repair restores the missing shards.
//
// A shard that was handed off is not missing: the stripe is complete, just not
// where the record says, and both the read path and repair look there.
func (r writeResult) degraded() bool { return len(r.missing) > 0 }

// fullWidth is the result of a write with no shards to place, which is an empty
// object: nothing is missing because nothing was owed, and there is nothing on
// any node to commit or abort.
func fullWidth(total int) writeResult {
	landed := make([]bool, total)
	for i := range landed {
		landed[i] = true
	}

	return writeResult{landed: landed}
}

// mapPutErr translates a shard-write error into the S3 error returned to the
// client. A pool-full shard write must surface as 507, not the generic 500
// other failures get.
//
// The body is read through the decoder as the shards are written, so a framing
// or signature failure arrives here rather than at the end of the body. Those
// are the client's fault and must not be reported as ours.
func mapPutErr(err error) *model.S3Error {
	if errors.Is(err, engine.ErrStoreFull) {
		return model.ErrInsufficientStorageError
	}
	if s3err, ok := model.IsS3Error(mapChunkedErr(err)); ok && !isInternal(s3err) {
		return s3err
	}
	return model.NewS3Error(model.ErrInternalError, err.Error(), 500)
}

// isInternal reports whether an S3 error is the generic server fault, which is
// mapChunkedErr's answer for anything it does not recognise.
func isInternal(err *model.S3Error) bool {
	return err.Code == model.ErrInternalError
}

// writeFailureReason classifies a failed object write into one of the bounded
// reasons the counter carries, drawing the same line mapPutErr draws for the
// client: capacity is its own outcome, everything else is one bucket.
//
// A body the client malformed is not a shard-write failure, and counting it as
// one makes the storage-failure metric fire on client errors.
func writeFailureReason(err error) string {
	switch {
	case errors.Is(err, engine.ErrStoreFull):
		return telemetry.WriteReasonStoreFull
	case errors.Is(err, sigv4.ErrContentSHA256Mismatch),
		errors.Is(err, chunked.ErrChunkSignature),
		errors.Is(err, chunked.ErrMalformedFraming):
		return telemetry.WriteReasonBadRequest
	default:
		return telemetry.WriteReasonShardWrite
	}
}

// placeShards resolves where an object's shards live, in the ring order
// writeObject writes them in. Recording the placement is what makes the
// object retrievable, so the two must derive from the same object hash.
// The epoch is minted here, once, before any shard is written, and the same
// value goes to every shard and into the record. A retry mints a new one: two
// attempts must never be mistaken for one complete write.
func placeShards(ring *placement.Ring, cfg Config, objectHash [32]byte, size int64) (ObjectToShardNodes, error) {
	nodes, err := ring.Nodes(objectHash, cfg.TotalShards())
	if err != nil {
		return ObjectToShardNodes{}, err
	}

	epoch, err := cfg.Epochs.Next()
	if err != nil {
		return ObjectToShardNodes{}, err
	}

	return ObjectToShardNodes{
		Size:             size,
		WriteEpoch:       epoch,
		Timestamped:      true,
		BlockSize:        writeLayout(cfg.DataShards, size).blockSize,
		DataShardNodes:   append([]config.NodeID(nil), nodes[:cfg.DataShards]...),
		ParityShardNodes: append([]config.NodeID(nil), nodes[cfg.DataShards:]...),
	}, nil
}

// countingReader records how many bytes were actually read, so a body that
// stops short of its declared length can be told from a complete one once the
// stream has already been consumed.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// shardFailureReason classifies a shard failure into one of the bounded
// reasons the counters carry. The error text names a node and a key, so only
// the classification is recorded.
func shardFailureReason(err error) string {
	switch {
	case errors.Is(err, blob.ErrNotFound):
		return telemetry.ShardReasonNotFound
	case errors.Is(err, engine.ErrStoreFull):
		return telemetry.ShardReasonStoreFull
	default:
		return telemetry.ShardReasonTransport
	}
}

// recordShardOutcome counts one shard operation against one node with its
// duration, and adds an error under a bounded reason when it failed. Called
// once per shard per object, which is why the attribute sets behind it are
// cached rather than built per call.
func recordShardOutcome(ctx context.Context, op string, node config.NodeID, start time.Time, err error) {
	outcome := telemetry.OutcomeSuccess
	if err != nil {
		outcome = telemetry.OutcomeError
	}
	telemetry.RecordShardOp(ctx, op, outcome, uint64(node), time.Since(start).Seconds())
	if err != nil {
		telemetry.RecordShardError(ctx, op, shardFailureReason(err), uint64(node))
	}
}

// writeSingleShard streams the body to one node without staging it. The blob
// client reads at most size bytes, so an over-long body is truncated rather
// than overrunning the shard; a short one is caught here, because placement
// records size and a short value would read back as a corrupt object.
func writeSingleShard(
	ctx context.Context, bc BlobClient, node config.NodeID, body io.Reader, size int64,
	objectHash [32]byte, epoch uint64,
) (_ writeResult, err error) {
	start := time.Now()
	defer func() { recordShardOutcome(ctx, telemetry.ShardOpWrite, node, start, err) }()

	counted := &countingReader{r: body}

	resp, err := bc.Put(ctx, node, blob.PutRequest{Key: objectHash, Size: size, Index: 0, Epoch: epoch}, counted)
	if err != nil {
		return writeResult{landed: []bool{false}, missing: []config.NodeID{node}}, err
	}
	holders := []config.NodeID{node}
	if counted.n != size {
		return writeResult{landed: []bool{true}, holders: holders},
			fmt.Errorf("body delivered %d bytes, declared %d", counted.n, size)
	}

	return writeResult{poolNearFull: resp.PoolNearFull, landed: []bool{true}, holders: holders}, nil
}

// writeObject splits body into RS shards and sends each to the appropriate
// node. size is what the caller declared the body to be: the splitter needs it
// up front, and it is what placement records, so a body that does not deliver
// exactly that many bytes is an error rather than a short object.
// poolNearFull is set if any shard's target node reported pressure.
func writeObject(ctx context.Context, bc BlobClient, cfg Config, ring *placement.Ring, body io.Reader, size int64, objectHash [32]byte, place ObjectToShardNodes) (writeResult, error) {
	// An empty object has no shard to write: the blob protocol rejects a
	// zero-length value, and recorded placement is enough to serve the GET.
	if size == 0 {
		return fullWidth(cfg.TotalShards()), nil
	}

	// The nodes come from the placement that will be published, not from a
	// second ring lookup: the record is what the read path dials, so a write
	// that derived its own list could put shards where nothing looks for them.
	shardNodes := place.AllNodes()
	epoch := place.WriteEpoch

	// RS(1,0) is the whole object on one node: nothing to split and no parity
	// to encode. Streaming it straight through is what keeps a single-node
	// write off the heap, so it is its own path rather than a degenerate split.
	if cfg.DataShards == 1 && cfg.ParityShards == 0 {
		return writeSingleShard(ctx, bc, shardNodes[0], body, size, objectHash, epoch)
	}

	return streamShards(ctx, bc, cfg, objectHash, epoch, shardNodes, body,
		newLayout(cfg.DataShards, size, place.BlockSize), size,
		handoffNode(ring, cfg, objectHash))
}

// streamBlockSize is how much of each shard the gate holds at once. The whole
// working set is TotalShards blocks of it, so it sets the memory a write costs
// and nothing else does: the object itself is never resident.
//
// 4 MiB matches the encoder's own streaming block, which is where the
// throughput of the underlying Encode was tuned.
const streamBlockSize = 4 << 20

// zeroPadding supplies the tail of the last stripe. Reed-Solomon needs every
// shard the same length and an object rarely divides evenly into k of them, so
// the shortfall is zeros the reader never asks for: placement records the real
// size, and the read path stops there.
type zeroPadding struct{}

func (zeroPadding) Read(p []byte) (int, error) {
	clear(p)

	return len(p), nil
}

// streamShards encodes the body a stripe at a time and carries each shard to
// its node as it is produced, so the gate holds TotalShards blocks rather than
// the object.
//
// A shard whose node stops taking bytes does not fail the write. Its stream is
// abandoned and the loop keeps going, which leaves that position undurable and
// nothing else disturbed — the same missing shard degraded writes, handoff and
// repair already deal with. Failing the encode instead is what the buffered
// version had to avoid by holding every shard whole.
func streamShards(
	ctx context.Context, bc BlobClient, cfg Config,
	objectHash [32]byte, epoch uint64, shardNodes []config.NodeID,
	body io.Reader, lay layout, size int64, handoff config.NodeID,
) (writeResult, error) {
	total := cfg.TotalShards()
	shardSize, blockSize := lay.shardSize, lay.blockSize

	// What a write costs in memory is the working set, not the object: the
	// stripe loop holds TotalShards blocks and the body streams through it.
	defer telemetry.EnterGateInflight(ctx, telemetry.GateOpPut, int64(total)*blockSize)()

	enc, err := reedsolomon.New(cfg.DataShards, cfg.ParityShards)
	if err != nil {
		return writeResult{landed: make([]bool, total)}, err
	}

	// Every shard is written, so the whole set is taken up front. The pipes the
	// stripe is handed to are unbuffered, so a write returns only once its bytes
	// have been read out of the block and the next stripe may overwrite it.
	blocks := newBlockSet(total, blockSize)
	defer blocks.release()
	stripe := make([][]byte, total)

	streams := make([]*shardStream, total)
	for i := range total {
		streams[i] = startShardStream(ctx, bc, objectHash, epoch, i, shardNodes[i], shardSize)
	}

	// The body is padded so a short last stripe still encodes, and counted so a
	// body that does not deliver what it declared is an error rather than an
	// object with zeros on the end.
	counted := &countingReader{r: body}
	padded := io.MultiReader(counted, zeroPadding{})

	result := writeResult{landed: make([]bool, total), holders: make([]config.NodeID, total)}
	for offset := int64(0); offset < shardSize; offset += blockSize {
		n := min(blockSize, shardSize-offset)
		for i := range cfg.DataShards {
			stripe[i] = blocks.at(i)[:n]
			if _, readErr := io.ReadFull(padded, stripe[i]); readErr != nil {
				closeStreams(streams)
				return result, fmt.Errorf("read object body: %w", readErr)
			}
		}
		for i := cfg.DataShards; i < total; i++ {
			stripe[i] = blocks.at(i)[:n]
		}
		if encErr := enc.Encode(stripe); encErr != nil {
			closeStreams(streams)
			return result, fmt.Errorf("encode parity: %w", encErr)
		}

		refused := writeStripe(streams, stripe)
		if offset == 0 {
			handOff(ctx, bc, objectHash, epoch, streams, stripe, refused, handoff, shardSize)
		}
	}

	if counted.n != size {
		closeStreams(streams)

		return result, fmt.Errorf("body delivered %d bytes, declared %d", counted.n, size)
	}

	return collectStreams(ctx, cfg, streams, result)
}

// shardStream carries one shard to one node while the encoder is still
// producing the rest of it. The pipe is unbuffered, so a write to it is a
// write onto the wire, and the shards advance together rather than one
// completing before the next begins.
type shardStream struct {
	index  int
	owner  config.NodeID
	holder config.NodeID
	pw     *io.PipeWriter
	done   chan shardWriteOutcome
	failed bool
}

func startShardStream(
	ctx context.Context, bc BlobClient, objectHash [32]byte, epoch uint64,
	index int, node config.NodeID, shardSize int64,
) *shardStream {
	pr, pw := io.Pipe()
	s := &shardStream{
		index: index, owner: node, holder: node,
		pw: pw, done: make(chan shardWriteOutcome, 1),
	}

	go func() {
		resp, err := bc.Put(ctx, node, blob.PutRequest{
			Key:   objectHash,
			Size:  shardSize,
			Index: uint32(index), //nolint:gosec // G115: index bounded by the shard count.
			Epoch: epoch,
		}, pr)
		// Closing the read side is what releases a producer still writing into
		// a stream whose node has gone: io.Pipe has no buffer and does not
		// observe ctx, so without this the whole write hangs on one dead node.
		_ = pr.CloseWithError(io.ErrClosedPipe)

		if err != nil {
			s.done <- shardWriteOutcome{shardIndex: index, err: err}

			return
		}
		s.done <- shardWriteOutcome{shardIndex: index, poolNearFull: resp.PoolNearFull}
	}()

	return s
}

// write sends one block, reporting whether the stream is still alive. A stream
// that has already failed is a no-op rather than an error: the position is
// recorded once, and the encode carries on for the shards that remain.
func (s *shardStream) write(p []byte) bool {
	if s.failed {
		return false
	}
	if _, err := s.pw.Write(p); err != nil {
		s.failed = true
		_ = s.pw.CloseWithError(err)

		return false
	}

	return true
}

func (s *shardStream) abandon() {
	if !s.failed {
		s.failed = true
		_ = s.pw.CloseWithError(io.ErrClosedPipe)
	}
}

// writeStripe sends one block of every shard at once and names the positions
// whose node would not take it. Concurrently, because each write blocks until
// its node reads: in sequence, the slowest node would set the rate for all of
// them and the shards would no longer advance together.
func writeStripe(streams []*shardStream, stripe [][]byte) []int {
	var (
		mu      sync.Mutex
		refused []int
		wg      sync.WaitGroup
	)
	for i, s := range streams {
		if s.failed {
			continue
		}
		wg.Go(func() {
			if s.write(stripe[i]) {
				return
			}
			mu.Lock()
			refused = append(refused, i)
			mu.Unlock()
		})
	}
	wg.Wait()

	// Sorted so the handoff list and the log are in shard order rather than in
	// whichever order the concurrent writes happened to fail.
	slices.Sort(refused)

	return refused
}

// handOff restarts the shards their owner refused against the one node off the
// end of the stripe.
//
// It is only ever called for the first block, and that is the whole of what
// makes it possible: nothing is buffered, so a shard can only be re-aimed
// while none of it has been sent yet. A node that stops taking bytes part way
// through leaves that position missing, for repair to restore — which is the
// case handoff used to cover and now does not.
func handOff(
	ctx context.Context, bc BlobClient, objectHash [32]byte, epoch uint64,
	streams []*shardStream, stripe [][]byte, refused []int,
	handoff config.NodeID, shardSize int64,
) {
	if handoff == 0 {
		return
	}
	for _, i := range refused {
		if streams[i].owner == handoff {
			continue
		}
		alt := startShardStream(ctx, bc, objectHash, epoch, i, handoff, shardSize)
		alt.owner = streams[i].owner
		if !alt.write(stripe[i]) {
			<-alt.done
			logShardWriteFailure(ctx, handoff, i, io.ErrClosedPipe)

			continue
		}

		// The refused stream is already closed, so its outcome is waiting and
		// has to be taken or the goroutine holding it never retires.
		<-streams[i].done
		streams[i] = alt
		slog.InfoContext(ctx, "Shard handed off; its owner would not take it",
			"owner", alt.owner, "holder", handoff, "index", i,
			"epoch", fmt.Sprintf("%016x", epoch))
	}
}

// closeStreams abandons every stream and waits for its put to retire, so a
// write that gives up leaves no goroutine holding a pipe.
func closeStreams(streams []*shardStream) {
	for _, s := range streams {
		s.abandon()
		<-s.done
	}
}

// collectStreams closes the live streams, waits for every put to report, and
// assembles what the write left on the cluster.
func collectStreams(ctx context.Context, cfg Config, streams []*shardStream, result writeResult) (writeResult, error) {
	for _, s := range streams {
		if !s.failed {
			_ = s.pw.Close()
		}
	}

	var firstErr error
	for i, s := range streams {
		result.holders[i] = s.holder
		outcome := <-s.done
		if outcome.err != nil {
			if firstErr == nil {
				firstErr = outcome.err
			}
			logShardWriteFailure(ctx, s.holder, i, outcome.err)
			// The holder took the body and went quiet. Leave it named in the
			// record so a read finds the shard if it is there, and let the
			// commit phase decide; a refusal we never received is not one.
			if errors.Is(outcome.err, blob.ErrCommitUnknown) {
				result.ambiguous = append(result.ambiguous, i)

				continue
			}
			result.holders[i] = s.owner
			result.missing = append(result.missing, s.owner)

			continue
		}
		result.landed[i] = true
		if s.holder != s.owner {
			result.handoff = append(result.handoff, i)
		}
		if outcome.poolNearFull {
			result.poolNearFull = true
		}
	}

	if landed := result.landedCount(); landed < cfg.MinShards() {
		return result, fmt.Errorf("%w: %d of %d shards durable, nodes %v unreachable: %w",
			errShardWriteFloor, landed, cfg.MinShards(), result.missing, firstErr)
	}

	return result, nil
}

// handoffNode is the node one step off the end of the stripe on the ring: where
// a shard goes when its owner will not take it. It is derived and never
// recorded, which is what lets the read path and repair find it later without a
// hint to store. Zero means there is none — handoff is off, or the cluster has
// no node to spare.
func handoffNode(ring *placement.Ring, cfg Config, objectHash [32]byte) config.NodeID {
	if !cfg.HintedHandoff || ring == nil {
		return 0
	}
	nodes, err := ring.Nodes(objectHash, cfg.TotalShards()+1)
	if err != nil || len(nodes) <= cfg.TotalShards() {
		return 0
	}

	return nodes[cfg.TotalShards()]
}

// errShardWriteFloor reports a write that could not place enough shards to be
// acknowledged, as distinct from one that placed enough but not all.
var errShardWriteFloor = errors.New("too few shards durable")

// logShardWriteFailure reports one shard a write could not place, sampled per
// node and reason for the same reason a read failure is: a node down makes
// every write degraded, so an unsampled line floods exactly when the logs are
// being read.
func logShardWriteFailure(ctx context.Context, node config.NodeID, index int, err error) {
	reason := shardErrorReason(err)
	telemetry.RecordShardError(ctx, "write", reason, uint64(node))
	if !shardLogSampler.allow(node, reason) {
		return
	}
	slog.WarnContext(ctx, "Shard write failed; the stripe is short one holder",
		"node", node, "index", index, "reason", reason, "err", err)
}

// commitShards publishes every prepared shard before the placement record
// naming the epoch lands in global state. A superseded epoch is reported so
// its writer cannot publish a record that no shard can satisfy.
//
// Non-supersession failures remain recoverable by a reader or repair, but an
// epoch mismatch means another write has won the shard and this writer must
// not publish its placement.
// commitShards publishes every prepared shard and reports how many were
// overtaken by a newer generation.
//
// Being overtaken is not a failure and does not fail the write. Concurrent
// writers of one key have no defined winner, the epoch decides it, and the
// loser is still owed an acknowledgement — the object it wrote existed and was
// durable, and a newer one replaced it. That is last-write-wins, and the count
// is returned so it can be logged rather than acted on.
func commitShards(ctx context.Context, bc BlobClient, objectHash [32]byte, place ObjectToShardNodes, written writeResult) (superseded int) {
	var lost atomic.Int64
	forEachShard(written, func(index int, node config.NodeID) {
		overtaken, err := bc.Commit(ctx, node, blob.CommitRequest{
			Key:   objectHash,
			Index: uint32(index), //nolint:gosec // G115: index bounded by DataShards + ParityShards (small uint).
			Epoch: place.WriteEpoch,
		})
		if slices.Contains(written.ambiguous, index) {
			// This commit is the answer the put never gave. Say which way it
			// went: a node whose writes all land but never report in time is
			// invisible otherwise, and that is the whole defect.
			logAmbiguousResolution(ctx, node, index, place.WriteEpoch, overtaken, err)
			if overtaken {
				lost.Add(1)
			}

			return
		}
		switch {
		case err != nil:
			slog.WarnContext(ctx, "Shard commit failed",
				"node", node, "index", index,
				"epoch", fmt.Sprintf("%016x", place.WriteEpoch), "err", err)
		case overtaken:
			lost.Add(1)
		}
	})
	return int(lost.Load())
}

// logAmbiguousResolution reports what became of a shard whose put was
// abandoned. Prepared means the node was only slow and the stripe is at full
// width after all; not prepared means the shard really is absent and repair
// owns it. Overtaken means it did land and a newer generation has since won
// the position, which is last-write-wins and not this write's problem.
func logAmbiguousResolution(
	ctx context.Context, node config.NodeID, index int, epoch uint64, overtaken bool, err error,
) {
	switch {
	case err != nil && errors.Is(err, blob.ErrNotPrepared):
		slog.WarnContext(ctx, "Shard was not prepared; the put that was abandoned did not land",
			"node", node, "index", index, "epoch", fmt.Sprintf("%016x", epoch))
	case err != nil:
		slog.WarnContext(ctx, "Shard commit failed after an abandoned put; a read will complete it",
			"node", node, "index", index, "epoch", fmt.Sprintf("%016x", epoch), "err", err)
	case overtaken:
		telemetry.RecordShardError(ctx, telemetry.ShardOpWrite, telemetry.ShardReasonSlowCommit, uint64(node))
		slog.InfoContext(ctx, "Shard was prepared after its put was abandoned, then overtaken",
			"node", node, "index", index, "epoch", fmt.Sprintf("%016x", epoch))
	default:
		telemetry.RecordShardError(ctx, telemetry.ShardOpWrite, telemetry.ShardReasonSlowCommit, uint64(node))
		slog.InfoContext(ctx, "Shard was prepared after its put was abandoned; committed at full width",
			"node", node, "index", index, "epoch", fmt.Sprintf("%016x", epoch))
	}
}

// abortShards discards shards prepared for a write that will not be published,
// releasing their space now rather than leaving the nodes to age them out.
// Nothing references them either way, so a failure is logged and not returned.
func abortShards(ctx context.Context, bc BlobClient, objectHash [32]byte, place ObjectToShardNodes, written writeResult) {
	forEachShard(written, func(index int, node config.NodeID) {
		err := bc.Abort(ctx, node, blob.CommitRequest{
			Key:   objectHash,
			Index: uint32(index), //nolint:gosec // G115: index bounded by DataShards + ParityShards (small uint).
			Epoch: place.WriteEpoch,
		})
		if err != nil {
			slog.WarnContext(ctx, "Shard abort failed; the node will age the prepared extent out",
				"node", node, "index", index,
				"epoch", fmt.Sprintf("%016x", place.WriteEpoch), "err", err)
		}
	})
}

// forEachShard runs fn concurrently against every shard position that landed,
// at the node it actually landed on, and waits.
//
// A position whose put never succeeded has nothing prepared anywhere, so
// committing it would be told so and aborting it would ask a node to discard
// whatever generation it does hold. A position that was handed off is prepared
// on the holder and nowhere else, so both have to be addressed there.
// An ambiguous position is visited alongside the landed ones: committing it
// publishes a shard that did prepare, and answers ErrNotPrepared for one that
// did not, which is how a put nobody reported on is settled at no cost.
func forEachShard(written writeResult, fn func(index int, node config.NodeID)) {
	var wg sync.WaitGroup
	for i, node := range written.holders {
		if i < len(written.landed) && !written.landed[i] && !slices.Contains(written.ambiguous, i) {
			continue
		}
		wg.Go(func() { fn(i, node) })
	}
	wg.Wait()
}

// loadPlacement retrieves shard location metadata for an object.
func loadPlacement(ctx context.Context, mc MetaClient, ring *placement.Ring, cfg Config, bucket string, object string) (ObjectToShardNodes, int64, error) {
	objectHash := model.ObjectHash(bucket, object)

	shardNodes, err := ring.Nodes(objectHash, cfg.TotalShards())
	if err != nil {
		return ObjectToShardNodes{}, 0, err
	}

	data, err := metaGet(ctx, mc, model.TableObjects, string(objectHash[:]))
	if err != nil {
		return ObjectToShardNodes{}, 0, err
	}

	objectToShardNodes, err := DecodePlacement(data)
	if err != nil {
		return ObjectToShardNodes{}, 0, err
	}

	if len(shardNodes) != (len(objectToShardNodes.DataShardNodes) + len(objectToShardNodes.ParityShardNodes)) {
		return ObjectToShardNodes{}, 0, errors.New("number of shards does not match number of hash ring shards")
	}

	return objectToShardNodes, objectToShardNodes.Size, nil
}

// recordShardReadError counts a failed shard read under a bounded reason. The
// error text names a node and a key, so only the classification is recorded:
// a node that answered without the shard, or anything else.
func recordShardReadError(ctx context.Context, node config.NodeID, err error) {
	telemetry.RecordShardError(ctx, "read", shardErrorReason(err), uint64(node))
}

// shardErrorReason classifies a failed shard read into one of the bounded
// metric reasons. stale_epoch is the one worth separating: absent and
// transport say a node is down, which is visible everywhere else, while a
// stale epoch is a node that is up, answering and wrong.
func shardErrorReason(err error) string {
	switch {
	case errors.Is(err, blob.ErrEpochMismatch):
		return telemetry.ShardReasonStaleEpoch
	case errors.Is(err, blob.ErrNotFound):
		return telemetry.ShardReasonNotFound
	case errors.Is(err, blob.ErrCommitUnknown):
		return telemetry.ShardReasonCommitUnknown
	default:
		return telemetry.ShardReasonTransport
	}
}

// shardFailure records one shard the read could not use, and why. It is what
// makes a degraded read say which node was at fault rather than only that one
// was.
type shardFailure struct {
	index  int
	node   config.NodeID
	reason string
}

// logShardFailure reports one unusable shard, sampled per node and reason. A
// node stale for the whole keyspace makes every read degraded, so an unsampled
// line is a flood at exactly the moment the logs are being read; the counter in
// recordShardReadError is the unbounded-safe record of volume.
func logShardFailure(ctx context.Context, node config.NodeID, index int, err error, took time.Duration) {
	reason := shardErrorReason(err)
	if !shardLogSampler.allow(node, reason) {
		return
	}
	slog.InfoContext(ctx, "Shard unusable, falling back to parity",
		"node", node, "index", index, "reason", reason, "err", err,
		"duration_ms", took.Milliseconds())
}

// shardLogSampler admits one line per (node, reason) per second.
var shardLogSampler = &sampler{interval: time.Second, last: map[string]time.Time{}}

type sampler struct {
	interval time.Duration
	mu       sync.Mutex
	last     map[string]time.Time
}

func (s *sampler) allow(node config.NodeID, reason string) bool {
	key := fmt.Sprintf("%d/%s", node, reason)

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	if at, ok := s.last[key]; ok && now.Sub(at) < s.interval {
		return false
	}
	s.last[key] = now

	return true
}

// reportDegradedRead records a read that had to be reconstructed. The read
// itself succeeded and returned the correct bytes: a degraded stripe is ours to
// fix and never a reason to fail a client's request.
func reportDegradedRead(ctx context.Context, bucket, key string, failures []shardFailure, reconstructed int, took time.Duration) {
	if len(failures) == 0 {
		return
	}

	reasons := make([]string, 0, len(failures))
	nodes := make([]config.NodeID, 0, len(failures))
	for _, f := range failures {
		if !slices.Contains(reasons, f.reason) {
			reasons = append(reasons, f.reason)
		}
		if !slices.Contains(nodes, f.node) {
			nodes = append(nodes, f.node)
		}
	}

	slog.InfoContext(ctx, "Served degraded read",
		"bucket", bucket, "key", key,
		"reconstructed", reconstructed, "reasons", reasons, "nodes", nodes,
		"read_ms", took.Milliseconds())
}

// slowShardFloor is the delivered rate below which a shard read is worth naming
// its node for. A degrading node should be visible before it takes something
// down, and a rate says that where an absolute duration cannot: two seconds is
// healthy for a large block and hopeless for a small one.
const slowShardFloor = 8 << 20 // bytes per second

// hedgeProbeInterval is how often the stripe loop asks whether a shard is still
// delivering. Fine enough that a stall is caught well inside the delay, coarse
// enough to cost nothing on a healthy read.
const hedgeProbeInterval = 25 * time.Millisecond

// shardStallWindow is how long a shard that was delivering may deliver nothing
// before parity replaces it. Well below the blob client's own idle timeout,
// which aborts the transfer rather than routing around it.
const shardStallWindow = time.Second

// shardOpenTimeout caps the request and response envelope of a shard read.
// Those are small and fixed, so a total bound is the right one for them: a
// node that accepts a stream and never answers otherwise spends the whole
// request on itself, and a generous caller is punished hardest.
//
// It deliberately does not cover the body. The shard that follows is bounded
// by progress instead, which is the only bound that a 16 GiB shard and a
// stalled node can both be measured against.
const shardOpenTimeout = 5 * time.Second

// hedgeDelay is how long a shard gets to produce its first byte, measured from
// the moment a peer answered, before parity is fetched in its place. It bounds
// only silence: a shard delivering steadily is never hedged, whatever it is
// delivering at.
const hedgeDelay = 250 * time.Millisecond

// shardConvictionWindow is how long a shard may deliver nothing before it is
// treated as gone rather than slow. Past it, replacing the shard may spend the
// parity unit the hedge reserve holds back, which is the right call: the
// reserve exists for a genuine failure and this has become one.
//
// The same 5s as shardOpenTimeout, for the same reason. Without it a stalled
// shard falls to the blob client's 30s idle timeout, which is a correct read
// and far too slow a one.
const shardConvictionWindow = 5 * time.Second

// deleteObject sends DELETE requests to all shard nodes.
func deleteObject(ctx context.Context, bc BlobClient, bucket, key string, objectHash [32]byte, place ObjectToShardNodes) error {
	// Build (node, shardIndex) pairs so each delete carries the correct shard index.
	type nodeShard struct {
		node       config.NodeID
		shardIndex int
	}
	targets := make([]nodeShard, 0, len(place.DataShardNodes)+len(place.ParityShardNodes))
	for i, n := range place.DataShardNodes {
		targets = append(targets, nodeShard{node: n, shardIndex: i})
	}
	for i, n := range place.ParityShardNodes {
		targets = append(targets, nodeShard{node: n, shardIndex: len(place.DataShardNodes) + i})
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(targets))

	for _, t := range targets {
		wg.Add(1)
		go func(ns nodeShard) {
			defer wg.Done()

			delReq := blob.DeleteRequest{
				Key:   objectHash,
				Index: uint32(ns.shardIndex), //nolint:gosec // G115: shardIndex bounded by DataShards + ParityShards (small uint).
			}

			start := time.Now()
			resp, err := bc.Delete(ctx, ns.node, delReq)
			recordShardOutcome(ctx, telemetry.ShardOpDelete, ns.node, start, err)
			if err != nil {
				slog.Error("deleteObject: delete failed", "node", ns.node, "error", err)
				errCh <- err
				return
			}

			if !resp.Deleted {
				slog.Warn("deleteObject: shard not found on node", "node", ns.node, "shardIndex", ns.shardIndex)
			} else {
				slog.Debug("deleteObject: deleted shard", "node", ns.node, "shardIndex", ns.shardIndex, "bucket", bucket, "key", key)
			}
		}(t)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return nil
}

// releaseSuperseded tells the nodes behind a replaced placement record that its
// generation is finished with, so the disk comes back on the next sweep rather
// than at the end of the retention window.
//
// Best-effort by construction: the object is already durable and visible, the
// sweep reclaims the same rows on age alone, and a node that cannot be reached
// must not fail a write that has landed.
func releaseSuperseded(ctx context.Context, bc BlobClient, objectHash [32]byte, previous []byte, mine uint64) {
	if len(previous) == 0 {
		return
	}
	prev, err := DecodePlacement(previous)
	if err != nil || prev.WriteEpoch == mine || prev.Size == 0 {
		return
	}

	var wg sync.WaitGroup
	for index, node := range prev.AllNodes() {
		wg.Go(func() {
			if err := bc.Release(ctx, node, blob.ReleaseRequest{
				Key:   objectHash,
				Index: uint32(index), //nolint:gosec // G115: index bounded by DataShards + ParityShards (small uint).
				Epoch: prev.WriteEpoch,
			}); err != nil {
				slog.DebugContext(ctx, "Could not release a superseded generation; the sweep will age it out",
					"node", node, "index", index,
					"epoch", fmt.Sprintf("%016x", prev.WriteEpoch), "err", err)
			}
		})
	}
	wg.Wait()
}
