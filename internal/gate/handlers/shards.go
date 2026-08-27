package handlers

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"
	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/blob/engine"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/gate/placement"
	"github.com/mulgadc/predastore/internal/telemetry"
)

// ObjectToShardNodes maps an object to its shard locations. The node ids are
// the record of where the shards physically went, not a cache of what the ring
// would derive today: the read path dials these, so they are what survives a
// ring whose tuning changed under them.
type ObjectToShardNodes struct {
	Size             int64
	WriteEpoch       uint64
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

// mintEpoch draws a write epoch. It is compared only for equality, so it needs
// uniqueness and nothing else: no ordering, no clock, no counter. Zero is
// reserved as "invalid", which removes the ambiguity between an absent epoch
// and a draw that happened to come back zero.
func mintEpoch() (uint64, error) {
	var b [8]byte
	for {
		if _, err := rand.Read(b[:]); err != nil {
			return 0, fmt.Errorf("draw write epoch: %w", err)
		}
		if e := binary.BigEndian.Uint64(b[:]); e != 0 {
			return e, nil
		}
	}
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
	missing      []config.NodeID
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
func (r writeResult) degraded() bool { return len(r.missing) > 0 }

// fullWidth is the result of a write with no shards to place, which is an empty
// object: nothing is missing because nothing was owed.
func fullWidth(total int) writeResult {
	landed := make([]bool, total)
	for i := range landed {
		landed[i] = true
	}

	return writeResult{landed: landed}
}

// bytesBufferWriter wraps a byte slice pointer for use as io.Writer.
type bytesBufferWriter struct {
	buf *[]byte
}

func (w *bytesBufferWriter) Write(p []byte) (n int, err error) {
	*w.buf = append(*w.buf, p...)
	return len(p), nil
}

// mapPutErr translates a shard-write error into the S3 error returned to the
// client. A pool-full shard write must surface as 507, not the generic 500
// other failures get.
func mapPutErr(err error) *model.S3Error {
	if errors.Is(err, engine.ErrStoreFull) {
		return model.ErrInsufficientStorageError
	}
	return model.NewS3Error(model.ErrInternalError, err.Error(), 500)
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

	epoch, err := mintEpoch()
	if err != nil {
		return ObjectToShardNodes{}, err
	}

	return ObjectToShardNodes{
		Size:             size,
		WriteEpoch:       epoch,
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

// writeSingleShard streams the body to one node without staging it. The blob
// client reads at most size bytes, so an over-long body is truncated rather
// than overrunning the shard; a short one is caught here, because placement
// records size and a short value would read back as a corrupt object.
func writeSingleShard(
	ctx context.Context, bc BlobClient, node config.NodeID, body io.Reader, size int64,
	objectHash [32]byte, epoch uint64,
) (writeResult, error) {
	counted := &countingReader{r: body}

	resp, err := bc.Put(ctx, node, blob.PutRequest{Key: objectHash, Size: size, Index: 0, Epoch: epoch}, counted)
	if err != nil {
		return writeResult{landed: []bool{false}, missing: []config.NodeID{node}}, err
	}
	if counted.n != size {
		return writeResult{landed: []bool{true}}, fmt.Errorf("body delivered %d bytes, declared %d", counted.n, size)
	}

	return writeResult{poolNearFull: resp.PoolNearFull, landed: []bool{true}}, nil
}

// writeObject splits body into RS shards and sends each to the appropriate
// node. size is what the caller declared the body to be: the splitter needs it
// up front, and it is what placement records, so a body that does not deliver
// exactly that many bytes is an error rather than a short object.
// poolNearFull is set if any shard's target node reported pressure.
//
// The stream encoder is constructed per request; hoisting it into the gate
// belongs with the streaming refactor, not here.
func writeObject(ctx context.Context, bc BlobClient, cfg Config, body io.Reader, size int64, objectHash [32]byte, place ObjectToShardNodes) (writeResult, error) {
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

	shards, err := encodeShards(cfg, body, size)
	if err != nil {
		return writeResult{landed: make([]bool, cfg.TotalShards())}, err
	}

	return putShards(ctx, bc, cfg, objectHash, epoch, shardNodes, shards)
}

// encodeShards splits the body into its data shards and encodes the parity
// from them, all in memory.
//
// The parity used to be streamed to its nodes through pipes fed by the
// encoder, which coupled the shards to each other: one parity node refusing
// closed its pipe, failed the encode, and took every other parity shard down
// with it. Under degraded writes that is the difference between losing one
// shard and losing all the redundancy at once. The data shards were already
// buffered whole to encode from, so holding the parity too costs m/k more.
func encodeShards(cfg Config, body io.Reader, size int64) ([][]byte, error) {
	enc, err := reedsolomon.NewStream(cfg.DataShards, cfg.ParityShards)
	if err != nil {
		return nil, err
	}

	ds := int64(cfg.DataShards)
	shardSize := int((size + ds - 1) / ds)

	shards := make([][]byte, cfg.TotalShards())
	dataWriters := make([]io.Writer, cfg.DataShards)
	for i := range cfg.DataShards {
		shards[i] = make([]byte, 0, shardSize)
		dataWriters[i] = &bytesBufferWriter{buf: &shards[i]}
	}
	if splitErr := enc.Split(body, dataWriters, size); splitErr != nil {
		return nil, splitErr
	}

	if cfg.ParityShards == 0 {
		return shards, nil
	}

	dataReaders := make([]io.Reader, cfg.DataShards)
	for i := range cfg.DataShards {
		dataReaders[i] = bytes.NewReader(shards[i])
	}
	parityWriters := make([]io.Writer, cfg.ParityShards)
	for i := range cfg.ParityShards {
		idx := cfg.DataShards + i
		shards[idx] = make([]byte, 0, shardSize)
		parityWriters[i] = &bytesBufferWriter{buf: &shards[idx]}
	}
	if encodeErr := enc.Encode(dataReaders, parityWriters); encodeErr != nil {
		return nil, encodeErr
	}

	return shards, nil
}

// putShards prepares every shard on its node concurrently and reports which
// landed. A node that refuses is not fatal on its own: the write is acceptable
// once cfg.MinShards() of the stripe are durable, because any DataShards of
// them reconstruct the object. Below that the write fails and names the nodes
// that were missing.
func putShards(
	ctx context.Context, bc BlobClient, cfg Config,
	objectHash [32]byte, epoch uint64, shardNodes []config.NodeID, shards [][]byte,
) (writeResult, error) {
	outcomes := make(chan shardWriteOutcome, len(shards))
	var wg sync.WaitGroup
	for i, shardData := range shards {
		node := shardNodes[i]
		wg.Go(func() {
			putReq := blob.PutRequest{
				Key:   objectHash,
				Size:  int64(len(shardData)),
				Index: uint32(i), //nolint:gosec // G115: i bounded by DataShards + ParityShards (small uint).
				Epoch: epoch,
			}
			resp, putErr := bc.Put(ctx, node, putReq, bytes.NewReader(shardData))
			if putErr != nil {
				outcomes <- shardWriteOutcome{shardIndex: i, err: putErr}
				return
			}
			outcomes <- shardWriteOutcome{shardIndex: i, poolNearFull: resp.PoolNearFull}
		})
	}
	wg.Wait()
	close(outcomes)

	result := writeResult{landed: make([]bool, len(shards))}
	var firstErr error
	for outcome := range outcomes {
		if outcome.err != nil {
			if firstErr == nil {
				firstErr = outcome.err
			}
			node := shardNodes[outcome.shardIndex]
			result.missing = append(result.missing, node)
			logShardWriteFailure(ctx, node, outcome.shardIndex, outcome.err)

			continue
		}
		result.landed[outcome.shardIndex] = true
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

// errShardWriteFloor reports a write that could not place enough shards to be
// acknowledged, as distinct from one that placed enough but not all.
var errShardWriteFloor = errors.New("too few shards durable")

// logShardWriteFailure reports one shard a write could not place, sampled per
// node and reason for the same reason a read failure is: a node down makes
// every write degraded, so an unsampled line floods exactly when the logs are
// being read.
func logShardWriteFailure(ctx context.Context, node config.NodeID, index int, err error) {
	reason := shardErrorReason(err)
	telemetry.RecordShardError(ctx, "write", reason)
	if !shardLogSampler.allow(node, reason) {
		return
	}
	slog.WarnContext(ctx, "Shard write failed; the stripe is short one holder",
		"node", node, "index", index, "reason", reason, "err", err)
}

// commitShards publishes every prepared shard, after the placement record
// naming the epoch has landed in global state.
//
// The record is the commit point, so a failure here is recoverable rather than
// fatal: the shards are durable under exactly the epoch the record names, and
// a reader asking for that epoch completes the commit itself. It is reported
// because a node failing to commit is worth seeing, not because the object is
// in doubt.
func commitShards(ctx context.Context, bc BlobClient, objectHash [32]byte, place ObjectToShardNodes, landed []bool) {
	forEachShard(place, landed, func(index int, node config.NodeID) {
		err := bc.Commit(ctx, node, blob.CommitRequest{
			Key:   objectHash,
			Index: uint32(index), //nolint:gosec // G115: index bounded by DataShards + ParityShards (small uint).
			Epoch: place.WriteEpoch,
		})
		if err != nil {
			slog.WarnContext(ctx, "Shard commit failed; the record is published and a read will complete it",
				"node", node, "index", index,
				"epoch", fmt.Sprintf("%016x", place.WriteEpoch), "err", err)
		}
	})
}

// abortShards discards shards prepared for a write that will not be published,
// releasing their space now rather than leaving the nodes to age them out.
// Nothing references them either way, so a failure is logged and not returned.
func abortShards(ctx context.Context, bc BlobClient, objectHash [32]byte, place ObjectToShardNodes, landed []bool) {
	forEachShard(place, landed, func(index int, node config.NodeID) {
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
// and waits. A position whose put never succeeded has nothing prepared on its
// node, so committing it would be told so and aborting it would ask the node to
// discard whatever generation it does hold.
func forEachShard(place ObjectToShardNodes, landed []bool, fn func(index int, node config.NodeID)) {
	var wg sync.WaitGroup
	for i, node := range place.AllNodes() {
		if i < len(landed) && !landed[i] {
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

// readShard fetches one shard whole and buffers it. The data is read into
// memory before the stream is closed, so the caller never reads from a closed
// connection.
func readShard(ctx context.Context, bc BlobClient, objectHash [32]byte, node config.NodeID, index int, epoch uint64) ([]byte, error) {
	reader, err := bc.Get(ctx, node, blob.GetRequest{
		Key:        objectHash,
		RangeStart: -1, // -1 means full shard (no range)
		RangeEnd:   -1,
		Index:      uint32(index), //nolint:gosec // G115: index bounded by shard count (small uint).
		Epoch:      epoch,
	})
	if err != nil {
		recordShardReadError(ctx, err)
		return nil, err
	}
	data, err := io.ReadAll(reader)
	if closeErr := reader.Close(); closeErr != nil {
		slog.DebugContext(ctx, "Failed to close shard stream reader", "node", node, "error", closeErr)
	}
	if err != nil {
		recordShardReadError(ctx, err)
		return nil, err
	}
	return data, nil
}

// recordShardReadError counts a failed shard read under a bounded reason. The
// error text names a node and a key, so only the classification is recorded:
// a node that answered without the shard, or anything else.
func recordShardReadError(ctx context.Context, err error) {
	telemetry.RecordShardError(ctx, "read", shardErrorReason(err))
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
	default:
		return telemetry.ShardReasonTransport
	}
}

// shardBytes reads an object's shards, concurrently, tolerantly and hedged.
//
// Concurrently, because reading them one after another makes an object's read
// latency the sum of its shards rather than the slowest of them, so one slow
// node stalls every read that touches it.
//
// Tolerantly, because a shard that cannot be read is exactly what the parity
// exists for. A failed shard leaves a nil entry for the caller to reconstruct
// from; an error is returned only when too few shards survive for any
// reconstruction to be possible.
//
// Hedged, because the read is complete once enough shards have arrived, and
// waiting for the rest hands a stalled node the power to set every reader's
// latency. The data shards go first, so a healthy read never fetches a parity
// shard it does not need; the parity shards follow only if the data shards have
// not all landed shortly after, or one of them has already failed.
func shardBytes(ctx context.Context, bc BlobClient, objectHash [32]byte, place ObjectToShardNodes) ([][]byte, []shardFailure, error) {
	need := len(place.DataShardNodes)
	shards := make([][]byte, need+len(place.ParityShardNodes))
	if need == 0 {
		return shards, nil, nil
	}

	// Cancelled on return, which abandons whatever is still outstanding: a
	// shard that arrives after the object has been served is wasted work.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var mu sync.Mutex
	available := 0
	var failures []shardFailure
	enough := make(chan struct{})
	dataFailed := make(chan struct{})
	var enoughOnce, failedOnce sync.Once

	read := func(index int, node config.NodeID) {
		shardCtx, shardCancel := context.WithTimeout(ctx, shardReadTimeout)
		defer shardCancel()

		start := time.Now()
		data, err := readShard(shardCtx, bc, objectHash, node, index, place.WriteEpoch)
		if err != nil {
			mu.Lock()
			failures = append(failures, shardFailure{index: index, node: node, reason: shardErrorReason(err)})
			mu.Unlock()
			logShardFailure(ctx, node, index, err, time.Since(start))
			if index < need {
				failedOnce.Do(func() { close(dataFailed) })
			}
			return
		}
		if elapsed := time.Since(start); elapsed >= slowShardThreshold {
			slog.WarnContext(ctx, "Shard read slow",
				"node", node, "index", index, "duration_ms", elapsed.Milliseconds())
		}

		mu.Lock()
		defer mu.Unlock()
		// Indices are distinct, but the lock still guards the slice itself: the
		// caller snapshots it once enough shards land, while slower reads are
		// still outstanding and may yet write their own entry.
		shards[index] = data
		available++
		if available >= need {
			enoughOnce.Do(func() { close(enough) })
		}
	}

	// Every goroutine is registered before anything waits on them, so the
	// parity reads wait for their cue rather than being started later.
	var wg sync.WaitGroup
	for i, node := range place.DataShardNodes {
		wg.Go(func() { read(i, node) })
	}
	for i, node := range place.ParityShardNodes {
		wg.Go(func() {
			hedge := time.NewTimer(hedgeDelay)
			defer hedge.Stop()
			select {
			case <-enough:
				return // the data shards were enough on their own
			case <-ctx.Done():
				return
			case <-dataFailed:
			case <-hedge.C:
			}
			select {
			case <-enough:
				return
			default:
			}
			read(need+i, node)
		})
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-enough:
	case <-done:
	}

	// The hedge returns as soon as enough shards have landed, so the slower
	// reads are still running. Hand back a snapshot: a straggler writing its
	// entry must not mutate the slice the caller is reconstructing from.
	mu.Lock()
	defer mu.Unlock()
	snapshot := make([][]byte, len(shards))
	copy(snapshot, shards)
	failed := slices.Clone(failures)
	if available < need {
		return snapshot, failed, fmt.Errorf("%w: %d of %d shards available",
			errInsufficientShards, available, need)
	}
	return snapshot, failed, nil
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

// slowShardThreshold is the point past which a shard read is worth naming its
// node for. A degrading node should be visible before it takes something down.
const slowShardThreshold = 2 * time.Second

// shardReadTimeout caps a single shard read regardless of how much budget the
// caller had. A node that accepts a stream and never answers otherwise spends
// the whole request on itself, and a generous caller is punished hardest.
const shardReadTimeout = 5 * time.Second

// hedgeDelay is how long the data shards get before the parity shards are
// fetched as well. Comfortably above a healthy shard read, and far below
// anything a client would notice.
const hedgeDelay = 250 * time.Millisecond

// errInsufficientShards reports that too few shards survived to rebuild the
// object, as distinct from some shards being missing but recoverable.
var errInsufficientShards = errors.New("insufficient shards to reconstruct")

// shardReadersOf turns shard bytes into readers, leaving a nil for every
// shard that could not be read so the encoder knows to rebuild it.
func shardReadersOf(shards [][]byte) []io.Reader {
	readers := make([]io.Reader, len(shards))
	for i, s := range shards {
		if s != nil {
			readers[i] = bytes.NewReader(s)
		}
	}
	return readers
}

// missingShards counts the first n shards the read could not fill.
func missingShards(shards [][]byte, n int) int {
	missing := 0
	for i := 0; i < n && i < len(shards); i++ {
		if shards[i] == nil {
			missing++
		}
	}
	return missing
}

// reconstructObject rebuilds an object from its parity shards.
//
// Recovered shards are held in memory. They used to be written to temp files
// named only for the object hash and shard index, so two concurrent reads of
// one object wrote and deleted the same paths underneath each other. Keeping
// them in memory also removes a second read of every shard, since the encoder
// consumes the readers it is given.
func reconstructObject(enc reedsolomon.StreamEncoder, shards [][]byte, size int64) (*bytes.Buffer, error) {
	recovered := make([]*bytes.Buffer, len(shards))
	writers := make([]io.Writer, len(shards))
	for i := range shards {
		if shards[i] == nil {
			recovered[i] = &bytes.Buffer{}
			writers[i] = recovered[i]
		}
	}

	if err := enc.Reconstruct(shardReadersOf(shards), writers); err != nil {
		return nil, fmt.Errorf("reconstruction failed: %w", err)
	}
	for i := range shards {
		if shards[i] == nil && recovered[i] != nil {
			shards[i] = recovered[i].Bytes()
		}
	}

	var out bytes.Buffer
	if err := enc.Join(&out, shardReadersOf(shards), size); err != nil {
		return nil, fmt.Errorf("join after reconstruction failed: %w", err)
	}
	return &out, nil
}

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

			resp, err := bc.Delete(ctx, ns.node, delReq)
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
