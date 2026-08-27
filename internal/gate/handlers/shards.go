package handlers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
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

// shardWriteOutcome captures the result of writing a shard to a blob node.
type shardWriteOutcome struct {
	shardIndex   int
	shardSize    int64
	poolNearFull bool // mirrors PutResponse.PoolNearFull for this shard's node.
	err          error
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
func placeShards(ring *placement.Ring, cfg Config, objectHash [32]byte, size int64) (ObjectToShardNodes, error) {
	nodes, err := ring.Nodes(objectHash, cfg.TotalShards())
	if err != nil {
		return ObjectToShardNodes{}, err
	}

	return ObjectToShardNodes{
		Size:             size,
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
	ctx context.Context, bc BlobClient, node config.NodeID, body io.Reader, size int64, objectHash [32]byte,
) (poolNearFull bool, err error) {
	counted := &countingReader{r: body}

	resp, err := bc.Put(ctx, node, blob.PutRequest{Key: objectHash, Size: size, Index: 0}, counted)
	if err != nil {
		return false, err
	}
	if counted.n != size {
		return false, fmt.Errorf("body delivered %d bytes, declared %d", counted.n, size)
	}
	return resp.PoolNearFull, nil
}

// writeObject splits body into RS shards and sends each to the appropriate
// node. size is what the caller declared the body to be: the splitter needs it
// up front, and it is what placement records, so a body that does not deliver
// exactly that many bytes is an error rather than a short object.
// poolNearFull is set if any shard's target node reported pressure.
//
// The stream encoder is constructed per request; hoisting it into the gate
// belongs with the streaming refactor, not here.
func writeObject(ctx context.Context, bc BlobClient, ring *placement.Ring, cfg Config, body io.Reader, size int64, objectHash [32]byte) (poolNearFull bool, err error) {
	// An empty object has no shard to write: the blob protocol rejects a
	// zero-length value, and recorded placement is enough to serve the GET.
	if size == 0 {
		return false, nil
	}

	// Use objectHash for hash ring placement for consistency with storage and retrieval
	shardNodes, err := ring.Nodes(objectHash, cfg.TotalShards())
	if err != nil {
		return false, err
	}

	// RS(1,0) is the whole object on one node: nothing to split and no parity
	// to encode. Streaming it straight through is what keeps a single-node
	// write off the heap, so it is its own path rather than a degenerate split.
	if cfg.DataShards == 1 && cfg.ParityShards == 0 {
		return writeSingleShard(ctx, bc, shardNodes[0], body, size, objectHash)
	}

	enc, err := reedsolomon.NewStream(cfg.DataShards, cfg.ParityShards)
	if err != nil {
		return false, err
	}

	// Calculate shard size
	ds := int64(cfg.DataShards)
	shardSize := int((size + ds - 1) / ds)

	// Step 1: Split the body into data shard buffers (in memory)
	// This allows us to both send to the blob nodes and use for parity encoding
	dataShardBuffers := make([][]byte, cfg.DataShards)
	dataWriters := make([]io.Writer, cfg.DataShards)
	for i := 0; i < cfg.DataShards; i++ {
		dataShardBuffers[i] = make([]byte, 0, shardSize)
		dataWriters[i] = &bytesBufferWriter{buf: &dataShardBuffers[i]}
	}

	if splitErr := enc.Split(body, dataWriters, size); splitErr != nil {
		return false, splitErr
	}

	// Step 2: Send data shards to their nodes
	dataCh := make(chan shardWriteOutcome, cfg.DataShards)
	var dataWG sync.WaitGroup

	for i := 0; i < cfg.DataShards; i++ {
		dataWG.Add(1)
		go func(idx int, shardData []byte) {
			defer dataWG.Done()

			nodeNum := shardNodes[idx]

			putReq := blob.PutRequest{
				Key:   objectHash,
				Size:  int64(len(shardData)),
				Index: uint32(idx), //nolint:gosec // G115: idx bounded by DataShards (small uint).
			}

			resp, putErr := bc.Put(ctx, nodeNum, putReq, bytes.NewReader(shardData))
			if putErr != nil {
				slog.Error("writeObject: put failed", "node", nodeNum, "error", putErr)
				dataCh <- shardWriteOutcome{shardIndex: idx, err: putErr}
				return
			}

			dataCh <- shardWriteOutcome{shardIndex: idx, shardSize: resp.Size, poolNearFull: resp.PoolNearFull}
		}(i, dataShardBuffers[i])
	}

	go func() {
		dataWG.Wait()
		close(dataCh)
	}()

	var firstErr error
	for outcome := range dataCh {
		if outcome.err != nil && firstErr == nil {
			firstErr = outcome.err
		}
		if outcome.poolNearFull {
			poolNearFull = true
		}
	}
	if firstErr != nil {
		return false, firstErr
	}

	// Step 3: Encode parity shards using the buffered data shards. Zero parity
	// has nothing to encode, and the encoder would read every data shard back
	// to produce nothing, so stop at the data shards.
	if cfg.ParityShards == 0 {
		return poolNearFull, nil
	}

	dataReaders := make([]io.Reader, cfg.DataShards)
	for i := 0; i < cfg.DataShards; i++ {
		dataReaders[i] = bytes.NewReader(dataShardBuffers[i])
	}

	parityWriters := make([]io.Writer, cfg.ParityShards)
	parityPipeWriters := make([]*io.PipeWriter, cfg.ParityShards)
	parityCh := make(chan shardWriteOutcome, cfg.ParityShards)
	var parityWG sync.WaitGroup

	for i := 0; i < cfg.ParityShards; i++ {
		pr, pw := io.Pipe()
		parityPipeWriters[i] = pw
		parityWriters[i] = pw

		parityIdx := cfg.DataShards + i
		parityWG.Add(1)
		go func(localParityIdx int, shardIdx int, r *io.PipeReader) {
			defer parityWG.Done()
			// enc.Encode runs on the caller's goroutine and writes into this
			// pipe. Abandoning the read side on an early return would block it
			// forever, and io.Pipe does not observe ctx.
			defer func() { _ = r.Close() }()

			nodeNum := shardNodes[shardIdx]

			putReq := blob.PutRequest{
				Key:   objectHash,
				Size:  int64(shardSize),
				Index: uint32(shardIdx), //nolint:gosec // G115: shardIdx bounded by DataShards + ParityShards (small uint).
			}

			resp, putErr := bc.Put(ctx, nodeNum, putReq, r)
			if putErr != nil {
				slog.Error("writeObject: put parity failed", "node", nodeNum, "error", putErr)
				parityCh <- shardWriteOutcome{shardIndex: localParityIdx, err: putErr}
				return
			}

			parityCh <- shardWriteOutcome{shardIndex: localParityIdx, shardSize: resp.Size, poolNearFull: resp.PoolNearFull}
		}(i, parityIdx, pr)
	}

	encodeErr := enc.Encode(dataReaders, parityWriters)

	for i := 0; i < cfg.ParityShards; i++ {
		if encodeErr != nil {
			_ = parityPipeWriters[i].CloseWithError(encodeErr)
		} else {
			_ = parityPipeWriters[i].Close()
		}
	}

	go func() {
		parityWG.Wait()
		close(parityCh)
	}()

	firstErr = nil
	for outcome := range parityCh {
		if outcome.err != nil && firstErr == nil {
			firstErr = outcome.err
		}
		if outcome.poolNearFull {
			poolNearFull = true
		}
	}
	if encodeErr != nil && firstErr == nil {
		firstErr = encodeErr
	}
	if firstErr != nil {
		return false, firstErr
	}

	return poolNearFull, nil
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

	objectToShardNodes, err := decodePlacement(data)
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
func readShard(ctx context.Context, bc BlobClient, objectHash [32]byte, node config.NodeID, index int) ([]byte, error) {
	reader, err := bc.Get(ctx, node, blob.GetRequest{
		Key:        objectHash,
		RangeStart: -1, // -1 means full shard (no range)
		RangeEnd:   -1,
		Index:      uint32(index), //nolint:gosec // G115: index bounded by shard count (small uint).
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
	reason := telemetry.ShardReasonTransport
	if errors.Is(err, blob.ErrNotFound) {
		reason = telemetry.ShardReasonNotFound
	}
	telemetry.RecordShardError(ctx, "read", reason)
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
func shardBytes(ctx context.Context, bc BlobClient, objectHash [32]byte, place ObjectToShardNodes) ([][]byte, error) {
	need := len(place.DataShardNodes)
	shards := make([][]byte, need+len(place.ParityShardNodes))
	if need == 0 {
		return shards, nil
	}

	// Cancelled on return, which abandons whatever is still outstanding: a
	// shard that arrives after the object has been served is wasted work.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var mu sync.Mutex
	available := 0
	enough := make(chan struct{})
	dataFailed := make(chan struct{})
	var enoughOnce, failedOnce sync.Once

	read := func(index int, node config.NodeID) {
		shardCtx, shardCancel := context.WithTimeout(ctx, shardReadTimeout)
		defer shardCancel()

		start := time.Now()
		data, err := readShard(shardCtx, bc, objectHash, node, index)
		if err != nil {
			slog.WarnContext(ctx, "Shard read failed, falling back to parity",
				"node", node, "index", index, "err", err,
				"duration_ms", time.Since(start).Milliseconds())
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
	if available < need {
		return snapshot, fmt.Errorf("%w: %d of %d shards available",
			errInsufficientShards, available, need)
	}
	return snapshot, nil
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
