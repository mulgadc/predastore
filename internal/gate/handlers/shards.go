package handlers

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/klauspost/reedsolomon"
	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/gate/placement"
)

// ObjectToShardNodes maps an object to its shard locations.
type ObjectToShardNodes struct {
	Object           [32]byte
	Size             int64
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
	if errors.Is(err, blob.ErrStoreFull) {
		return model.ErrInsufficientStorageError
	}
	return model.NewS3Error(model.ErrInternalError, err.Error(), 500)
}

// placeShards resolves where an object's shards live, in the ring order
// putObjectViaQUIC writes them in. Recording the placement is what makes the
// object retrievable, so the two must derive from the same object hash.
func placeShards(ring *placement.Ring, cfg Config, objectHash [32]byte, size int64) (ObjectToShardNodes, error) {
	nodes, err := ring.Nodes(objectHash, cfg.TotalShards())
	if err != nil {
		return ObjectToShardNodes{}, err
	}

	return ObjectToShardNodes{
		Object:           objectHash,
		Size:             size,
		DataShardNodes:   append([]config.NodeID(nil), nodes[:cfg.DataShards]...),
		ParityShardNodes: append([]config.NodeID(nil), nodes[cfg.DataShards:]...),
	}, nil
}

// putObjectViaQUIC splits a file into RS shards and sends each to the
// appropriate node. poolNearFull is set if any shard's target node reported
// pressure.
//
// The stream encoder is constructed per request; hoisting it into the gate
// belongs with the streaming refactor, not here.
func putObjectViaQUIC(ctx context.Context, shards *blob.Client, ring *placement.Ring, cfg Config, objectPath string, objectHash [32]byte) (size int64, poolNearFull bool, err error) {
	enc, err := reedsolomon.NewStream(cfg.DataShards, cfg.ParityShards)
	if err != nil {
		return 0, false, err
	}

	f, err := os.Open(objectPath)
	if err != nil {
		return 0, false, err
	}
	defer f.Close()

	instat, err := f.Stat()
	if err != nil {
		return 0, false, err
	}

	size = instat.Size()

	// Use objectHash for hash ring placement for consistency with storage and retrieval
	shardNodes, err := ring.Nodes(objectHash, cfg.TotalShards())
	if err != nil {
		return 0, false, err
	}

	// Calculate shard size
	fileSize := instat.Size()
	ds := int64(cfg.DataShards)
	shardSize := int((fileSize + ds - 1) / ds)

	// Step 1: Split file into data shard buffers (in memory)
	// This allows us to both send to the blob nodes and use for parity encoding
	dataShardBuffers := make([][]byte, cfg.DataShards)
	dataWriters := make([]io.Writer, cfg.DataShards)
	for i := 0; i < cfg.DataShards; i++ {
		dataShardBuffers[i] = make([]byte, 0, shardSize)
		dataWriters[i] = &bytesBufferWriter{buf: &dataShardBuffers[i]}
	}

	if splitErr := enc.Split(f, dataWriters, fileSize); splitErr != nil {
		return 0, false, splitErr
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
				ObjectHash: objectHash,
				ShardSize:  int64(len(shardData)),
				ShardIndex: uint32(idx), //nolint:gosec // G115: idx bounded by DataShards (small uint).
			}

			resp, putErr := shards.PutShard(ctx, nodeNum, putReq, bytes.NewReader(shardData))
			if putErr != nil {
				slog.Error("putObjectViaQUIC: put failed", "node", nodeNum, "error", putErr)
				dataCh <- shardWriteOutcome{shardIndex: idx, err: putErr}
				return
			}

			dataCh <- shardWriteOutcome{shardIndex: idx, shardSize: resp.ShardSize, poolNearFull: resp.PoolNearFull}
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
		return 0, false, firstErr
	}

	// Step 3: Encode parity shards using the buffered data shards
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

			nodeNum := shardNodes[shardIdx]

			putReq := blob.PutRequest{
				ObjectHash: objectHash,
				ShardSize:  int64(shardSize),
				ShardIndex: uint32(shardIdx), //nolint:gosec // G115: shardIdx bounded by DataShards + ParityShards (small uint).
			}

			resp, putErr := shards.PutShard(ctx, nodeNum, putReq, r)
			if putErr != nil {
				slog.Error("putObjectViaQUIC: put parity failed", "node", nodeNum, "error", putErr)
				parityCh <- shardWriteOutcome{shardIndex: localParityIdx, err: putErr}
				return
			}

			parityCh <- shardWriteOutcome{shardIndex: localParityIdx, shardSize: resp.ShardSize, poolNearFull: resp.PoolNearFull}
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
		return 0, false, firstErr
	}

	return size, poolNearFull, nil
}

// openInput retrieves shard location metadata for an object.
func openInput(st Meta, ring *placement.Ring, cfg Config, bucket string, object string) (ObjectToShardNodes, int64, error) {
	objectHash := model.ObjectHash(bucket, object)

	shardNodes, err := ring.Nodes(objectHash, cfg.TotalShards())
	if err != nil {
		return ObjectToShardNodes{}, 0, err
	}

	data, err := metaGet(st, model.TableObjects, string(objectHash[:]))
	if err != nil {
		return ObjectToShardNodes{}, 0, err
	}

	var objectToShardNodes ObjectToShardNodes
	r := bytes.NewReader(data)
	dec := gob.NewDecoder(r) //nolint:gosec // G709: the input is state this gate wrote, not client data.

	if err := dec.Decode(&objectToShardNodes); err != nil {
		return ObjectToShardNodes{}, 0, err
	}

	if len(shardNodes) != (len(objectToShardNodes.DataShardNodes) + len(objectToShardNodes.ParityShardNodes)) {
		return ObjectToShardNodes{}, 0, errors.New("number of shards does not match number of hash ring shards")
	}

	return objectToShardNodes, objectToShardNodes.Size, nil
}

// shardReaders creates readers for each shard.
// Data is buffered into memory before connections are closed to avoid
// "connection closed" errors when the caller reads from the returned readers.
func shardReaders(client *blob.Client, objectHash [32]byte, shards ObjectToShardNodes, parity bool) ([]io.Reader, error) {
	readers := make([]io.Reader, len(shards.DataShardNodes)+len(shards.ParityShardNodes))

	totalNodes := make([]config.NodeID, 0)
	totalNodes = append(totalNodes, shards.DataShardNodes...)

	if parity {
		totalNodes = append(totalNodes, shards.ParityShardNodes...)
	}

	for i := range totalNodes {
		nodeNum := totalNodes[i]

		objectRequest := blob.GetRequest{
			ObjectHash: objectHash,
			RangeStart: -1, // -1 means full shard (no range)
			RangeEnd:   -1,
			ShardIndex: uint32(i), // Include shard index for unique lookup
		}

		reader, err := client.GetShard(context.Background(), nodeNum, objectRequest)
		if err != nil {
			slog.Error("Error reading shard from blob node", "node", nodeNum, "err", err)
			// Don't close - connection stays in pool
			return readers, err
		}

		// Buffer the shard data into memory before closing the stream.
		// This prevents "stream closed" errors when the caller reads.
		data, err := io.ReadAll(reader)
		if closeErr := reader.Close(); closeErr != nil {
			slog.Debug("Failed to close shard stream reader", "node", nodeNum, "error", closeErr)
		}

		if err != nil {
			slog.Error("Error buffering shard data", "node", nodeNum, "err", err)
			return readers, err
		}

		readers[i] = bytes.NewReader(data)
	}

	return readers, nil
}

// reconstructObject attempts to rebuild an object using parity shards.
func reconstructObject(ctx context.Context, client *blob.Client, objectHash [32]byte, shards ObjectToShardNodes, enc reedsolomon.StreamEncoder, size int64) (*bytes.Buffer, error) {
	// Get all shard readers including parity
	readers, err := shardReaders(client, objectHash, shards, true)
	if err != nil {
		return nil, err
	}

	// Create reconstruction writers for missing shards
	reconstruction := make([]io.Writer, len(readers))
	files := make([]*os.File, len(readers))

	for i := range reconstruction {
		if readers[i] == nil {
			filename := fmt.Sprintf("%s.%d", hex.EncodeToString(objectHash[:]), i)
			outfn := filepath.Join(os.TempDir(), filename)

			files[i], err = os.Create(outfn)
			if err != nil {
				return nil, err
			}
			defer os.Remove(outfn)
			defer files[i].Close()

			slog.InfoContext(ctx, "Creating temporary file for reconstruction", "filename", outfn)
			reconstruction[i] = files[i]
		}
	}

	// Reconstruct missing shards
	err = enc.Reconstruct(readers, reconstruction)
	if err != nil {
		return nil, fmt.Errorf("reconstruction failed: %w", err)
	}

	// Close reconstruction writers
	for i := range files {
		if files[i] != nil {
			if closeErr := files[i].Close(); closeErr != nil {
				slog.Debug("Failed to close reconstruction file", "index", i, "error", closeErr)
			}
		}
	}

	// Re-read shards with reconstructed data
	readers, err = shardReaders(client, objectHash, shards, true)
	if err != nil {
		return nil, err
	}

	// Fill in reconstructed shards
	for i := range readers {
		if readers[i] == nil && files[i] != nil {
			f, err := os.Open(files[i].Name())
			if err != nil {
				return nil, err
			}
			defer f.Close()
			readers[i] = f
		}
	}

	// Join the shards
	var out bytes.Buffer
	err = enc.Join(&out, readers, size)
	if err != nil {
		return nil, fmt.Errorf("join after reconstruction failed: %w", err)
	}

	return &out, nil
}

// deleteObjectViaQUIC sends DELETE requests to all shard nodes.
func deleteObjectViaQUIC(ctx context.Context, client *blob.Client, bucket, key string, objectHash [32]byte, shards ObjectToShardNodes) error {
	// Build (node, shardIndex) pairs so each delete carries the correct shard index.
	type nodeShard struct {
		node       config.NodeID
		shardIndex int
	}
	targets := make([]nodeShard, 0, len(shards.DataShardNodes)+len(shards.ParityShardNodes))
	for i, n := range shards.DataShardNodes {
		targets = append(targets, nodeShard{node: n, shardIndex: i})
	}
	for i, n := range shards.ParityShardNodes {
		targets = append(targets, nodeShard{node: n, shardIndex: len(shards.DataShardNodes) + i})
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(targets))

	for _, t := range targets {
		wg.Add(1)
		go func(ns nodeShard) {
			defer wg.Done()

			delReq := blob.DeleteRequest{
				ObjectHash: objectHash,
				ShardIndex: uint32(ns.shardIndex), //nolint:gosec // G115: shardIndex bounded by DataShards + ParityShards (small uint).
			}

			resp, err := client.DeleteShard(ctx, ns.node, delReq)
			if err != nil {
				slog.Error("deleteObjectViaQUIC: delete failed", "node", ns.node, "error", err)
				errCh <- err
				return
			}

			if !resp.Deleted {
				slog.Warn("deleteObjectViaQUIC: shard not found on node", "node", ns.node, "shardIndex", ns.shardIndex)
			} else {
				slog.Debug("deleteObjectViaQUIC: deleted shard", "node", ns.node, "shardIndex", ns.shardIndex, "bucket", bucket, "key", key)
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
