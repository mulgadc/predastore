package s3

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash/v2"
	"github.com/klauspost/reedsolomon"
	"github.com/mulgadc/predastore/internal/gateway/model"
	"github.com/mulgadc/predastore/internal/storage"
	"github.com/mulgadc/predastore/internal/topology"
)

// Reed-Solomon and hash ring defaults, applied when the config leaves them at
// zero. The ring's load factor bounds how unevenly partitions may be spread.
const (
	defaultDataShards             = 3
	defaultParityShards           = 2
	ringPartitionCount            = 5
	ringReplicationFactor         = 100
	ringLoad              float64 = 1.25
)

// ObjectToShardNodes maps an object to its shard locations.
type ObjectToShardNodes struct {
	Object           [32]byte
	Size             int64
	DataShardNodes   []uint32
	ParityShardNodes []uint32
}

// hasher implements consistent.Hasher using xxhash.
type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

// myMember implements consistent.Member.
type myMember string

func (m myMember) String() string {
	return string(m)
}

// shardWriteOutcome captures the result of writing a shard to a storage node.
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

// storageNodeIDs returns the ids of the cluster's shard-storage nodes: the
// members the gateway places shards across.
func (s3 *Config) storageNodeIDs() []int {
	var ids []int
	for _, n := range s3.ClusterNodes {
		if n.Role == topology.RoleShardStorage {
			ids = append(ids, n.ID)
		}
	}
	return ids
}

// newHashRing builds the placement ring over the given storage nodes. Ring
// member names carry the node id, so placement resolves straight to the id the
// storage client addresses.
func newHashRing(nodes []int) *consistent.Consistent {
	ring := consistent.New(nil, consistent.Config{
		PartitionCount:    ringPartitionCount,
		ReplicationFactor: ringReplicationFactor,
		Load:              ringLoad,
		Hasher:            hasher{},
	})
	for _, id := range nodes {
		ring.Add(myMember(fmt.Sprintf("node-%d", id)))
	}
	return ring
}

// NodeToUint32 converts a node name to uint32. Returns an error if the
// numeric component is negative or exceeds math.MaxUint32.
func NodeToUint32(value string) (uint32, error) {
	s := strings.Replace(value, "node-", "", 1)
	vint, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}
	if vint < 0 || vint > math.MaxUint32 {
		return 0, fmt.Errorf("node id %d out of uint32 range", vint)
	}
	return uint32(vint), nil
}

// mapPutErr translates a shard-write error into the S3 error returned to the
// client. A pool-full shard write must surface as 507, not the generic 500
// other failures get.
func mapPutErr(err error) *model.S3Error {
	if errors.Is(err, storage.ErrStoreFull) {
		return model.ErrInsufficientStorageError
	}
	return model.NewS3Error(model.ErrInternalError, err.Error(), 500)
}

// placeShards resolves where an object's shards live, in the ring order
// putObjectViaQUIC writes them in. Recording the placement is what makes the
// object retrievable, so the two must derive from the same object hash.
func (s *HTTP2Server) placeShards(objectHash [32]byte, size int64) (ObjectToShardNodes, error) {
	hashRingShards, err := s.hashRing.GetClosestN(objectHash[:], s.rsDataShard+s.rsParityShard)
	if err != nil {
		return ObjectToShardNodes{}, err
	}

	placement := ObjectToShardNodes{
		Object:           objectHash,
		Size:             size,
		DataShardNodes:   make([]uint32, s.rsDataShard),
		ParityShardNodes: make([]uint32, s.rsParityShard),
	}
	for i := 0; i < s.rsDataShard; i++ {
		placement.DataShardNodes[i], err = NodeToUint32(hashRingShards[i].String())
		if err != nil {
			return ObjectToShardNodes{}, err
		}
	}
	for i := 0; i < s.rsParityShard; i++ {
		placement.ParityShardNodes[i], err = NodeToUint32(hashRingShards[s.rsDataShard+i].String())
		if err != nil {
			return ObjectToShardNodes{}, err
		}
	}
	return placement, nil
}

// putObjectViaQUIC splits a file into RS shards and sends each to the
// appropriate node. poolNearFull is set if any shard's target node reported
// pressure.
//
// The stream encoder is constructed per request; hoisting it into the gateway
// belongs with the streaming refactor, not here.
func (s *HTTP2Server) putObjectViaQUIC(ctx context.Context, objectPath string, objectHash [32]byte) (size int64, poolNearFull bool, err error) {
	enc, err := reedsolomon.NewStream(s.rsDataShard, s.rsParityShard)
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
	hashRingShards, err := s.hashRing.GetClosestN(objectHash[:], s.rsDataShard+s.rsParityShard)
	if err != nil {
		return 0, false, err
	}

	// Calculate shard size
	fileSize := instat.Size()
	ds := int64(s.rsDataShard)
	shardSize := int((fileSize + ds - 1) / ds)

	// Step 1: Split file into data shard buffers (in memory)
	// This allows us to both send to the storage nodes and use for parity encoding
	dataShardBuffers := make([][]byte, s.rsDataShard)
	dataWriters := make([]io.Writer, s.rsDataShard)
	for i := 0; i < s.rsDataShard; i++ {
		dataShardBuffers[i] = make([]byte, 0, shardSize)
		dataWriters[i] = &bytesBufferWriter{buf: &dataShardBuffers[i]}
	}

	if splitErr := enc.Split(f, dataWriters, fileSize); splitErr != nil {
		return 0, false, splitErr
	}

	// Step 2: Send data shards to their nodes
	dataCh := make(chan shardWriteOutcome, s.rsDataShard)
	var dataWG sync.WaitGroup

	for i := 0; i < s.rsDataShard; i++ {
		dataWG.Add(1)
		go func(idx int, shardData []byte) {
			defer dataWG.Done()

			nodeNum, nodeErr := NodeToUint32(hashRingShards[idx].String())
			if nodeErr != nil {
				dataCh <- shardWriteOutcome{shardIndex: idx, err: nodeErr}
				return
			}

			putReq := storage.PutRequest{
				ObjectHash: objectHash,
				ShardSize:  int64(len(shardData)),
				ShardIndex: uint32(idx), //nolint:gosec // G115: idx bounded by rsDataShard (small uint).
			}

			resp, putErr := s.shards.PutShard(ctx, int(nodeNum), putReq, bytes.NewReader(shardData))
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
	dataReaders := make([]io.Reader, s.rsDataShard)
	for i := 0; i < s.rsDataShard; i++ {
		dataReaders[i] = bytes.NewReader(dataShardBuffers[i])
	}

	parityWriters := make([]io.Writer, s.rsParityShard)
	parityPipeWriters := make([]*io.PipeWriter, s.rsParityShard)
	parityCh := make(chan shardWriteOutcome, s.rsParityShard)
	var parityWG sync.WaitGroup

	for i := 0; i < s.rsParityShard; i++ {
		pr, pw := io.Pipe()
		parityPipeWriters[i] = pw
		parityWriters[i] = pw

		parityIdx := s.rsDataShard + i
		parityWG.Add(1)
		go func(localParityIdx int, hashRingIdx int, r *io.PipeReader) {
			defer parityWG.Done()

			nodeNum, nodeErr := NodeToUint32(hashRingShards[hashRingIdx].String())
			if nodeErr != nil {
				parityCh <- shardWriteOutcome{shardIndex: localParityIdx, err: nodeErr}
				_, _ = io.Copy(io.Discard, r)
				return
			}

			putReq := storage.PutRequest{
				ObjectHash: objectHash,
				ShardSize:  int64(shardSize),
				ShardIndex: uint32(hashRingIdx), //nolint:gosec // G115: hashRingIdx bounded by rsDataShard + rsParityShard (small uint).
			}

			resp, putErr := s.shards.PutShard(ctx, int(nodeNum), putReq, r)
			if putErr != nil {
				slog.Error("putObjectViaQUIC: put parity failed", "node", nodeNum, "error", putErr)
				parityCh <- shardWriteOutcome{shardIndex: localParityIdx, err: putErr}
				return
			}

			parityCh <- shardWriteOutcome{shardIndex: localParityIdx, shardSize: resp.ShardSize, poolNearFull: resp.PoolNearFull}
		}(i, parityIdx, pr)
	}

	encodeErr := enc.Encode(dataReaders, parityWriters)

	for i := 0; i < s.rsParityShard; i++ {
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
func (s *HTTP2Server) openInput(bucket string, object string) (ObjectToShardNodes, int64, error) {
	objectHash := model.ObjectHash(bucket, object)

	hashRingShards, err := s.hashRing.GetClosestN(objectHash[:], s.rsDataShard+s.rsParityShard)
	if err != nil {
		return ObjectToShardNodes{}, 0, err
	}

	data, err := s.stateGet(model.TableObjects, string(objectHash[:]))
	if err != nil {
		return ObjectToShardNodes{}, 0, err
	}

	var objectToShardNodes ObjectToShardNodes
	r := bytes.NewReader(data)
	dec := gob.NewDecoder(r) //nolint:gosec // G709: the input is state this gateway wrote, not client data.

	if err := dec.Decode(&objectToShardNodes); err != nil {
		return ObjectToShardNodes{}, 0, err
	}

	if len(hashRingShards) != (len(objectToShardNodes.DataShardNodes) + len(objectToShardNodes.ParityShardNodes)) {
		return ObjectToShardNodes{}, 0, errors.New("number of shards does not match number of hash ring shards")
	}

	return objectToShardNodes, objectToShardNodes.Size, nil
}

// shardReaders creates readers for each shard.
// Data is buffered into memory before connections are closed to avoid
// "connection closed" errors when the caller reads from the returned readers.
func (s *HTTP2Server) shardReaders(objectHash [32]byte, shards ObjectToShardNodes, parity bool) ([]io.Reader, error) {
	shardReaders := make([]io.Reader, len(shards.DataShardNodes)+len(shards.ParityShardNodes))

	totalNodes := make([]uint32, 0)
	totalNodes = append(totalNodes, shards.DataShardNodes...)

	if parity {
		totalNodes = append(totalNodes, shards.ParityShardNodes...)
	}

	for i := range totalNodes {
		nodeNum := int(totalNodes[i])

		objectRequest := storage.GetRequest{
			ObjectHash: objectHash,
			RangeStart: -1, // -1 means full shard (no range)
			RangeEnd:   -1,
			ShardIndex: uint32(i), // Include shard index for unique lookup
		}

		reader, err := s.shards.GetShard(context.Background(), nodeNum, objectRequest)
		if err != nil {
			slog.Error("Error reading shard from storage node", "node", nodeNum, "err", err)
			// Don't close - connection stays in pool
			return shardReaders, err
		}

		// Buffer the shard data into memory before closing the stream.
		// This prevents "stream closed" errors when the caller reads.
		data, err := io.ReadAll(reader)
		if closeErr := reader.Close(); closeErr != nil {
			slog.Debug("Failed to close shard stream reader", "node", nodeNum, "error", closeErr)
		}

		if err != nil {
			slog.Error("Error buffering shard data", "node", nodeNum, "err", err)
			return shardReaders, err
		}

		shardReaders[i] = bytes.NewReader(data)
	}

	return shardReaders, nil
}

// reconstructObject attempts to rebuild an object using parity shards.
func (s *HTTP2Server) reconstructObject(ctx context.Context, objectHash [32]byte, shards ObjectToShardNodes, enc reedsolomon.StreamEncoder, size int64) (*bytes.Buffer, error) {
	// Get all shard readers including parity
	shardReaders, err := s.shardReaders(objectHash, shards, true)
	if err != nil {
		return nil, err
	}

	// Create reconstruction writers for missing shards
	reconstruction := make([]io.Writer, len(shardReaders))
	files := make([]*os.File, len(shardReaders))

	for i := range reconstruction {
		if shardReaders[i] == nil {
			filename := fmt.Sprintf("%s.%d", hex.EncodeToString(objectHash[:]), i)
			outfn := filepath.Join(os.TempDir(), filename)

			files[i], err = os.Create(outfn)
			if err != nil {
				return nil, err
			}
			defer os.Remove(outfn)
			defer files[i].Close()

			slog.Info("Creating temporary file for reconstruction", "filename", outfn)
			reconstruction[i] = files[i]
		}
	}

	// Reconstruct missing shards
	err = enc.Reconstruct(shardReaders, reconstruction)
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
	shardReaders, err = s.shardReaders(objectHash, shards, true)
	if err != nil {
		return nil, err
	}

	// Fill in reconstructed shards
	for i := range shardReaders {
		if shardReaders[i] == nil && files[i] != nil {
			f, err := os.Open(files[i].Name())
			if err != nil {
				return nil, err
			}
			defer f.Close()
			shardReaders[i] = f
		}
	}

	// Join the shards
	var out bytes.Buffer
	err = enc.Join(&out, shardReaders, size)
	if err != nil {
		return nil, fmt.Errorf("join after reconstruction failed: %w", err)
	}

	return &out, nil
}

// deleteObjectViaQUIC sends DELETE requests to all shard nodes.
func (s *HTTP2Server) deleteObjectViaQUIC(ctx context.Context, bucket, key string, objectHash [32]byte, shards ObjectToShardNodes) error {
	// Build (node, shardIndex) pairs so each delete carries the correct shard index.
	type nodeShard struct {
		node       uint32
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

			delReq := storage.DeleteRequest{
				ObjectHash: objectHash,
				ShardIndex: uint32(ns.shardIndex), //nolint:gosec // G115: shardIndex bounded by rsDataShard + rsParityShard (small uint).
			}

			resp, err := s.shards.DeleteShard(ctx, int(ns.node), delReq)
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
