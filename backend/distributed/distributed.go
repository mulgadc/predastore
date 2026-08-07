package distributed

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash/v2"
	"github.com/klauspost/reedsolomon"
	"github.com/mulgadc/predastore/internal/gateway/model"
	"github.com/mulgadc/predastore/internal/state"
	"github.com/mulgadc/predastore/internal/storage"
)

// BucketConfig holds configuration for a bucket.
type BucketConfig struct {
	Name      string
	Region    string
	Type      string
	Public    bool
	AccountID string
}

// Config holds distributed backend configuration.
type Config struct {
	// Reed-Solomon configuration
	DataShards   int
	ParityShards int

	// Hash ring configuration
	PartitionCount    int
	ReplicationFactor int

	// StorageNodes are the ids of the nodes the hash ring places shards on.
	StorageNodes []int

	// Buckets configuration (from cluster.toml)
	Buckets []BucketConfig

	// Storage reaches the storage nodes holding shards. Required.
	Storage *storage.Client

	// State reaches the state replicas holding global state. Required.
	State *state.Client
}

// Backend implements the distributed storage backend with Reed-Solomon erasure coding.
type Backend struct {
	rsDataShard   int
	rsParityShard int
	hashRing      *consistent.Consistent
	globalState   *state.Client   // global state held by the state replicas
	shards        *storage.Client // shard operations against storage nodes
	buckets       []BucketConfig  // bucket configurations
}

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

// shardWriteOutcome captures the result of writing a shard via QUIC.
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

// New creates a new distributed backend. Both clients and a non-empty
// storage node set are required: the backend owns neither transport and
// cannot place a shard without a ring to place it on.
func New(cfg *Config) (*Backend, error) {
	if cfg == nil {
		return nil, errors.New("distributed backend: config is required")
	}
	if cfg.State == nil {
		return nil, errors.New("distributed backend: State client is required")
	}
	if cfg.Storage == nil {
		return nil, errors.New("distributed backend: Storage client is required")
	}
	if len(cfg.StorageNodes) == 0 {
		return nil, errors.New("distributed backend: at least one storage node is required")
	}

	// Set defaults
	dataShards := cfg.DataShards
	if dataShards == 0 {
		dataShards = 3
	}
	parityShards := cfg.ParityShards
	if parityShards == 0 {
		parityShards = 2
	}
	partitionCount := cfg.PartitionCount
	if partitionCount == 0 {
		partitionCount = 5
	}
	replicationFactor := cfg.ReplicationFactor
	if replicationFactor == 0 {
		replicationFactor = 100
	}

	// Create hash ring
	ringCfg := consistent.Config{
		PartitionCount:    partitionCount,
		ReplicationFactor: replicationFactor,
		Load:              1.25,
		Hasher:            hasher{},
	}
	hashRing := consistent.New(nil, ringCfg)

	// Ring member names carry the node id, so placement resolves straight to
	// the id the storage client addresses.
	for _, id := range cfg.StorageNodes {
		hashRing.Add(myMember(fmt.Sprintf("node-%d", id)))
	}

	return &Backend{
		rsDataShard:   dataShards,
		rsParityShard: parityShards,
		hashRing:      hashRing,
		globalState:   cfg.State,
		shards:        cfg.Storage,
		buckets:       cfg.Buckets,
	}, nil
}

// Type returns the backend type identifier.
func (b *Backend) Type() string {
	return "distributed"
}

// RsDataShard returns the number of data shards (for testing).
func (b *Backend) RsDataShard() int {
	return b.rsDataShard
}

// RsParityShard returns the number of parity shards (for testing).
func (b *Backend) RsParityShard() int {
	return b.rsParityShard
}

// HashRing returns the hash ring (for testing).
func (b *Backend) HashRing() *consistent.Consistent {
	return b.hashRing
}

// putObjectViaQUIC splits a file into RS shards and sends each to the
// appropriate node via QUIC. poolNearFull is set if any shard's target node
// reported pressure.
func (b *Backend) putObjectViaQUIC(ctx context.Context, objectPath string, objectHash [32]byte) (size int64, poolNearFull bool, err error) {
	enc, err := reedsolomon.NewStream(b.rsDataShard, b.rsParityShard)
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
	hashRingShards, err := b.hashRing.GetClosestN(objectHash[:], b.rsDataShard+b.rsParityShard)
	if err != nil {
		return 0, false, err
	}

	// Calculate shard size
	fileSize := instat.Size()
	ds := int64(b.rsDataShard)
	shardSize := int((fileSize + ds - 1) / ds)

	// Step 1: Split file into data shard buffers (in memory)
	// This allows us to both send to QUIC and use for parity encoding
	dataShardBuffers := make([][]byte, b.rsDataShard)
	dataWriters := make([]io.Writer, b.rsDataShard)
	for i := 0; i < b.rsDataShard; i++ {
		dataShardBuffers[i] = make([]byte, 0, shardSize)
		dataWriters[i] = &bytesBufferWriter{buf: &dataShardBuffers[i]}
	}

	if splitErr := enc.Split(f, dataWriters, fileSize); splitErr != nil {
		return 0, false, splitErr
	}

	// Step 2: Send data shards to nodes via QUIC
	dataCh := make(chan shardWriteOutcome, b.rsDataShard)
	var dataWG sync.WaitGroup

	for i := 0; i < b.rsDataShard; i++ {
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

			resp, putErr := b.shards.PutShard(ctx, int(nodeNum), putReq, bytes.NewReader(shardData))
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
	dataReaders := make([]io.Reader, b.rsDataShard)
	for i := 0; i < b.rsDataShard; i++ {
		dataReaders[i] = bytes.NewReader(dataShardBuffers[i])
	}

	parityWriters := make([]io.Writer, b.rsParityShard)
	parityPipeWriters := make([]*io.PipeWriter, b.rsParityShard)
	parityCh := make(chan shardWriteOutcome, b.rsParityShard)
	var parityWG sync.WaitGroup

	for i := 0; i < b.rsParityShard; i++ {
		pr, pw := io.Pipe()
		parityPipeWriters[i] = pw
		parityWriters[i] = pw

		parityIdx := b.rsDataShard + i
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

			resp, putErr := b.shards.PutShard(ctx, int(nodeNum), putReq, r)
			if putErr != nil {
				slog.Error("putObjectViaQUIC: put parity failed", "node", nodeNum, "error", putErr)
				parityCh <- shardWriteOutcome{shardIndex: localParityIdx, err: putErr}
				return
			}

			parityCh <- shardWriteOutcome{shardIndex: localParityIdx, shardSize: resp.ShardSize, poolNearFull: resp.PoolNearFull}
		}(i, parityIdx, pr)
	}

	encodeErr := enc.Encode(dataReaders, parityWriters)

	for i := 0; i < b.rsParityShard; i++ {
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
func (b *Backend) openInput(bucket string, object string) (ObjectToShardNodes, int64, error) {
	objectHash := model.ObjectHash(bucket, object)

	hashRingShards, err := b.hashRing.GetClosestN(objectHash[:], b.rsDataShard+b.rsParityShard)
	if err != nil {
		return ObjectToShardNodes{}, 0, err
	}

	data, err := b.stateGet(model.TableObjects, string(objectHash[:]))
	if err != nil {
		return ObjectToShardNodes{}, 0, err
	}

	var objectToShardNodes ObjectToShardNodes
	r := bytes.NewReader(data)
	dec := gob.NewDecoder(r)

	if err := dec.Decode(&objectToShardNodes); err != nil {
		return ObjectToShardNodes{}, 0, err
	}

	if len(hashRingShards) != (len(objectToShardNodes.DataShardNodes) + len(objectToShardNodes.ParityShardNodes)) {
		return ObjectToShardNodes{}, 0, errors.New("number of shards does not match number of hash ring shards")
	}

	return objectToShardNodes, objectToShardNodes.Size, nil
}

// shardReaders creates readers for each shard via QUIC.
// Data is buffered into memory before connections are closed to avoid
// "connection closed" errors when the caller reads from the returned readers.
func (b *Backend) shardReaders(objectHash [32]byte, shards ObjectToShardNodes, parity bool) ([]io.Reader, error) {
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

		reader, err := b.shards.GetShard(context.Background(), nodeNum, objectRequest)
		if err != nil {
			slog.Error("Error reading from QUIC server", "node", nodeNum, "err", err)
			// Don't close - connection stays in pool
			return shardReaders, err
		}

		// Buffer the shard data into memory before closing the stream.
		// This prevents "stream closed" errors when the caller reads.
		data, err := io.ReadAll(reader)
		if closeErr := reader.Close(); closeErr != nil {
			slog.Debug("Failed to close QUIC stream reader", "node", nodeNum, "error", closeErr)
		}

		if err != nil {
			slog.Error("Error buffering shard data", "node", nodeNum, "err", err)
			return shardReaders, err
		}

		shardReaders[i] = bytes.NewReader(data)
	}

	return shardReaders, nil
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
