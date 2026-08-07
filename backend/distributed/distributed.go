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
	"path/filepath"
	"strconv"
	"strings"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash/v2"
	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/quic/quicserver"
	s3db "github.com/mulgadc/predastore/s3db"
)

// NodeConfig holds configuration for a single node.
type NodeConfig struct {
	ID     int
	Host   string
	Port   int
	Path   string
	DB     bool
	DBPort int
	DBPath string
	Leader bool
	Epoch  int
}

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
	// DataDir is the root directory for distributed node storage
	DataDir string

	// BadgerDir is the directory for the Badger KV database (used when DBClient is nil)
	BadgerDir string

	// Reed-Solomon configuration
	DataShards   int
	ParityShards int

	// Hash ring configuration
	PartitionCount    int
	ReplicationFactor int

	// QUIC server base port (each node uses BasePort + nodeNum)
	QuicBasePort int

	// Nodes configuration (from cluster.toml)
	Nodes []NodeConfig

	// Buckets configuration (from cluster.toml)
	Buckets []BucketConfig

	// DBClient holds configuration for the distributed database client
	// When set, uses distributed s3db for global state instead of local BadgerDB
	DBClient *DBClientConfig

	// ShardClient overrides how storage nodes are reached. Nil falls back
	// to pooled QUIC connections against per-node addresses.
	ShardClient ShardClient

	// StateClient overrides how global state is reached. Nil falls back to
	// the DBClient HTTP path (or local BadgerDB when that is nil too).
	StateClient StateClient
}

// Backend implements the distributed storage backend with Reed-Solomon erasure coding.
type Backend struct {
	config        *Config
	rsDataShard   int
	rsParityShard int
	hashRing      *consistent.Consistent
	dataDir       string
	badgerDir     string
	globalState   GlobalState // abstraction for global state storage (local or distributed)
	shards        ShardClient // transport for shard operations against storage nodes
	quicBasePort  int
	nodeAddrs     map[int]string // node ID -> "host:port"
	buckets       []BucketConfig // bucket configurations
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

// New creates a new distributed backend.
func New(config any) (backend.Backend, error) {
	cfg, ok := config.(*Config)
	if !ok {
		return nil, errors.New("invalid configuration type for distributed backend")
	}

	// Global state needs a source: an injected client, a DB cluster, or a
	// local BadgerDB.
	if cfg.StateClient == nil && cfg.DBClient == nil && cfg.BadgerDir == "" {
		return nil, errors.New("one of StateClient, DBClient or BadgerDir is required for distributed backend")
	}

	// Set defaults
	dataShards := cfg.DataShards
	if dataShards == 0 {
		dataShards = 3
	}
	parityShards := cfg.ParityShards
	if parityShards == 0 && dataShards != 1 {
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
	quicBasePort := cfg.QuicBasePort
	if quicBasePort == 0 {
		quicBasePort = 9991
	}

	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = filepath.Join("s3", "tests", "data", "distributed", "nodes")
	}

	// Create global state store (distributed or local)
	var globalState GlobalState
	var err error

	if cfg.StateClient != nil {
		// An injected client decides its own transport and topology.
		slog.Info("Using injected state client for global state")
		globalState = NewDistributedStateWithClient(cfg.StateClient)
	} else if cfg.DBClient != nil && len(cfg.DBClient.Nodes) > 0 {
		// Use distributed s3db cluster for global state
		slog.Info("Using distributed database for global state",
			"nodes", cfg.DBClient.Nodes,
			"region", cfg.DBClient.Region,
		)
		globalState, err = NewDistributedState(cfg.DBClient)
		if err != nil {
			return nil, fmt.Errorf("failed to create distributed state: %w", err)
		}
	} else {
		// Fallback to local BadgerDB
		slog.Info("Using local BadgerDB for global state", "path", cfg.BadgerDir)
		globalState, err = NewLocalState(cfg.BadgerDir)
		if err != nil {
			return nil, fmt.Errorf("failed to create local state: %w", err)
		}
	}

	// Create hash ring
	ringCfg := consistent.Config{
		PartitionCount:    partitionCount,
		ReplicationFactor: replicationFactor,
		Load:              1.25,
		Hasher:            hasher{},
	}
	hashRing := consistent.New(nil, ringCfg)

	// Add nodes to hash ring using config node IDs
	// This ensures hash ring node names match config node IDs (e.g., node-1, node-2, node-3)
	// Node directories are created by QUIC servers, not by the S3 server.
	if len(cfg.Nodes) > 0 {
		for _, node := range cfg.Nodes {
			hashRing.Add(myMember(fmt.Sprintf("node-%d", node.ID)))
		}
	} else {
		// Fallback for tests without config: use 0-indexed nodes
		for i := 0; i < partitionCount; i++ {
			hashRing.Add(myMember(fmt.Sprintf("node-%d", i)))
		}
	}

	// Build node address map from config
	nodeAddrs := make(map[int]string)
	for _, node := range cfg.Nodes {
		nodeAddrs[node.ID] = fmt.Sprintf("%s:%d", node.Host, node.Port)
	}

	b := &Backend{
		config:        cfg,
		rsDataShard:   dataShards,
		rsParityShard: parityShards,
		hashRing:      hashRing,
		dataDir:       dataDir,
		badgerDir:     cfg.BadgerDir,
		globalState:   globalState,
		quicBasePort:  quicBasePort,
		nodeAddrs:     nodeAddrs,
		buckets:       cfg.Buckets,
	}
	b.shards = cfg.ShardClient
	if b.shards == nil {
		b.shards = &quicShardClient{addr: b.getNodeAddr}
	}
	return b, nil
}

// Type returns the backend type identifier.
func (b *Backend) Type() string {
	return "distributed"
}

// Close cleans up resources.
func (b *Backend) Close() error {
	if b.globalState != nil {
		return b.globalState.Close()
	}
	return nil
}

// getNodeAddr returns the QUIC address for a node
// It uses the nodeAddrs map from config if available, otherwise falls back to computed address.
func (b *Backend) getNodeAddr(nodeNum int) string {
	if addr, ok := b.nodeAddrs[nodeNum]; ok {
		return addr
	}
	// Fallback to computed address for backward compatibility
	return fmt.Sprintf("127.0.0.1:%d", b.quicBasePort+nodeNum)
}

// DataDir returns the data directory (for testing).
func (b *Backend) DataDir() string {
	return b.dataDir
}

// SetDataDir sets the data directory (for testing).
func (b *Backend) SetDataDir(dir string) {
	b.dataDir = dir
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

// DB returns the local badger database (for testing/backward compatibility)
// Returns nil if using distributed state.
func (b *Backend) DB() *s3db.S3DB {
	if localState, ok := b.globalState.(*LocalState); ok {
		return localState.DB()
	}
	return nil
}

// GlobalState returns the global state interface.
func (b *Backend) GlobalState() GlobalState {
	return b.globalState
}

// openInput retrieves shard location metadata for an object.
func (b *Backend) openInput(bucket string, object string) (ObjectToShardNodes, int64, error) {
	key := s3db.GenObjectHash(bucket, object)

	hashRingShards, err := b.hashRing.GetClosestN(key[:], b.rsDataShard+b.rsParityShard)
	if err != nil {
		return ObjectToShardNodes{}, 0, err
	}

	objectHash := s3db.GenObjectHash(bucket, object)

	// Use GlobalState interface to get object metadata
	data, err := b.globalState.Get(TableObjects, objectHash[:])
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
func (b *Backend) shardReaders(ctx context.Context, bucket string, object string, shards ObjectToShardNodes, parity bool) ([]io.Reader, error) {
	shardReaders := make([]io.Reader, len(shards.DataShardNodes)+len(shards.ParityShardNodes))

	totalNodes := make([]uint32, 0)
	totalNodes = append(totalNodes, shards.DataShardNodes...)

	if parity {
		totalNodes = append(totalNodes, shards.ParityShardNodes...)
	}

	for i := range totalNodes {
		nodeNum := int(totalNodes[i])

		objectRequest := quicserver.ObjectRequest{
			Bucket:     bucket,
			Object:     object,
			RangeStart: -1, // -1 means full shard (no range)
			RangeEnd:   -1,
			ShardIndex: uint32(i), // Include shard index for unique lookup
		}

		reader, err := b.shards.GetShard(ctx, nodeNum, objectRequest)
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
