package distributed

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/klauspost/reedsolomon"
	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/quic/quicclient"
	"github.com/mulgadc/predastore/quic/quicserver"
	"github.com/mulgadc/predastore/s3/wal"
	s3db "github.com/mulgadc/predastore/s3db"
)

// Config holds distributed backend configuration
type Config struct {
	// DataDir is the root directory for distributed node storage
	DataDir string

	// BadgerDir is the directory for the Badger KV database
	BadgerDir string

	// Reed-Solomon configuration
	DataShards   int
	ParityShards int

	// Hash ring configuration
	PartitionCount    int
	ReplicationFactor int

	// QUIC server base port (each node uses BasePort + nodeNum)
	QuicBasePort int
}

// Backend implements the distributed storage backend with Reed-Solomon erasure coding
type Backend struct {
	config        *Config
	rsDataShard   int
	rsParityShard int
	hashRing      *consistent.Consistent
	dataDir       string
	badgerDir     string
	db            *s3db.S3DB
	quicBasePort  int
}

// ObjectToShardNodes maps an object to its shard locations
type ObjectToShardNodes struct {
	Object           [32]byte
	Size             int64
	DataShardNodes   []uint32
	ParityShardNodes []uint32
}

// ObjectShardReader provides access to a shard stored in WAL
type ObjectShardReader struct {
	File        *os.File
	WALFileInfo wal.WALFileInfo
}

// hasher implements consistent.Hasher using xxhash
type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

// myMember implements consistent.Member
type myMember string

func (m myMember) String() string {
	return string(m)
}

// shardWriteOutcome captures the result of writing a shard to WAL
type shardWriteOutcome struct {
	shardIndex int
	result     *wal.WriteResult
	err        error
}

// New creates a new distributed backend
func New(config interface{}) (backend.Backend, error) {
	cfg, ok := config.(*Config)
	if !ok {
		return nil, errors.New("invalid configuration type for distributed backend")
	}

	if cfg.BadgerDir == "" {
		return nil, errors.New("badger directory is required for distributed backend")
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
	quicBasePort := cfg.QuicBasePort
	if quicBasePort == 0 {
		quicBasePort = 9991
	}

	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = filepath.Join("s3", "tests", "data", "distributed", "nodes")
	}

	// Create Badger DB
	db, err := s3db.New(cfg.BadgerDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create badger db: %w", err)
	}

	// Create hash ring
	ringCfg := consistent.Config{
		PartitionCount:    partitionCount,
		ReplicationFactor: replicationFactor,
		Load:              1.25,
		Hasher:            hasher{},
	}
	hashRing := consistent.New(nil, ringCfg)

	// Create node directories and add nodes to ring
	for i := 0; i < partitionCount; i++ {
		nodeDir := filepath.Join(dataDir, fmt.Sprintf("node-%d", i))
		if err := os.MkdirAll(nodeDir, 0750); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to create node directory: %w", err)
		}
		hashRing.Add(myMember(fmt.Sprintf("node-%d", i)))
	}

	return &Backend{
		config:        cfg,
		rsDataShard:   dataShards,
		rsParityShard: parityShards,
		hashRing:      hashRing,
		dataDir:       dataDir,
		badgerDir:     cfg.BadgerDir,
		db:            db,
		quicBasePort:  quicBasePort,
	}, nil
}

// Type returns the backend type identifier
func (b *Backend) Type() string {
	return "distributed"
}

// Close cleans up resources
func (b *Backend) Close() error {
	if b.db != nil {
		return b.db.Close()
	}
	return nil
}

// nodeDir returns the directory for a given node
func (b *Backend) nodeDir(node string) string {
	if b.dataDir == "" {
		return filepath.Join("s3", "tests", "data", "distributed", "nodes", node)
	}
	return filepath.Join(b.dataDir, node)
}

// DataDir returns the data directory (for testing)
func (b *Backend) DataDir() string {
	return b.dataDir
}

// SetDataDir sets the data directory (for testing)
func (b *Backend) SetDataDir(dir string) {
	b.dataDir = dir
}

// RsDataShard returns the number of data shards (for testing)
func (b *Backend) RsDataShard() int {
	return b.rsDataShard
}

// RsParityShard returns the number of parity shards (for testing)
func (b *Backend) RsParityShard() int {
	return b.rsParityShard
}

// HashRing returns the hash ring (for testing)
func (b *Backend) HashRing() *consistent.Consistent {
	return b.hashRing
}

// DB returns the badger database (for testing)
func (b *Backend) DB() *s3db.S3DB {
	return b.db
}

// putObjectToWAL splits a file into RS shards and writes each to the appropriate node's WAL
func (b *Backend) putObjectToWAL(bucket string, objectPath string, objectHash [32]byte) (dataResults []*wal.WriteResult, parityResults []*wal.WriteResult, size int64, err error) {
	enc, err := reedsolomon.NewStream(b.rsDataShard, b.rsParityShard)
	if err != nil {
		return nil, nil, 0, err
	}

	f, err := os.Open(objectPath)
	if err != nil {
		return nil, nil, 0, err
	}
	defer f.Close()

	instat, err := f.Stat()
	if err != nil {
		return nil, nil, 0, err
	}

	size = instat.Size()

	_, file := filepath.Split(objectPath)
	key := []byte(fmt.Sprintf("%s/%s", bucket, file))

	hashRingShards, err := b.hashRing.GetClosestN(key, b.rsDataShard+b.rsParityShard)
	if err != nil {
		return nil, nil, 0, err
	}

	totalShards := b.rsDataShard + b.rsParityShard
	walFiles := make([]*wal.WAL, totalShards)
	for i := range walFiles {
		walDir := b.nodeDir(hashRingShards[i].String())
		if mkErr := os.MkdirAll(walDir, 0750); mkErr != nil {
			return nil, nil, 0, mkErr
		}
		walFiles[i], err = wal.New(filepath.Join(walDir, "state.json"), walDir)
		if err != nil {
			return nil, nil, 0, err
		}
	}
	defer func() {
		for i := range walFiles {
			if walFiles[i] != nil {
				_ = walFiles[i].Close()
				_ = walFiles[i].DB.Close()
			}
		}
	}()

	// Calculate shard size
	fileSize := instat.Size()
	ds := int64(b.rsDataShard)
	shardSize := int((fileSize + ds - 1) / ds)

	// Split input -> data shard writers
	dataWriters := make([]io.Writer, b.rsDataShard)
	dataPipeWriters := make([]*io.PipeWriter, b.rsDataShard)
	dataResults = make([]*wal.WriteResult, b.rsDataShard)
	dataCh := make(chan shardWriteOutcome, b.rsDataShard)
	var dataWG sync.WaitGroup

	for i := 0; i < b.rsDataShard; i++ {
		pr, pw := io.Pipe()
		dataPipeWriters[i] = pw
		dataWriters[i] = pw

		dataWG.Add(1)
		go func(idx int, r *io.PipeReader) {
			defer dataWG.Done()
			res, werr := walFiles[idx].Write(r, shardSize)

			if werr == nil {
				walFiles[idx].UpdateObjectToWAL(objectHash, res)
			}

			dataCh <- shardWriteOutcome{shardIndex: idx, result: res, err: werr}
		}(i, pr)
	}

	splitErr := enc.Split(f, dataWriters, fileSize)

	for i := 0; i < b.rsDataShard; i++ {
		if splitErr != nil {
			_ = dataPipeWriters[i].CloseWithError(splitErr)
		} else {
			_ = dataPipeWriters[i].Close()
		}
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
		dataResults[outcome.shardIndex] = outcome.result
	}
	if splitErr != nil && firstErr == nil {
		firstErr = splitErr
	}
	if firstErr != nil {
		return nil, nil, 0, firstErr
	}

	// Encode parity shards
	dataReaders := make([]io.Reader, b.rsDataShard)
	for i := 0; i < b.rsDataShard; i++ {
		bdata, rerr := walFiles[i].ReadFromWriteResult(dataResults[i])
		if rerr != nil {
			return nil, nil, 0, rerr
		}
		dataReaders[i] = bytes.NewReader(bdata)
	}

	parityWriters := make([]io.Writer, b.rsParityShard)
	parityPipeWriters := make([]*io.PipeWriter, b.rsParityShard)
	parityResults = make([]*wal.WriteResult, b.rsParityShard)
	parityCh := make(chan shardWriteOutcome, b.rsParityShard)
	var parityWG sync.WaitGroup

	for i := 0; i < b.rsParityShard; i++ {
		pr, pw := io.Pipe()
		parityPipeWriters[i] = pw
		parityWriters[i] = pw

		walIdx := b.rsDataShard + i
		parityWG.Add(1)
		go func(localParityIdx int, walIndex int, r *io.PipeReader) {
			defer parityWG.Done()
			res, werr := walFiles[walIndex].Write(r, shardSize)
			parityCh <- shardWriteOutcome{shardIndex: localParityIdx, result: res, err: werr}

			if werr == nil {
				walFiles[walIndex].UpdateObjectToWAL(objectHash, res)
			}
		}(i, walIdx, pr)
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
		parityResults[outcome.shardIndex] = outcome.result
	}
	if encodeErr != nil && firstErr == nil {
		firstErr = encodeErr
	}
	if firstErr != nil {
		return nil, nil, 0, firstErr
	}

	return dataResults, parityResults, size, nil
}

// openInput retrieves shard location metadata for an object
func (b *Backend) openInput(bucket string, object string) (ObjectToShardNodes, int64, error) {
	key := []byte(fmt.Sprintf("%s/%s", bucket, object))

	hashRingShards, err := b.hashRing.GetClosestN(key, b.rsDataShard+b.rsParityShard)
	if err != nil {
		return ObjectToShardNodes{}, 0, err
	}

	objectHash := s3db.GenObjectHash(bucket, object)

	data, err := b.db.Get(objectHash[:])
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

// shardReaders creates readers for each shard via QUIC
func (b *Backend) shardReaders(bucket string, object string, shards ObjectToShardNodes, parity bool) ([]io.Reader, error) {
	shardReaders := make([]io.Reader, len(shards.DataShardNodes)+len(shards.ParityShardNodes))

	totalNodes := make([]uint32, 0)
	totalNodes = append(totalNodes, shards.DataShardNodes...)

	if parity {
		totalNodes = append(totalNodes, shards.ParityShardNodes...)
	}

	for i := range totalNodes {
		nodeNum := int(totalNodes[i])
		c, err := quicclient.Dial(context.Background(), fmt.Sprintf("127.0.0.1:%d", b.quicBasePort+nodeNum))
		if err != nil {
			slog.Error("Failed to dial QUIC server", "node", nodeNum, "err", err)
			continue
		}
		defer c.Close()

		objectRequest := quicserver.ObjectRequest{
			Bucket: bucket,
			Object: object,
		}

		shardReaders[i], err = c.Get(context.Background(), objectRequest)
		if err != nil {
			slog.Error("Error reading from QUIC server", "node", nodeNum, "err", err)
			return shardReaders, err
		}
	}

	return shardReaders, nil
}

// NodeToUint32 converts a node name to uint32
func NodeToUint32(value string) (uint32, error) {
	s := strings.Replace(value, "node-", "", 1)
	vint, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}
	return uint32(vint), nil
}

func init() {
	// Register the distributed backend with the default registry
	backend.Register("distributed", New)
}
