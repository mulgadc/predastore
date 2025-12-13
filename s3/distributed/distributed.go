package distributed

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/davecgh/go-spew/spew"
	"github.com/dgraph-io/badger/v4"
	"github.com/gofiber/fiber/v2"
	"github.com/klauspost/reedsolomon"
	"github.com/mulgadc/predastore/s3"
	"github.com/mulgadc/predastore/s3/wal"
	s3db "github.com/mulgadc/predastore/s3db"
)

type Backend struct {
	Config        s3.Config
	RsDataShard   int
	RsParityShard int
	HashRing      *consistent.Consistent
	// DataDir is the root directory for distributed node storage.
	// Each node will have its own sub-directory inside DataDir.
	// Example: <DataDir>/node-0
	DataDir string

	// KV dir
	BadgerDir string

	// Badger DB for local metadata
	DB *s3db.S3DB
}

type Node struct {
	Id string
}

type ObjectToShardNodes struct {
	Object           [32]byte
	Size             int64
	DataShardNodes   []uint32
	ParityShardNodes []uint32
}

type ObjectShardReader struct {
	File *os.File

	WALFileInfo wal.WALFileInfo
}

// consistent package doesn't provide a default hashing function.
// You should provide a proper one to distribute keys/members uniformly.
type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	// you should use a proper hash function for uniformity.
	return xxhash.Sum64(data)
}

// Add nodes to the hash ring
type myMember string

func (m myMember) String() string {
	return string(m)
}

type StdoutWriter struct{}

func (w StdoutWriter) Write(p []byte) (int, error) {
	return os.Stdout.Write(p)
}

func New(config interface{}) (svc *Backend, err error) {
	svc = &Backend{
		Config: s3.Config{},

		RsDataShard:   3,
		RsParityShard: 2,
		DataDir:       filepath.Join("s3", "tests", "data", "distributed", "nodes"),

		BadgerDir: config.(Backend).BadgerDir,
	}

	if svc.BadgerDir == "" {
		return nil, errors.New("badger directory is required to save shard/WAL state")
	}

	// Create badger DB
	svc.DB, err = s3db.New(svc.BadgerDir)

	if err != nil {
		return nil, err
	}

	// Create a new consistent instance
	cfg := consistent.Config{
		PartitionCount:    11,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            hasher{},
	}

	svc.HashRing = consistent.New(nil, cfg)

	// Create necessary directories for distributed storage (for testing purposes)
	for i := range cfg.PartitionCount {
		os.MkdirAll(filepath.Join(svc.DataDir, fmt.Sprintf("node-%d", i)), 0750)
	}

	for i := range cfg.PartitionCount {
		svc.HashRing.Add(myMember(fmt.Sprintf("node-%d", i)))
	}

	return svc, nil
}

func (backend Backend) nodeDir(node string) string {
	if backend.DataDir == "" {
		return filepath.Join("s3", "tests", "data", "distributed", "nodes", node)
	}
	return filepath.Join(backend.DataDir, node)
}

func (backend Backend) Delete(bucket string, object string, c *fiber.Ctx) (err error) {

	return

}

func (backend Backend) GetObjectHead(bucket string, object string, c *fiber.Ctx) (err error) {

	return

}

func (backend Backend) Get(bucket string, object string, out io.Writer, ctx *fiber.Ctx) (err error) {

	// Simple example
	/*
		// Create matrix
		enc, err := reedsolomon.New(backend.RsDataShard, backend.RsParityShard)
		checkErr(err)

		// Create shards and load the data.
		shards := make([][]byte, backend.RsDataShard+backend.RsParityShard)

		key := []byte(fmt.Sprintf("%s/%s", bucket, object))

		// calculates partition id for the given key
		// partID := hash(key) % partitionCount
		// the partitions are already distributed among members by Add function.
		owner := backend.HashRing.LocateKey(key)
		fmt.Println(owner.String())

		// Next, get the data and parity shards from the owner node
		hashRingShards, err := backend.HashRing.GetClosestN(key, backend.RsDataShard+backend.RsParityShard)

		for i := range hashRingShards {

			fmt.Println(i, hashRingShards[i].String())

			infn := filepath.Join(backend.nodeDir(hashRingShards[i].String()), fmt.Sprintf("%s.%d", object, i))
			fmt.Println("Opening", infn)
			shards[i], err = os.ReadFile(infn)
			if err != nil {
				fmt.Println("Error reading file", err)
				shards[i] = nil
			}

		}

		//os.Exit(1)

		// Verify the shards
		ok, err := enc.Verify(shards)
		if ok {
			fmt.Println("No reconstruction needed")
		} else {
			fmt.Println("Verification failed. Reconstructing data")

			err = enc.Reconstruct(shards)
			if err != nil {
				fmt.Println("Reconstruct failed -", err)
				os.Exit(1)
			}
			ok, err = enc.Verify(shards)
			if !ok {
				fmt.Println("Verification failed after reconstruction, data likely corrupted.")
				os.Exit(1)
			}
			checkErr(err)
		}

		// Join the shards and write them
		outfn := fmt.Sprintf("%s/%s", os.TempDir(), object)

		fmt.Println("Writing data to", outfn)
		f, err := os.Create(outfn)
		checkErr(err)

		// We don't know the exact filesize.
		err = enc.Join(f, shards, len(shards[0])*backend.RsDataShard)
		checkErr(err)

	*/

	// Streaming example

	// Create matrix
	enc, err := reedsolomon.NewStream(backend.RsDataShard, backend.RsParityShard)
	checkErr(err)

	// First, query which nodes have our object shards
	shards, size, err := backend.openInput(bucket, object)

	fmt.Println("Open input size", size)
	checkErr(err)

	// Next, loop through each node, create a WAL instance, and read the shard from the WAL
	// TODO: Optimise
	shardReaders := make([]io.Reader, len(shards.DataShardNodes)+len(shards.ParityShardNodes))

	totalNodes := make([]uint32, 0)

	totalNodes = append(totalNodes, shards.DataShardNodes...)
	//totalNodes = append(totalNodes, shards.ParityShardNodes...)

	// TODO: Check parity written
	//totalNodes = append(totalNodes, shards.ParityShardNodes...)

	// TODO: Optimise, validate data shards are correct, before parity and validation to improve performance
	for i := range totalNodes {
		nodeDir := backend.nodeDir(fmt.Sprintf("node-%d", totalNodes[i]))

		fmt.Println("nodeDir", nodeDir)

		walInstance, err := wal.New("", nodeDir)

		if err != nil {
			return err
		}

		objectHash := genObjectHash(bucket, object)
		// Query local node, where does the shard belong?
		result, err := walInstance.DB.Get(objectHash[:])
		if err != nil {
			fmt.Println("Shard not found")
			continue
			//			return err
		}

		var objectWriteResult wal.ObjectWriteResult

		r := bytes.NewReader(result)
		dec := gob.NewDecoder(r)
		if err := dec.Decode(&objectWriteResult); err != nil {
			return err
		}

		shardReaders[i], err = walInstance.ReadFromWriteResultStream(&objectWriteResult.WriteResult)

		if err != nil {
			fmt.Println("Error reading from write result stream", err)
			return err
		}

		walInstance.Close()
	}

	// Verify the shards
	/*
		ok, err := enc.Verify(shardReaders)

		if ok {
			fmt.Println("No reconstruction needed")
		} else {
			fmt.Println("Verification failed. Reconstructing data")

			// TODO: Refactor

				shards, size, err = backend.openInput(bucket, object)
				checkErr(err)

				key := []byte(fmt.Sprintf("%s/%s", bucket, object))

				// calculates partition id for the given key
				// partID := hash(key) % partitionCount
				// the partitions are already distributed among members by Add function.
				owner := backend.HashRing.LocateKey(key)
				fmt.Println(owner.String())

				// Next, get the data and parity shards from the owner node
				hashRingShards, err := backend.HashRing.GetClosestN(key, backend.RsDataShard+backend.RsParityShard)

				// Create out destination writers
				out := make([]io.Writer, len(shards))
				for i := range out {
					if shards[i] == nil {

						outfn := fmt.Sprintf("%s.%d", object, i)

						fmt.Println("Writing to", outfn)
						out[i], err = os.Create(filepath.Join(backend.nodeDir(hashRingShards[i].String()), outfn))
						checkErr(err)
					}
				}
				err = enc.Reconstruct(shards, out)
				if err != nil {
					fmt.Println("Reconstruct failed -", err)
					os.Exit(1)
				}
				// Close output.
				for i := range out {
					if out[i] != nil {
						err := out[i].(*os.File).Close()
						checkErr(err)
					}
				}
				shards, size, err = backend.openInput(bucket, object)
				ok, err = enc.Verify(shards)
				if !ok {
					fmt.Println("Verification failed after reconstruction, data likely corrupted:", err)
					os.Exit(1)
				}
				checkErr(err)

		}

	*/

	//f, err := os.Create(outfn)
	//checkErr(err)

	//shards, size, err = backend.openInput(bucket, object)
	//shardReaders[i], _ = walInstance.ReadFromWriteResultStream(&results[i])
	//checkErr(err)

	// We don't know the exact filesize.
	//std := StdoutWriter{}

	err = enc.Join(out, shardReaders, shards.Size)

	fmt.Println("shard num", len(shardReaders))
	fmt.Println(shards.Size)
	fmt.Println(err)
	checkErr(err)

	return
}

func (backend Backend) ListBuckets(c *fiber.Ctx) (err error) {

	return

}

func (backend Backend) ListObjectsV2Handler(bucket string, c *fiber.Ctx) (err error) {

	return

}

func (backend Backend) CompleteMultipartUpload(bucket string, object string, uploadId string, c *fiber.Ctx) (err error) {

	return

}

func (backend Backend) PutObjectPart(bucket string, object string, partNumber int, uploadId string, c *fiber.Ctx) {

}

type shardWriteOutcome struct {
	shardIndex int
	result     *wal.WriteResult
	err        error
}

func (backend Backend) PutObject(bucket string, object string, c *fiber.Ctx) (err error) {

	objectHash := genObjectHash(bucket, object)

	objectToShardNodes := ObjectToShardNodes{}

	// Check if existing
	data, err := backend.DB.Get(objectHash[:])

	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return err
	} else if errors.Is(err, badger.ErrKeyNotFound) {

		// Set the defaults
		objectToShardNodes = ObjectToShardNodes{
			Object:           objectHash,
			DataShardNodes:   make([]uint32, backend.RsDataShard),
			ParityShardNodes: make([]uint32, backend.RsParityShard),
		}
	} else {

		r := bytes.NewReader(data)
		dec := gob.NewDecoder(r)

		if err := dec.Decode(&objectToShardNodes); err != nil {
			return err
		}

		spew.Dump(data)
		spew.Dump(objectToShardNodes)

	}

	//objectSha256 := hex.EncodeToString(hashSha256[:])

	dataRes, parityRes, size, err := backend.putObjectToWAL(bucket, object, objectHash)
	if err != nil {
		return err
	}

	objectToShardNodes.Size = size

	_, file := filepath.Split(object)
	key := []byte(fmt.Sprintf("%s/%s", bucket, file))
	hashRingShards, _ := backend.HashRing.GetClosestN(key, backend.RsDataShard+backend.RsParityShard)

	// Print the WAL location results for now (Badger KV later).
	for i := 0; i < backend.RsDataShard; i++ {
		fmt.Printf("put_object wal_write data_shard=%d node=%s write_result=%#v\n",
			i, hashRingShards[i].String(), dataRes[i])

		objectToShardNodes.DataShardNodes[i], err = NodeToUint32(hashRingShards[i].String())

		if err != nil {
			return err
		}

	}
	for i := 0; i < backend.RsParityShard; i++ {
		fmt.Printf("put_object wal_write parity_shard=%d node=%s write_result=%#v\n",
			i, hashRingShards[backend.RsDataShard+i].String(), parityRes[i])

		objectToShardNodes.ParityShardNodes[i], err = NodeToUint32(hashRingShards[backend.RsDataShard+i].String())

		if err != nil {
			return err
		}

	}

	err = backend.DB.Badger.Update(func(txn *badger.Txn) error {

		// Marshal objectToShardNodes to []byte
		/*
			objectToShardNodesBytes, err := json.Marshal(objectToShardNodes)
			if err != nil {
				return err
			}
		*/

		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(objectToShardNodes); err != nil {
			return err
		}

		//err = txn.Set(objectHash[:], objectToShardNodesBytes)
		e := badger.NewEntry(objectHash[:], buf.Bytes())
		err = txn.SetEntry(e)

		return err
	})

	return err

}

// Close, cleanup
func (backend Backend) Close() (err error) {

	return
}

// Private methods

// putObjectToWAL splits an on-disk file into RS shards and writes each shard to the WAL
// for the node selected by the hash ring. It returns the WAL WriteResults for data and parity shards.
func (backend Backend) putObjectToWAL(bucket string, objectPath string, objectHash [32]byte) (dataResults []*wal.WriteResult, parityResults []*wal.WriteResult, size int64, err error) {
	// Stream encoder
	enc, err := reedsolomon.NewStream(backend.RsDataShard, backend.RsParityShard)
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

	hashRingShards, err := backend.HashRing.GetClosestN(key, backend.RsDataShard+backend.RsParityShard)
	if err != nil {
		return nil, nil, 0, err
	}

	totalShards := backend.RsDataShard + backend.RsParityShard
	walFiles := make([]*wal.WAL, totalShards)
	for i := range walFiles {
		walDir := backend.nodeDir(hashRingShards[i].String())
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
				// Close the Badger DB
				_ = walFiles[i].DB.Close()
			}
		}
	}()

	// Each data shard is ceil(fileSize / dataShards) bytes (RS pads as needed).
	fileSize := instat.Size()
	ds := int64(backend.RsDataShard)
	shardSize := int((fileSize + ds - 1) / ds)

	// 1) Split input -> data shard writers (pipes) -> per-node WAL writers (wal.Write reads from pipes).
	dataWriters := make([]io.Writer, backend.RsDataShard)
	dataPipeWriters := make([]*io.PipeWriter, backend.RsDataShard)

	dataResults = make([]*wal.WriteResult, backend.RsDataShard)
	dataCh := make(chan shardWriteOutcome, backend.RsDataShard)
	var dataWG sync.WaitGroup

	for i := 0; i < backend.RsDataShard; i++ {
		pr, pw := io.Pipe()
		dataPipeWriters[i] = pw
		dataWriters[i] = pw

		dataWG.Add(1)
		go func(idx int, r *io.PipeReader) {
			defer dataWG.Done()
			res, werr := walFiles[idx].Write(r, shardSize)

			// Write the object hash, to where it is stored on disk, to the WAL log of the node
			if werr == nil {
				walFiles[idx].UpdateObjectToWAL(objectHash, res)
				if err != nil {
					werr = err
				}
			}

			dataCh <- shardWriteOutcome{shardIndex: idx, result: res, err: werr}
		}(i, pr)
	}

	splitErr := enc.Split(f, dataWriters, fileSize)

	// Close all writers to unblock WAL readers.
	for i := 0; i < backend.RsDataShard; i++ {
		if splitErr != nil {
			_ = dataPipeWriters[i].CloseWithError(splitErr)
		} else {
			_ = dataPipeWriters[i].Close()
		}
	}

	// Wait for all WAL goroutines to send results, then close channel.
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

	// 2) Encode parity using the data shards we just wrote (read back) -> parity pipes -> parity WAL writes.
	dataReaders := make([]io.Reader, backend.RsDataShard)
	for i := 0; i < backend.RsDataShard; i++ {
		b, rerr := walFiles[i].ReadFromWriteResult(dataResults[i])
		if rerr != nil {
			return nil, nil, 0, rerr
		}
		dataReaders[i] = bytes.NewReader(b)
	}

	parityWriters := make([]io.Writer, backend.RsParityShard)
	parityPipeWriters := make([]*io.PipeWriter, backend.RsParityShard)
	parityResults = make([]*wal.WriteResult, backend.RsParityShard)

	parityCh := make(chan shardWriteOutcome, backend.RsParityShard)
	var parityWG sync.WaitGroup
	for i := 0; i < backend.RsParityShard; i++ {
		pr, pw := io.Pipe()
		parityPipeWriters[i] = pw
		parityWriters[i] = pw

		walIdx := backend.RsDataShard + i
		parityWG.Add(1)
		go func(localParityIdx int, walIndex int, r *io.PipeReader) {
			defer parityWG.Done()
			res, werr := walFiles[walIndex].Write(r, shardSize)
			parityCh <- shardWriteOutcome{shardIndex: localParityIdx, result: res, err: werr}
		}(i, walIdx, pr)
	}

	encodeErr := enc.Encode(dataReaders, parityWriters)

	for i := 0; i < backend.RsParityShard; i++ {
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

func checkErr(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s", err.Error())
		os.Exit(2)
	}
}

func (backend Backend) openInput(bucket string, object string) (objectToShardNodes ObjectToShardNodes, size int64, err error) {

	// TODO: Validate to use sha256 of bucket/object as key
	key := []byte(fmt.Sprintf("%s/%s", bucket, object))

	// calculates partition id for the given key
	// partID := hash(key) % partitionCount
	// the partitions are already distributed among members by Add function.

	// TODO: Validate ring vs badger data

	owner := backend.HashRing.LocateKey(key)
	fmt.Println(owner.String())

	// Next, get the data and parity shards from the owner node
	hashRingShards, err := backend.HashRing.GetClosestN(key, backend.RsDataShard+backend.RsParityShard)

	if err != nil {
		return objectToShardNodes, 0, err
	}

	// Query Badger for which files, and offsets,
	// and create ObjectShardReader for each shard

	objectHash := genObjectHash(bucket, object)

	data, err := backend.DB.Get(objectHash[:])

	// Check if exists
	if err != nil {
		return objectToShardNodes, 0, err
	}

	// Decode from Badger DB
	r := bytes.NewReader(data)
	dec := gob.NewDecoder(r)

	if err := dec.Decode(&objectToShardNodes); err != nil {
		return objectToShardNodes, 0, err
	}

	// TODO: Confirm
	if len(hashRingShards) != (len(objectToShardNodes.DataShardNodes) + len(objectToShardNodes.ParityShardNodes)) {
		return objectToShardNodes, 0, errors.New("number of shards does not match number of hash ring shards")
	}

	return objectToShardNodes, size, nil

	/*
		// TODO: Confirm edge case when badger result does not match expected number of shards
		objectShardReader = make([]ObjectShardReader, backend.RsDataShard+backend.RsParityShard)

		// Create shards and load the data.
		//shards := make([]io.Reader, backend.RsDataShard+backend.RsParityShard)

		for i := range objectToShardNodes {
			//infn := fmt.Sprintf("%s.%d", fname, i)

			// Loop through each WAL if object spans multiple files

			for i2 := range objectToShardNodes[i].WALFiles {
				walFile := wal.FormatWalFile(objectToShardNodes[i].WALFiles[i2].WALNum)
				infn := filepath.Join(backend.nodeDir(hashRingShards[i].String()), walFile)
				fmt.Println("Opening", infn, "shard", i, "wal file", i2)
				f, err := os.Open(infn)
				if err != nil {
					// Potential, data or shard missing, can rebuilt in later step
					fmt.Println("Error reading file", err)
					objectShardReader[i] = ObjectShardReader{}
					continue
				}

				objectShardReader[i] = ObjectShardReader{
					File:        f,
					WALFileInfo: objectToShardNodes[i].WALFiles[i2],
				}

			}

		}
	*/
}

// Convert node name to uint32 for internal shard (data / parity tracking )
func NodeToUint32(value string) (v uint32, err error) {

	s := strings.Replace(value, "node-", "", 1)
	vint, err := strconv.Atoi(s)

	if err != nil {
		return 0, err
	}

	return uint32(vint), nil
}

func genObjectHash(bucket string, object string) [32]byte {
	objectKey := fmt.Sprintf("%s/%s", bucket, object)
	return sha256.Sum256([]byte(objectKey))
}
