package distributed

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/gofiber/fiber/v2"
	"github.com/klauspost/reedsolomon"
	"github.com/mulgadc/predastore/s3"
	"github.com/mulgadc/predastore/s3/wal"
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
}

type Node struct {
	Id string
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

func (backend Backend) Get(bucket string, object string, c *fiber.Ctx) (err error) {

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

	// Open the inputs
	shards, size, err := backend.openInput(bucket, object)
	checkErr(err)

	// Verify the shards
	ok, err := enc.Verify(shards)
	if ok {
		fmt.Println("No reconstruction needed")
	} else {
		fmt.Println("Verification failed. Reconstructing data")
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

	// Join the shards and write them
	outfn := fmt.Sprintf("%s/%s", os.TempDir(), object)

	fmt.Println("Writing data to", outfn)
	//f, err := os.Create(outfn)
	//checkErr(err)

	shards, size, err = backend.openInput(bucket, object)
	checkErr(err)

	// We don't know the exact filesize.
	std := StdoutWriter{}

	err = enc.Join(std, shards, int64(backend.RsDataShard)*size)
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

// putObjectToWAL splits an on-disk file into RS shards and writes each shard to the WAL
// for the node selected by the hash ring. It returns the WAL WriteResults for data and parity shards.
func (backend Backend) putObjectToWAL(bucket string, objectPath string) (dataResults []*wal.WriteResult, parityResults []*wal.WriteResult, err error) {
	// Stream encoder
	enc, err := reedsolomon.NewStream(backend.RsDataShard, backend.RsParityShard)
	if err != nil {
		return nil, nil, err
	}

	f, err := os.Open(objectPath)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	instat, err := f.Stat()
	if err != nil {
		return nil, nil, err
	}

	_, file := filepath.Split(objectPath)
	key := []byte(fmt.Sprintf("%s/%s", bucket, file))

	hashRingShards, err := backend.HashRing.GetClosestN(key, backend.RsDataShard+backend.RsParityShard)
	if err != nil {
		return nil, nil, err
	}

	totalShards := backend.RsDataShard + backend.RsParityShard
	walFiles := make([]*wal.WAL, totalShards)
	for i := range walFiles {
		walDir := backend.nodeDir(hashRingShards[i].String())
		if mkErr := os.MkdirAll(walDir, 0750); mkErr != nil {
			return nil, nil, mkErr
		}
		walFiles[i], err = wal.New(filepath.Join(walDir, "state.json"), walDir)
		if err != nil {
			return nil, nil, err
		}
	}
	defer func() {
		for i := range walFiles {
			if walFiles[i] != nil {
				_ = walFiles[i].Close()
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
		return nil, nil, firstErr
	}

	// 2) Encode parity using the data shards we just wrote (read back) -> parity pipes -> parity WAL writes.
	dataReaders := make([]io.Reader, backend.RsDataShard)
	for i := 0; i < backend.RsDataShard; i++ {
		b, rerr := walFiles[i].ReadFromWriteResult(dataResults[i])
		if rerr != nil {
			return nil, nil, rerr
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
		return nil, nil, firstErr
	}

	return dataResults, parityResults, nil
}

func (backend Backend) PutObject(bucket string, object string, c *fiber.Ctx) (err error) {

	dataRes, parityRes, err := backend.putObjectToWAL(bucket, object)
	if err != nil {
		return err
	}

	_, file := filepath.Split(object)
	key := []byte(fmt.Sprintf("%s/%s", bucket, file))
	hashRingShards, _ := backend.HashRing.GetClosestN(key, backend.RsDataShard+backend.RsParityShard)

	// Print the WAL location results for now (Badger KV later).
	for i := 0; i < backend.RsDataShard; i++ {
		fmt.Printf("put_object wal_write data_shard=%d node=%s write_result=%#v\n",
			i, hashRingShards[i].String(), dataRes[i])
	}
	for i := 0; i < backend.RsParityShard; i++ {
		fmt.Printf("put_object wal_write parity_shard=%d node=%s write_result=%#v\n",
			i, hashRingShards[backend.RsDataShard+i].String(), parityRes[i])
	}

	return nil

}

func checkErr(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s", err.Error())
		os.Exit(2)
	}
}

func (backend Backend) openInput(bucket string, object string) (r []io.Reader, size int64, err error) {

	key := []byte(fmt.Sprintf("%s/%s", bucket, object))

	// calculates partition id for the given key
	// partID := hash(key) % partitionCount
	// the partitions are already distributed among members by Add function.
	owner := backend.HashRing.LocateKey(key)
	fmt.Println(owner.String())

	// Next, get the data and parity shards from the owner node
	hashRingShards, err := backend.HashRing.GetClosestN(key, backend.RsDataShard+backend.RsParityShard)

	/*
		for i := range out {
			outfn := fmt.Sprintf("%s.%d", file, i)

			fmt.Println("Writing to", outfn)
			out[i], err = os.Create(filepath.Join(backend.nodeDir(hashRingShards[i].String()), outfn))
			checkErr(err)
		}

	*/

	// Create shards and load the data.
	shards := make([]io.Reader, backend.RsDataShard+backend.RsParityShard)
	for i := range shards {
		//infn := fmt.Sprintf("%s.%d", fname, i)

		infn := filepath.Join(backend.nodeDir(hashRingShards[i].String()), fmt.Sprintf("%s.%d", object, i))

		fmt.Println("Opening", infn)
		f, err := os.Open(infn)
		if err != nil {
			fmt.Println("Error reading file", err)
			shards[i] = nil
			continue
		} else {
			shards[i] = f
		}
		stat, err := f.Stat()
		checkErr(err)
		if stat.Size() > 0 {
			size = stat.Size()
		} else {
			shards[i] = nil
		}
	}
	return shards, size, nil
}
