package distributed

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/gofiber/fiber/v2"
	"github.com/klauspost/reedsolomon"
	"github.com/mulgadc/predastore/s3"
)

type Backend struct {
	Config        s3.Config
	RsDataShard   int
	RsParityShard int
	HashRing      *consistent.Consistent
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
		os.MkdirAll(fmt.Sprintf("s3/tests/data/distributed/nodes/node-%d", i), 0750)
	}

	for i := range cfg.PartitionCount {
		svc.HashRing.Add(myMember(fmt.Sprintf("node-%d", i)))
	}

	return svc, nil
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

			infn := fmt.Sprintf(fmt.Sprintf("s3/tests/data/distributed/nodes/%s/%s.%d", hashRingShards[i].String(), object, i))
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
				out[i], err = os.Create(filepath.Join(fmt.Sprintf("s3/tests/data/distributed/nodes/%s", hashRingShards[i]), outfn))
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

func (backend Backend) PutObject(bucket string, object string, c *fiber.Ctx) (err error) {

	// Simple encoder
	/*
		// Create encoding matrix.
		enc, err := reedsolomon.New(backend.RsDataShard, backend.RsParityShard)
		checkErr(err)

		fmt.Println("Opening", object)
		b, err := os.ReadFile(object)
		checkErr(err)

		// Split the file into equally sized shards.
		shards, err := enc.Split(b)
		checkErr(err)
		fmt.Printf("File split into %d data+parity shards with %d bytes/shard.\n", len(shards), len(shards[0]))

		// Encode parity
		err = enc.Encode(shards)
		checkErr(err)

		// Write out the resulting files.
		_, file := filepath.Split(object)

		key := []byte(fmt.Sprintf("%s/%s", bucket, file))

		// calculates partition id for the given key
		// partID := hash(key) % partitionCount
		// the partitions are already distributed among members by Add function.
		owner := backend.HashRing.LocateKey(key)
		fmt.Println(owner.String())

		// Next, get the data and parity shards from the owner node
		hashRingShards, err := backend.HashRing.GetClosestN(key, backend.RsDataShard+backend.RsParityShard)

		fmt.Println("Writing shards to nodes:")
		fmt.Println("hashRingShards:", hashRingShards)

		for i, shard := range shards {
			outfn := fmt.Sprintf("%s.%d", file, i)

			fmt.Println("Writing to", outfn)
			err = os.WriteFile(filepath.Join(fmt.Sprintf("s3/tests/data/distributed/nodes/%s", hashRingShards[i]), outfn), shard, 0644)
			checkErr(err)
		}
	*/

	// Stream encoder
	// Create encoding matrix.
	enc, err := reedsolomon.NewStream(backend.RsDataShard, backend.RsParityShard)
	checkErr(err)

	fmt.Println("Opening", object)
	f, err := os.Open(object)
	checkErr(err)

	instat, err := f.Stat()
	checkErr(err)

	shards := backend.RsDataShard + backend.RsParityShard
	out := make([]*os.File, shards)

	// Create the resulting files.

	_, file := filepath.Split(object)
	// Stream

	key := []byte(fmt.Sprintf("%s/%s", bucket, file))

	// calculates partition id for the given key
	// partID := hash(key) % partitionCount
	// the partitions are already distributed among members by Add function.
	owner := backend.HashRing.LocateKey(key)
	fmt.Println(owner.String())

	// Next, get the data and parity shards from the owner node
	hashRingShards, err := backend.HashRing.GetClosestN(key, backend.RsDataShard+backend.RsParityShard)

	for i := range out {
		outfn := fmt.Sprintf("%s.%d", file, i)

		fmt.Println("Writing to", outfn)
		out[i], err = os.Create(filepath.Join(fmt.Sprintf("s3/tests/data/distributed/nodes/%s", hashRingShards[i]), outfn))
		checkErr(err)
	}

	// Split into files.
	data := make([]io.Writer, backend.RsDataShard)
	for i := range data {
		data[i] = out[i]
	}
	// Do the split
	err = enc.Split(f, data, instat.Size())
	checkErr(err)

	// Close and re-open the files.
	input := make([]io.Reader, backend.RsDataShard)

	for i := range data {
		out[i].Close()
		f, err := os.Open(out[i].Name())
		checkErr(err)
		input[i] = f
		defer f.Close()
	}

	// Create parity output writers
	parity := make([]io.Writer, backend.RsParityShard)
	for i := range parity {
		parity[i] = out[backend.RsDataShard+i]
		defer out[backend.RsDataShard+i].Close()
	}

	// Encode parity
	err = enc.Encode(input, parity)
	checkErr(err)
	fmt.Printf("File split into %d data + %d parity shards.\n", backend.RsDataShard, backend.RsParityShard)

	return

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
			out[i], err = os.Create(filepath.Join(fmt.Sprintf("s3/tests/data/distributed/nodes/%s", hashRingShards[i]), outfn))
			checkErr(err)
		}

	*/

	// Create shards and load the data.
	shards := make([]io.Reader, backend.RsDataShard+backend.RsParityShard)
	for i := range shards {
		//infn := fmt.Sprintf("%s.%d", fname, i)

		infn := fmt.Sprintf("s3/tests/data/distributed/nodes/%s/%s.%d", hashRingShards[i], object, i)

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
