package distributed

import (
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/gofiber/fiber/v2"
	"github.com/klauspost/reedsolomon"
	s3db "github.com/mulgadc/predastore/s3db"
)

func (backend Backend) Get(bucket string, object string, out io.Writer, ctx *fiber.Ctx) (err error) {

	// Create matrix
	enc, err := reedsolomon.NewStream(backend.RsDataShard, backend.RsParityShard)
	if err != nil {
		return
	}

	// First, query which nodes have our object shards
	shards, _, err := backend.openInput(bucket, object)
	if err != nil {
		return
	}

	// Set parity to false, only query data nodes, fallback to parity if this fails and reconstruct
	// TODO: Add check for parity corruption, right now we are just interested in the data parts
	shardReaders, err := backend.shardReaders(bucket, object, shards, false)

	if err != nil {
		return err
	}

	// Attempt to parse shards into a single object, can we reconstruct the parts?
	err = enc.Join(out, shardReaders, shards.Size)

	if err != nil {

		slog.Error("Error decoding, reconstruction required", "err", err)

		// Open shards from both data and parity nodes
		shardReaders, err = backend.shardReaders(bucket, object, shards, true)

		if err != nil {
			return err
		}

		// Create out destination writers
		reconstruction := make([]io.Writer, len(shardReaders))
		files := make([]*os.File, len(shardReaders))
		for i := range reconstruction {
			if shardReaders[i] == nil {
				// Generate the object hash
				objHash := s3db.GenObjectHash(bucket, object)
				filename := fmt.Sprintf("%s.%d", hex.EncodeToString(objHash[:]), i)
				outfn := filepath.Join(os.TempDir(), filename)

				// Store the missing part on the local FS
				files[i], err = os.Create(outfn)

				slog.Info("Creating temporary file for reconstruction", "filename", outfn)

				if err != nil {
					return
				}

				// Remove for cleanup
				defer os.Remove(outfn)

				reconstruction[i] = files[i]

			}
		}

		err = enc.Reconstruct(shardReaders, reconstruction)

		if err != nil {
			return err
		}

		// Next, close the reconstruction writers
		for i := range files {
			if shardReaders[i] == nil {
				// Close and remove the file once complete
				defer func(int) {
					files[i].Close()
				}(i)
			}
		}

		// First, rewind readers
		shardReaders, err = backend.shardReaders(bucket, object, shards, true)
		if err != nil {
			return err
		}

		// Next, parse our reconstructed part into the shardReaders, fill in the missing part
		for i := range shardReaders {

			if shardReaders[i] == nil {
				files[i].Seek(0, 0)        // Rewind to the start
				shardReaders[i] = files[i] // New reader
			}

		}

		// TODO: Mark chunk to heal, the node may be offline, or local disk corruption.

		// Last, merge and provide the object
		// Attempt to parse shards into a single object
		err = enc.Join(out, shardReaders, shards.Size)

		// Still could not be correctly joined after rebuild, critical failure
		if err != nil {
			return err
		}

	}

	return err
}

func (backend Backend) GetObjectHead(bucket string, object string, c *fiber.Ctx) (err error) {

	return

}
