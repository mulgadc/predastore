package distributed

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/klauspost/reedsolomon"
	"github.com/mulgadc/predastore/s3/wal"
	"github.com/stretchr/testify/require"
)

func TestPutObjectToWAL_RoundTripVerifyJoin(t *testing.T) {

	tmpDir, err := os.MkdirTemp(os.TempDir(), fmt.Sprintf("unit-test-%d", time.Now().UnixNano()))
	require.NoError(t, err)

	backend, err := New(Backend{BadgerDir: tmpDir})
	require.NoError(t, err)
	require.NotNil(t, backend)

	// Ensure tests don't write into the repo's s3/tests/... directories.
	tmp := t.TempDir()
	backend.DataDir = filepath.Join(tmp, "nodes")

	// `New()` uses PartitionCount=11 and names nodes as node-0..node-10.
	for i := 0; i < 11; i++ {
		require.NoError(t, os.MkdirAll(filepath.Join(backend.DataDir, fmt.Sprintf("node-%d", i)), 0750))
	}

	// Deterministic data (avoid external fixtures).
	size := 256*1024 + 123 // spans multiple 8KB WAL chunks with remainder
	orig := make([]byte, size)
	for i := range orig {
		orig[i] = byte((i + size) % 251)
	}

	objPath := filepath.Join(tmp, "obj.bin")
	require.NoError(t, os.WriteFile(objPath, orig, 0644))

	objectKey := fmt.Sprintf("bucket/%s", objPath)
	objectHash := sha256.Sum256([]byte(objectKey))

	dataRes, parityRes, fsize, err := backend.putObjectToWAL("bucket", objPath, objectHash)
	require.NoError(t, err)
	require.Equal(t, fsize, int64(len(orig)))
	require.Len(t, dataRes, backend.RsDataShard)
	require.Len(t, parityRes, backend.RsParityShard)

	// Recompute shard->node mapping (must match putObjectToWAL).
	_, file := filepath.Split(objPath)
	key := []byte(fmt.Sprintf("%s/%s", "bucket", file))
	hashRingShards, err := backend.HashRing.GetClosestN(key, backend.RsDataShard+backend.RsParityShard)
	require.NoError(t, err)
	require.Len(t, hashRingShards, backend.RsDataShard+backend.RsParityShard)

	// Regression guard: metadata must be written to EACH node's local WAL Badger DB
	// for both data and parity shards. Missing parity metadata breaks reconstruction later.
	for i := 0; i < backend.RsDataShard+backend.RsParityShard; i++ {
		node := hashRingShards[i].String()
		nodeDir := backend.nodeDir(node)
		w, err := wal.New(filepath.Join(nodeDir, "state.json"), nodeDir)
		require.NoError(t, err)
		require.NotNil(t, w)

		val, err := w.DB.Get(objectHash[:])
		require.NoErrorf(t, err, "expected object hash metadata in WAL badger: node=%s shard_index=%d", node, i)
		require.NotEmpty(t, val, "expected non-empty metadata value: node=%s shard_index=%d", node, i)

		require.NoError(t, w.Close())
	}

	// Read shards back from WAL using the returned WriteResults.
	totalShards := backend.RsDataShard + backend.RsParityShard
	shardBytes := make([][]byte, totalShards)

	for i := 0; i < totalShards; i++ {
		nodeDir := backend.nodeDir(hashRingShards[i].String())
		w, err := wal.New(filepath.Join(nodeDir, "state.json"), nodeDir)
		require.NoError(t, err)
		require.NotNil(t, w)

		var res *wal.WriteResult
		if i < backend.RsDataShard {
			res = dataRes[i]
		} else {
			res = parityRes[i-backend.RsDataShard]
		}
		require.NotNil(t, res)

		b, err := w.ReadFromWriteResult(res)
		require.NoError(t, err)
		require.NotEmpty(t, b)
		shardBytes[i] = b

		require.NoError(t, w.Close())
	}

	enc, err := reedsolomon.NewStream(backend.RsDataShard, backend.RsParityShard)
	require.NoError(t, err)

	// Verify consumes the readers, so build readers from the stored bytes.
	verifyReaders := make([]io.Reader, totalShards)
	for i := range verifyReaders {
		verifyReaders[i] = bytes.NewReader(shardBytes[i])
	}

	ok, err := enc.Verify(verifyReaders)
	require.NoError(t, err)
	require.True(t, ok, "expected shards to verify")

	// Join consumes the readers too; build fresh readers.
	joinReaders := make([]io.Reader, totalShards)
	for i := range joinReaders {
		joinReaders[i] = bytes.NewReader(shardBytes[i])
	}

	var out bytes.Buffer
	require.NoError(t, enc.Join(&out, joinReaders, int64(len(orig))))
	require.Equal(t, orig, out.Bytes())
}

func TestReadFromWriteResultStream_RoundTripJoin(t *testing.T) {
	tmpDir, err := os.MkdirTemp(os.TempDir(), fmt.Sprintf("unit-test-%d", time.Now().UnixNano()))
	require.NoError(t, err)

	backend, err := New(Backend{BadgerDir: tmpDir})
	require.NoError(t, err)
	require.NotNil(t, backend)

	// Ensure tests don't write into the repo's s3/tests/... directories.
	tmp := t.TempDir()
	backend.DataDir = filepath.Join(tmp, "nodes")

	// `New()` uses PartitionCount=11 and names nodes as node-0..node-10.
	for i := 0; i < 11; i++ {
		require.NoError(t, os.MkdirAll(filepath.Join(backend.DataDir, fmt.Sprintf("node-%d", i)), 0750))
	}

	// Deterministic data (avoid external fixtures).
	size := 256*1024 + 123
	orig := make([]byte, size)
	for i := range orig {
		orig[i] = byte((i + size) % 251)
	}

	objPath := filepath.Join(tmp, "obj.bin")
	require.NoError(t, os.WriteFile(objPath, orig, 0644))

	objectKey := fmt.Sprintf("bucket/%s", objPath)
	objectHash := sha256.Sum256([]byte(objectKey))

	dataRes, parityRes, fsize, err := backend.putObjectToWAL("bucket", objPath, objectHash)
	require.NoError(t, err)
	require.Equal(t, fsize, int64(len(orig)))
	require.Len(t, dataRes, backend.RsDataShard)
	require.Len(t, parityRes, backend.RsParityShard)

	// Recompute shard->node mapping (must match putObjectToWAL).
	_, file := filepath.Split(objPath)
	key := []byte(fmt.Sprintf("%s/%s", "bucket", file))
	hashRingShards, err := backend.HashRing.GetClosestN(key, backend.RsDataShard+backend.RsParityShard)
	require.NoError(t, err)
	require.Len(t, hashRingShards, backend.RsDataShard+backend.RsParityShard)

	// Build streaming shard readers directly from the WAL using ReadFromWriteResultStream.
	totalShards := backend.RsDataShard + backend.RsParityShard
	shardReaders := make([]io.Reader, totalShards)

	// Keep WAL instances alive until join completes (stream goroutines capture *WAL).
	wals := make([]*wal.WAL, totalShards)
	t.Cleanup(func() {
		for _, w := range wals {
			if w != nil {
				_ = w.Close()
			}
		}
	})

	for i := 0; i < totalShards; i++ {
		nodeDir := backend.nodeDir(hashRingShards[i].String())
		w, err := wal.New(filepath.Join(nodeDir, "state.json"), nodeDir)
		require.NoError(t, err)
		require.NotNil(t, w)
		wals[i] = w

		var res *wal.WriteResult
		if i < backend.RsDataShard {
			res = dataRes[i]
		} else {
			res = parityRes[i-backend.RsDataShard]
		}
		require.NotNil(t, res)

		r, err := w.ReadFromWriteResultStream(res)
		require.NoError(t, err)
		require.NotNil(t, r)
		shardReaders[i] = r
	}

	enc, err := reedsolomon.NewStream(backend.RsDataShard, backend.RsParityShard)
	require.NoError(t, err)

	var out bytes.Buffer
	require.NoError(t, enc.Join(&out, shardReaders, int64(len(orig))))
	require.Equal(t, orig, out.Bytes())
}

func TestPutGet_ReconstructionValidation_CorruptionAndMissingWAL(t *testing.T) {
	bucket := "test-bucket"

	cases := []struct {
		name string
		size int
	}{
		{name: "32kb", size: 32 * 1024},
		{name: "128kb", size: 128 * 1024},
		{name: "1mb", size: 1 * 1024 * 1024},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir, err := os.MkdirTemp(os.TempDir(), fmt.Sprintf("unit-test-%d", time.Now().UnixNano()))
			require.NoError(t, err)

			backend, err := New(Backend{BadgerDir: tmpDir})
			require.NoError(t, err)
			require.NotNil(t, backend)

			// Ensure tests don't write into the repo's s3/tests/... directories.
			tmp := t.TempDir()
			backend.DataDir = filepath.Join(tmp, "nodes")

			// `New()` uses PartitionCount=11 and names nodes as node-0..node-10.
			for i := 0; i < 11; i++ {
				require.NoError(t, os.MkdirAll(filepath.Join(backend.DataDir, fmt.Sprintf("node-%d", i)), 0750))
			}

			// Create deterministic content.
			orig := make([]byte, tc.size)
			for i := range orig {
				orig[i] = byte((i + tc.size) % 251)
			}
			objPath := filepath.Join(tmp, "obj.bin")
			require.NoError(t, os.WriteFile(objPath, orig, 0644))

			// PUT
			require.NoError(t, backend.PutObject(bucket, objPath, nil))

			// GET should match.
			var out bytes.Buffer
			require.NoError(t, backend.Get(bucket, objPath, &out, nil))
			require.Equal(t, 0, bytes.Compare(orig, out.Bytes()))

			// Locate the shard placement and per-node WAL write result metadata.
			objectHash := genObjectHash(bucket, objPath)
			shards, _, err := backend.openInput(bucket, objPath)
			require.NoError(t, err)
			require.NotEmpty(t, shards.DataShardNodes)

			// Pick the first data shard node and corrupt its WAL.
			targetNode := fmt.Sprintf("node-%d", shards.DataShardNodes[0])
			targetNodeDir := backend.nodeDir(targetNode)

			w, err := wal.New(filepath.Join(targetNodeDir, "state.json"), targetNodeDir)
			require.NoError(t, err)
			require.NotNil(t, w)

			meta, err := w.DB.Get(objectHash[:])
			require.NoError(t, err)
			require.NotEmpty(t, meta)

			var owr wal.ObjectWriteResult
			require.NoError(t, gob.NewDecoder(bytes.NewReader(meta)).Decode(&owr))
			require.NotEmpty(t, owr.WriteResult.WALFiles)

			walHeaderSize := int64(w.WALHeaderSize())
			chunkSize := w.Shard.ChunkSize
			walFileInfo := owr.WriteResult.WALFiles[0]
			walPath := filepath.Join(targetNodeDir, wal.FormatWalFile(walFileInfo.WALNum))

			// Close WAL instance before modifying on-disk files.
			require.NoError(t, w.Close())

			seekBase := walHeaderSize + walFileInfo.Offset

			// Corruption 1a: corrupt fragment header (Length field), GET should fail.
			{
				f, err := os.OpenFile(walPath, os.O_RDWR, 0)
				require.NoError(t, err)
				defer f.Close()

				_, err = f.Seek(seekBase, io.SeekStart)
				require.NoError(t, err)

				origHeader := make([]byte, wal.FragmentHeaderBytes)
				_, err = io.ReadFull(f, origHeader)
				require.NoError(t, err)

				corruptHeader := make([]byte, len(origHeader))
				copy(corruptHeader, origHeader)
				// Set Length > ChunkSize so streaming read fails fast.
				binary.BigEndian.PutUint32(corruptHeader[20:24], chunkSize+1)

				_, err = f.Seek(seekBase, io.SeekStart)
				require.NoError(t, err)
				_, err = f.Write(corruptHeader)
				require.NoError(t, err)

				var out2 bytes.Buffer
				require.Error(t, backend.Get(bucket, objPath, &out2, nil))

				// Restore header.
				_, err = f.Seek(seekBase, io.SeekStart)
				require.NoError(t, err)
				_, err = f.Write(origHeader)
				require.NoError(t, err)
			}

			// Corruption 1b: flip a bit in fragment payload, GET should fail.
			{
				f, err := os.OpenFile(walPath, os.O_RDWR, 0)
				require.NoError(t, err)
				defer f.Close()

				payloadPos := seekBase + int64(wal.FragmentHeaderBytes)
				_, err = f.Seek(payloadPos, io.SeekStart)
				require.NoError(t, err)

				var b [1]byte
				_, err = io.ReadFull(f, b[:])
				require.NoError(t, err)
				origByte := b[0]
				b[0] = origByte ^ 0x01

				_, err = f.Seek(payloadPos, io.SeekStart)
				require.NoError(t, err)
				_, err = f.Write(b[:])
				require.NoError(t, err)

				var out2 bytes.Buffer
				require.Error(t, backend.Get(bucket, objPath, &out2, nil))

				// Restore byte.
				b[0] = origByte
				_, err = f.Seek(payloadPos, io.SeekStart)
				require.NoError(t, err)
				_, err = f.Write(b[:])
				require.NoError(t, err)

				// Re-test, should not fail
				require.NoError(t, backend.Get(bucket, objPath, &out2, nil))

			}

			// Corruption 2: WAL file missing, GET should fail; after restore, GET should succeed.
			{
				moved := walPath + ".moved"
				require.NoError(t, os.Rename(walPath, moved))

				var out2 bytes.Buffer
				require.Error(t, backend.Get(bucket, objPath, &out2, nil))

				require.NoError(t, os.Rename(moved, walPath))

				var out3 bytes.Buffer
				require.NoError(t, backend.Get(bucket, objPath, &out3, nil))
				require.Equal(t, 0, bytes.Compare(orig, out3.Bytes()))
			}
		})
	}
}
