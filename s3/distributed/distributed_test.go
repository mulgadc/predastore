package distributed

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/reedsolomon"
	"github.com/mulgadc/predastore/s3/wal"
	"github.com/stretchr/testify/require"
)

func TestPutObjectToWAL_RoundTripVerifyJoin(t *testing.T) {
	backend, err := New(nil)
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

	dataRes, parityRes, err := backend.putObjectToWAL("bucket", objPath)
	require.NoError(t, err)
	require.Len(t, dataRes, backend.RsDataShard)
	require.Len(t, parityRes, backend.RsParityShard)

	// Recompute shard->node mapping (must match putObjectToWAL).
	_, file := filepath.Split(objPath)
	key := []byte(fmt.Sprintf("%s/%s", "bucket", file))
	hashRingShards, err := backend.HashRing.GetClosestN(key, backend.RsDataShard+backend.RsParityShard)
	require.NoError(t, err)
	require.Len(t, hashRingShards, backend.RsDataShard+backend.RsParityShard)

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
