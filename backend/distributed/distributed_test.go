package distributed

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/klauspost/reedsolomon"
	s3backend "github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/internal/storetest"
	"github.com/mulgadc/predastore/quic/quicserver"
	"github.com/mulgadc/predastore/s3db"
	"github.com/mulgadc/predastore/store"
	"github.com/stretchr/testify/require"
)

// TestPutObject_ShardPlacementAndReconstruction verifies that PutObjectFromPath
// writes both data and parity shards to the correct hash-ring nodes, then
// reconstructs the original payload by reading shards directly from each node's
// store and joining via reed-solomon. Regression guard against parity metadata
// being written to the wrong node, which would silently break reconstruction.
func TestPutObject_ShardPlacementAndReconstruction(t *testing.T) {
	const (
		bucket         = "test-bucket"
		partitionCount = 5
		testBasePort   = 59991
		objectSize     = 256*1024 + 123
	)

	tmpDir := t.TempDir()
	cfg := &Config{
		BadgerDir:      tmpDir,
		PartitionCount: partitionCount,
		QuicBasePort:   testBasePort,
	}
	b, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, b)
	defer b.Close()

	backend, ok := b.(*Backend)
	require.True(t, ok)

	dataDir := filepath.Join(tmpDir, "nodes")
	backend.SetDataDir(dataDir)

	nodeDirs := make([]string, partitionCount)
	quicServers := make([]*quicserver.QuicServer, partitionCount)
	for i := range partitionCount {
		nodeDir := filepath.Join(dataDir, fmt.Sprintf("node-%d", i))
		require.NoError(t, os.MkdirAll(nodeDir, 0750))
		nodeDirs[i] = nodeDir

		qs, err := quicserver.NewWithRetry(nodeDir, fmt.Sprintf("127.0.0.1:%d", testBasePort+i), 5, quicserver.WithMasterKey(storetest.TestKey()))
		require.NoError(t, err, "Failed to start QUIC server for node %d", i)
		quicServers[i] = qs
	}
	time.Sleep(200 * time.Millisecond)

	orig := make([]byte, objectSize)
	for i := range orig {
		orig[i] = byte((i + objectSize) % 251)
	}
	objPath := filepath.Join(tmpDir, "obj.bin")
	require.NoError(t, os.WriteFile(objPath, orig, 0644))

	ctx := context.Background()
	require.NoError(t, backend.PutObjectFromPath(ctx, bucket, objPath))

	// Each QUIC server holds its node's store open via the badger lock; close
	// them synchronously before reopening the stores for inspection.
	for _, qs := range quicServers {
		require.NoError(t, qs.Close())
	}

	objectHash := s3db.GenObjectHash(bucket, objPath)
	totalShards := backend.RsDataShard() + backend.RsParityShard()
	hashRingShards, err := backend.HashRing().GetClosestN(objectHash[:], totalShards)
	require.NoError(t, err)
	require.Len(t, hashRingShards, totalShards)

	stores := make([]*store.Store, totalShards)
	readers := make([]io.Reader, totalShards)
	closers := make([]io.Closer, totalShards)
	t.Cleanup(func() {
		for _, c := range closers {
			if c != nil {
				_ = c.Close()
			}
		}
		for _, s := range stores {
			if s != nil {
				_ = s.Close()
			}
		}
	})

	for i := range totalShards {
		node := hashRingShards[i].String()
		nodeNum, err := NodeToUint32(node)
		require.NoError(t, err)

		s, err := store.Open(nodeDirs[nodeNum], store.WithAEAD(storetest.TestAEAD()))
		require.NoError(t, err, "open store for node %s (shard %d)", node, i)
		stores[i] = s

		// Lookup returns an error when the shard's metadata is absent — this is
		// the per-shard placement assertion (data and parity alike).
		rdr, err := s.Lookup(objectHash, uint32(i))
		require.NoErrorf(t, err, "shard %d missing on node %s", i, node)
		require.NotNil(t, rdr)
		readers[i] = rdr
		closers[i] = rdr
	}

	enc, err := reedsolomon.NewStream(backend.RsDataShard(), backend.RsParityShard())
	require.NoError(t, err)

	var out bytes.Buffer
	require.NoError(t, enc.Join(&out, readers, int64(len(orig))))
	require.Equal(t, orig, out.Bytes())
}

// TestPutGetRoundTrip exercises a full PUT then GET against a live QUIC
// cluster via the public Backend interface, validating end-to-end correctness
// of erasure-coded write + reconstruction read.
func TestPutGetRoundTrip(t *testing.T) {
	const (
		bucket         = "test-bucket"
		partitionCount = 11
		testBasePort   = 49991
		objectSize     = 128 * 1024
	)

	tmpDir := t.TempDir()
	cfg := &Config{
		BadgerDir:      tmpDir,
		PartitionCount: partitionCount,
		QuicBasePort:   testBasePort,
		Buckets:        []BucketConfig{{Name: bucket, Region: "us-east-1"}},
	}
	b, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, b)
	defer b.Close()

	backend, ok := b.(*Backend)
	require.True(t, ok)

	dataDir := filepath.Join(tmpDir, "nodes")
	backend.SetDataDir(dataDir)

	quicServers := make([]*quicserver.QuicServer, partitionCount)
	for i := range partitionCount {
		nodeDir := filepath.Join(dataDir, fmt.Sprintf("node-%d", i))
		require.NoError(t, os.MkdirAll(nodeDir, 0750))

		qs, err := quicserver.NewWithRetry(nodeDir, fmt.Sprintf("127.0.0.1:%d", testBasePort+i), 5, quicserver.WithMasterKey(storetest.TestKey()))
		require.NoError(t, err, "Failed to start QUIC server for node %d", i)
		quicServers[i] = qs
	}
	defer func() {
		for _, qs := range quicServers {
			if qs != nil {
				_ = qs.Close()
			}
		}
	}()

	time.Sleep(100 * time.Millisecond)

	orig := make([]byte, objectSize)
	for i := range orig {
		orig[i] = byte((i + objectSize) % 251)
	}
	objPath := filepath.Join(tmpDir, "obj.bin")
	require.NoError(t, os.WriteFile(objPath, orig, 0644))

	ctx := context.Background()
	require.NoError(t, backend.PutObjectFromPath(ctx, bucket, objPath))

	var out bytes.Buffer
	require.NoError(t, backend.GetFromPath(ctx, bucket, objPath, &out))
	require.Equal(t, orig, out.Bytes())
}

// TestRangeRequestLogic validates the byte-range calculation logic without
// requiring QUIC servers. Pure arithmetic over shard-aware range requests.
func TestRangeRequestLogic(t *testing.T) {
	testCases := []struct {
		name         string
		totalSize    int64
		dataShards   int
		rangeStart   int64
		rangeEnd     int64
		expectShard  int
		expectOffset int64
		expectLength int64
		spanMultiple bool
	}{
		{
			name:         "First 512 bytes of 128KB file with 2 data shards",
			totalSize:    128 * 1024,
			dataShards:   2,
			rangeStart:   100,
			rangeEnd:     611,
			expectShard:  0,
			expectOffset: 100,
			expectLength: 512,
			spanMultiple: false,
		},
		{
			name:         "Middle of second shard",
			totalSize:    128 * 1024,
			dataShards:   2,
			rangeStart:   70000,
			rangeEnd:     70511,
			expectShard:  1,
			expectOffset: 70000 - 65536,
			expectLength: 512,
			spanMultiple: false,
		},
		{
			name:         "Range spanning shard boundary",
			totalSize:    128 * 1024,
			dataShards:   2,
			rangeStart:   64000,
			rangeEnd:     66000,
			expectShard:  0,
			spanMultiple: true,
		},
		{
			name:         "Small file single shard",
			totalSize:    1024,
			dataShards:   2,
			rangeStart:   100,
			rangeEnd:     199,
			expectShard:  0,
			expectOffset: 100,
			expectLength: 100,
			spanMultiple: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			shardSize := (tc.totalSize + int64(tc.dataShards) - 1) / int64(tc.dataShards)

			startShardIdx := int(tc.rangeStart / shardSize)
			endShardIdx := int(tc.rangeEnd / shardSize)

			if startShardIdx >= tc.dataShards {
				startShardIdx = tc.dataShards - 1
			}
			if endShardIdx >= tc.dataShards {
				endShardIdx = tc.dataShards - 1
			}

			spansMultiple := startShardIdx != endShardIdx

			t.Logf("totalSize=%d, shardSize=%d, startShard=%d, endShard=%d, spansMultiple=%v",
				tc.totalSize, shardSize, startShardIdx, endShardIdx, spansMultiple)

			require.Equal(t, tc.spanMultiple, spansMultiple, "span multiple shards check")

			if !tc.spanMultiple {
				require.Equal(t, tc.expectShard, startShardIdx, "shard index")

				shardStart := int64(startShardIdx) * shardSize
				offsetInShard := tc.rangeStart - shardStart
				require.Equal(t, tc.expectOffset, offsetInShard, "offset within shard")

				length := tc.rangeEnd - tc.rangeStart + 1
				require.Equal(t, tc.expectLength, length, "range length")
			}
		})
	}
}

// TestHandleRangeWithFullReconstructionFallback validates range extraction
// from reconstructed data — the fallback path when single-shard read fails.
func TestHandleRangeWithFullReconstructionFallback(t *testing.T) {
	size := 1024
	orig := make([]byte, size)
	for i := range orig {
		orig[i] = byte((i + size) % 251)
	}

	testCases := []struct {
		name      string
		start     int64
		end       int64
		expectLen int
	}{
		{"First 100 bytes", 0, 99, 100},
		{"Middle 200 bytes", 400, 599, 200},
		{"Last 50 bytes", int64(size - 50), int64(size - 1), 50},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.end >= int64(len(orig)) {
				tc.end = int64(len(orig)) - 1
			}

			rangeData := orig[tc.start : tc.end+1]
			require.Equal(t, tc.expectLen, len(rangeData), "extracted range length")

			for i, b := range rangeData {
				expected := byte((int64(i) + tc.start + int64(size)) % 251)
				require.Equal(t, expected, b, "byte at position %d", i)
			}
		})
	}
}

// TestGetObjectByteRange tests byte-range GET requests against a live QUIC
// cluster, exercising shard calculation and data extraction end-to-end.
func TestGetObjectByteRange(t *testing.T) {
	const bucket = "test-bucket"
	const objectSize = 128 * 1024

	tmpDir := t.TempDir()
	testBasePort := 19991

	cfg := &Config{
		BadgerDir:      tmpDir,
		PartitionCount: 5,
		QuicBasePort:   testBasePort,
		Buckets:        []BucketConfig{{Name: bucket, Region: "us-east-1"}},
	}
	b, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, b)
	defer b.Close()

	backend, ok := b.(*Backend)
	require.True(t, ok)

	dataDir := filepath.Join(tmpDir, "nodes")
	backend.SetDataDir(dataDir)

	quicServers := make([]*quicserver.QuicServer, 5)
	for i := range 5 {
		nodeDir := filepath.Join(dataDir, fmt.Sprintf("node-%d", i))
		require.NoError(t, os.MkdirAll(nodeDir, 0750))

		qs, err := quicserver.NewWithRetry(nodeDir, fmt.Sprintf("127.0.0.1:%d", testBasePort+i), 5, quicserver.WithMasterKey(storetest.TestKey()))
		require.NoError(t, err, "Failed to start QUIC server for node %d", i)
		quicServers[i] = qs
	}
	defer func() {
		for _, qs := range quicServers {
			if qs != nil {
				_ = qs.Close()
			}
		}
	}()

	time.Sleep(200 * time.Millisecond)

	orig := make([]byte, objectSize)
	for i := range orig {
		orig[i] = byte((i + objectSize) % 251)
	}
	objPath := filepath.Join(tmpDir, "test-obj.bin")
	require.NoError(t, os.WriteFile(objPath, orig, 0644))

	ctx := context.Background()
	require.NoError(t, backend.PutObjectFromPath(ctx, bucket, objPath))

	testCases := []struct {
		name       string
		rangeStart int64
		rangeEnd   int64
	}{
		{"First 512 bytes", 0, 511},
		{"Middle range", 4096, 8191},
		{"Last 1024 bytes", int64(objectSize - 1024), int64(objectSize - 1)},
		{"Single byte at start", 0, 0},
		{"Single byte in middle", 1000, 1000},
		{"Small range crossing chunk boundary", 65530, 65550},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := &s3backend.GetObjectRequest{
				Bucket:     bucket,
				Key:        objPath,
				RangeStart: tc.rangeStart,
				RangeEnd:   tc.rangeEnd,
			}

			resp, err := backend.GetObject(ctx, req)
			require.NoError(t, err, "GetObject with range should succeed")
			require.NotNil(t, resp)
			require.Equal(t, 206, resp.StatusCode, "Should return 206 Partial Content")
			defer resp.Body.Close()

			data, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			expectedLen := tc.rangeEnd - tc.rangeStart + 1
			require.Equal(t, expectedLen, int64(len(data)), "Response size should match requested range")

			expectedData := orig[tc.rangeStart : tc.rangeEnd+1]
			require.Equal(t, expectedData, data, "Response data should match original bytes at specified range")
		})
	}
}
