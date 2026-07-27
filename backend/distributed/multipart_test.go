package distributed

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/backend/multipart"
	"github.com/mulgadc/predastore/internal/storetest"
	"github.com/mulgadc/predastore/internal/testcerts"
	"github.com/mulgadc/predastore/internal/testport"
	"github.com/mulgadc/predastore/quic/quicclient"
	"github.com/mulgadc/predastore/quic/quicserver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupMultipartTestBackend creates a distributed backend with QUIC servers for testing.
// The returned slice exposes the per-node QUIC servers (indexed by node number) so tests
// can probe shard presence and inject node failures.
func setupMultipartTestBackend(t *testing.T) (*Backend, []*quicserver.QuicServer, func()) {
	t.Helper()

	tmpDir := t.TempDir()

	testBasePort := testport.Block(t, 5)

	cfg := &Config{
		BadgerDir:      tmpDir,
		PartitionCount: 5,
		QuicBasePort:   testBasePort,
		Buckets: []BucketConfig{
			{Name: "test-bucket", Region: "us-east-1"},
		},
	}
	b, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, b)

	be, ok := b.(*Backend)
	require.True(t, ok)
	dataDir := filepath.Join(tmpDir, "nodes")
	be.SetDataDir(dataDir)

	certPath, keyPath, pool := testcerts.Generate(t)
	quicclient.SetDefaultRootCAs(pool)
	t.Cleanup(func() { quicclient.SetDefaultRootCAs(nil) })

	// Start QUIC servers
	quicServers := make([]*quicserver.QuicServer, 5)
	for i := range 5 {
		nodeDir := filepath.Join(dataDir, fmt.Sprintf("node-%d", i))
		require.NoError(t, os.MkdirAll(nodeDir, 0750))

		qs, err := quicserver.NewWithRetry(nodeDir, fmt.Sprintf("127.0.0.1:%d", testBasePort+i), 5,
			quicserver.WithMasterKey(storetest.TestKey()),
			quicserver.WithTLSCertFiles(certPath, keyPath),
		)
		require.NoError(t, err, "Failed to start QUIC server for node %d", i)
		quicServers[i] = qs
	}

	// Give QUIC servers time to start
	time.Sleep(200 * time.Millisecond)

	cleanup := func() {
		// Drop pooled client connections first: they are keyed by node address in a
		// package global, so leaving them open outlives the servers they point at
		// and leaks a socket (and its goroutines) for the rest of the test binary.
		quicclient.DefaultPool.Close()
		for _, qs := range quicServers {
			if qs != nil {
				_ = qs.Close()
			}
		}
		be.Close()
	}

	return be, quicServers, cleanup
}

// shardExistsOnNode probes a single QUIC node for the presence of one shard of an object,
// returning true if the node still serves it. Used to assert part shards are deleted on
// cleanup while the assembled object's shards survive.
func shardExistsOnNode(t *testing.T, be *Backend, node uint32, bucket, key string, shardIndex int) bool {
	t.Helper()
	ctx := context.Background()
	client, err := quicclient.DialPooled(ctx, be.getNodeAddr(int(node)))
	require.NoError(t, err)

	rc, err := client.Get(ctx, quicserver.ObjectRequest{
		Bucket:     bucket,
		Object:     key,
		RangeStart: -1,
		RangeEnd:   -1,
		ShardIndex: uint32(shardIndex),
	})
	if err != nil {
		return false
	}
	_ = rc.Close()
	return true
}

// loadPartShardNodes decodes the stored ObjectToShardNodes for a part from globalState.
func loadPartShardNodes(t *testing.T, be *Backend, uploadID string, partNumber int) ObjectToShardNodes {
	t.Helper()
	key := fmt.Sprintf("part:%s:%05d", uploadID, partNumber)
	data, err := be.globalState.Get(TableObjects, []byte(key))
	require.NoError(t, err)

	var nodes ObjectToShardNodes
	require.NoError(t, gob.NewDecoder(bytes.NewReader(data)).Decode(&nodes))
	return nodes
}

// assertAllPartShardsDeleted asserts every data+parity shard of the given part is gone
// from its node.
func assertAllPartShardsDeleted(t *testing.T, be *Backend, bucket, key, uploadID string, partNumber int, nodes ObjectToShardNodes) {
	t.Helper()
	partObjKey := partObjectKey(bucket, key, uploadID, partNumber)

	for i, node := range nodes.DataShardNodes {
		assert.False(t, shardExistsOnNode(t, be, node, bucket, partObjKey, i),
			"data shard %d for part %d should be deleted on node %d", i, partNumber, node)
	}
	for i, node := range nodes.ParityShardNodes {
		shardIndex := len(nodes.DataShardNodes) + i
		assert.False(t, shardExistsOnNode(t, be, node, bucket, partObjKey, shardIndex),
			"parity shard %d for part %d should be deleted on node %d", shardIndex, partNumber, node)
	}
}

// createUploadWithParts creates a multipart upload and uploads partData in order,
// returning the upload ID and the completed-part descriptors.
func createUploadWithParts(t *testing.T, be *Backend, bucket, key string, partData [][]byte) (string, []backend.CompletedPart) {
	t.Helper()
	ctx := context.Background()

	createResp, err := be.CreateMultipartUpload(ctx, &backend.CreateMultipartUploadRequest{Bucket: bucket, Key: key})
	require.NoError(t, err)

	parts := make([]backend.CompletedPart, len(partData))
	for i, data := range partData {
		resp, err := be.UploadPart(ctx, &backend.UploadPartRequest{
			Bucket:     bucket,
			Key:        key,
			UploadID:   createResp.UploadID,
			PartNumber: i + 1,
			Body:       bytes.NewReader(data),
		})
		require.NoError(t, err)
		parts[i] = backend.CompletedPart{PartNumber: i + 1, ETag: resp.ETag}
	}
	return createResp.UploadID, parts
}

// captureShardMaps records each part's ObjectToShardNodes before cleanup removes it.
func captureShardMaps(t *testing.T, be *Backend, uploadID string, parts []backend.CompletedPart) map[int]ObjectToShardNodes {
	t.Helper()
	maps := make(map[int]ObjectToShardNodes, len(parts))
	for _, p := range parts {
		maps[p.PartNumber] = loadPartShardNodes(t, be, uploadID, p.PartNumber)
	}
	return maps
}

func TestDistributed_CreateMultipartUpload(t *testing.T) {
	be, _, cleanup := setupMultipartTestBackend(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("successful creation", func(t *testing.T) {
		req := &backend.CreateMultipartUploadRequest{
			Bucket:      "test-bucket",
			Key:         "test-object.bin",
			ContentType: "application/octet-stream",
		}

		resp, err := be.CreateMultipartUpload(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		assert.Equal(t, "test-bucket", resp.Bucket)
		assert.Equal(t, "test-object.bin", resp.Key)
		assert.NotEmpty(t, resp.UploadID)

		// Verify upload metadata was stored
		metadata, err := be.getUploadMetadata(resp.UploadID)
		require.NoError(t, err)
		assert.Equal(t, req.Bucket, metadata.Bucket)
		assert.Equal(t, req.Key, metadata.Key)
		assert.Equal(t, req.ContentType, metadata.ContentType)
	})

	t.Run("empty bucket", func(t *testing.T) {
		req := &backend.CreateMultipartUploadRequest{
			Bucket: "",
			Key:    "test-object.bin",
		}

		resp, err := be.CreateMultipartUpload(ctx, req)
		assert.Error(t, err)
		assert.Nil(t, resp)
	})

	t.Run("empty key", func(t *testing.T) {
		req := &backend.CreateMultipartUploadRequest{
			Bucket: "test-bucket",
			Key:    "",
		}

		resp, err := be.CreateMultipartUpload(ctx, req)
		assert.Error(t, err)
		assert.Nil(t, resp)
	})
}

func TestDistributed_UploadPart(t *testing.T) {
	be, _, cleanup := setupMultipartTestBackend(t)
	defer cleanup()

	ctx := context.Background()

	// Create a multipart upload first
	createResp, err := be.CreateMultipartUpload(ctx, &backend.CreateMultipartUploadRequest{
		Bucket: "test-bucket",
		Key:    "test-object.bin",
	})
	require.NoError(t, err)
	uploadID := createResp.UploadID

	t.Run("successful part upload", func(t *testing.T) {
		partData := make([]byte, multipart.MinPartSize) // 5MB
		for i := range partData {
			partData[i] = byte(i % 256)
		}

		expectedMD5 := md5.Sum(partData)
		expectedETag := fmt.Sprintf("\"%x\"", expectedMD5)

		req := &backend.UploadPartRequest{
			Bucket:     "test-bucket",
			Key:        "test-object.bin",
			UploadID:   uploadID,
			PartNumber: 1,
			Body:       bytes.NewReader(partData),
		}

		resp, err := be.UploadPart(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		assert.Equal(t, 1, resp.PartNumber)
		assert.Equal(t, expectedETag, resp.ETag)
	})

	t.Run("small part upload", func(t *testing.T) {
		// Small parts are allowed (validated only during completion)
		partData := []byte("small part data")

		req := &backend.UploadPartRequest{
			Bucket:     "test-bucket",
			Key:        "test-object.bin",
			UploadID:   uploadID,
			PartNumber: 2,
			Body:       bytes.NewReader(partData),
		}

		resp, err := be.UploadPart(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, 2, resp.PartNumber)
	})

	t.Run("invalid upload ID", func(t *testing.T) {
		req := &backend.UploadPartRequest{
			Bucket:     "test-bucket",
			Key:        "test-object.bin",
			UploadID:   "non-existent-upload-id",
			PartNumber: 1,
			Body:       bytes.NewReader([]byte("data")),
		}

		resp, err := be.UploadPart(ctx, req)
		assert.Error(t, err)
		assert.Nil(t, resp)
	})

	t.Run("invalid part number zero", func(t *testing.T) {
		req := &backend.UploadPartRequest{
			Bucket:     "test-bucket",
			Key:        "test-object.bin",
			UploadID:   uploadID,
			PartNumber: 0,
			Body:       bytes.NewReader([]byte("data")),
		}

		resp, err := be.UploadPart(ctx, req)
		assert.Error(t, err)
		assert.Nil(t, resp)
	})

	t.Run("invalid part number too large", func(t *testing.T) {
		req := &backend.UploadPartRequest{
			Bucket:     "test-bucket",
			Key:        "test-object.bin",
			UploadID:   uploadID,
			PartNumber: 10001,
			Body:       bytes.NewReader([]byte("data")),
		}

		resp, err := be.UploadPart(ctx, req)
		assert.Error(t, err)
		assert.Nil(t, resp)
	})

	t.Run("mismatched bucket", func(t *testing.T) {
		req := &backend.UploadPartRequest{
			Bucket:     "wrong-bucket",
			Key:        "test-object.bin",
			UploadID:   uploadID,
			PartNumber: 3,
			Body:       bytes.NewReader([]byte("data")),
		}

		resp, err := be.UploadPart(ctx, req)
		assert.Error(t, err)
		assert.Nil(t, resp)
	})
}

func TestDistributed_CompleteMultipartUpload(t *testing.T) {
	be, _, cleanup := setupMultipartTestBackend(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("successful completion with two parts", func(t *testing.T) {
		// Create upload
		createResp, err := be.CreateMultipartUpload(ctx, &backend.CreateMultipartUploadRequest{
			Bucket: "test-bucket",
			Key:    "complete-test.bin",
		})
		require.NoError(t, err)
		uploadID := createResp.UploadID

		// Upload parts
		part1Data := make([]byte, multipart.MinPartSize)
		for i := range part1Data {
			part1Data[i] = byte(i % 256)
		}
		part2Data := []byte("last part data - can be small")

		part1Resp, err := be.UploadPart(ctx, &backend.UploadPartRequest{
			Bucket:     "test-bucket",
			Key:        "complete-test.bin",
			UploadID:   uploadID,
			PartNumber: 1,
			Body:       bytes.NewReader(part1Data),
		})
		require.NoError(t, err)

		part2Resp, err := be.UploadPart(ctx, &backend.UploadPartRequest{
			Bucket:     "test-bucket",
			Key:        "complete-test.bin",
			UploadID:   uploadID,
			PartNumber: 2,
			Body:       bytes.NewReader(part2Data),
		})
		require.NoError(t, err)

		// Complete upload
		completeResp, err := be.CompleteMultipartUpload(ctx, &backend.CompleteMultipartUploadRequest{
			Bucket:   "test-bucket",
			Key:      "complete-test.bin",
			UploadID: uploadID,
			Parts: []backend.CompletedPart{
				{PartNumber: 1, ETag: part1Resp.ETag},
				{PartNumber: 2, ETag: part2Resp.ETag},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, completeResp)

		assert.Equal(t, "test-bucket", completeResp.Bucket)
		assert.Equal(t, "complete-test.bin", completeResp.Key)
		assert.Contains(t, completeResp.ETag, "-2") // Multipart ETag format
		assert.NotEmpty(t, completeResp.Location)

		// Verify the final object can be read
		getResp, err := be.GetObject(ctx, &backend.GetObjectRequest{
			Bucket:     "test-bucket",
			Key:        "complete-test.bin",
			RangeStart: -1,
			RangeEnd:   -1,
		})
		require.NoError(t, err)
		require.NotNil(t, getResp)
		defer getResp.Body.Close()

		readData, err := io.ReadAll(getResp.Body)
		require.NoError(t, err)

		expectedSize := int64(len(part1Data) + len(part2Data))
		assert.Equal(t, expectedSize, int64(len(readData)))

		// Verify content
		assert.Equal(t, part1Data, readData[:len(part1Data)])
		assert.Equal(t, part2Data, readData[len(part1Data):])
	})

	t.Run("invalid upload ID", func(t *testing.T) {
		resp, err := be.CompleteMultipartUpload(ctx, &backend.CompleteMultipartUploadRequest{
			Bucket:   "test-bucket",
			Key:      "test.bin",
			UploadID: "non-existent-upload-id",
			Parts: []backend.CompletedPart{
				{PartNumber: 1, ETag: "\"etag\""},
			},
		})
		assert.Error(t, err)
		assert.Nil(t, resp)
	})

	t.Run("parts not in order", func(t *testing.T) {
		createResp, err := be.CreateMultipartUpload(ctx, &backend.CreateMultipartUploadRequest{
			Bucket: "test-bucket",
			Key:    "order-test.bin",
		})
		require.NoError(t, err)
		uploadID := createResp.UploadID

		// Upload parts
		part1Data := make([]byte, multipart.MinPartSize)
		part2Data := make([]byte, multipart.MinPartSize)

		part1Resp, err := be.UploadPart(ctx, &backend.UploadPartRequest{
			Bucket:     "test-bucket",
			Key:        "order-test.bin",
			UploadID:   uploadID,
			PartNumber: 1,
			Body:       bytes.NewReader(part1Data),
		})
		require.NoError(t, err)

		part2Resp, err := be.UploadPart(ctx, &backend.UploadPartRequest{
			Bucket:     "test-bucket",
			Key:        "order-test.bin",
			UploadID:   uploadID,
			PartNumber: 2,
			Body:       bytes.NewReader(part2Data),
		})
		require.NoError(t, err)

		// Try to complete with parts in wrong order
		resp, err := be.CompleteMultipartUpload(ctx, &backend.CompleteMultipartUploadRequest{
			Bucket:   "test-bucket",
			Key:      "order-test.bin",
			UploadID: uploadID,
			Parts: []backend.CompletedPart{
				{PartNumber: 2, ETag: part2Resp.ETag},
				{PartNumber: 1, ETag: part1Resp.ETag},
			},
		})
		assert.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "ascending order")
	})

	t.Run("non-last part too small", func(t *testing.T) {
		createResp, err := be.CreateMultipartUpload(ctx, &backend.CreateMultipartUploadRequest{
			Bucket: "test-bucket",
			Key:    "small-part-test.bin",
		})
		require.NoError(t, err)
		uploadID := createResp.UploadID

		// Upload small first part (should fail during completion)
		part1Data := []byte("too small for first part")
		part2Data := []byte("last part")

		part1Resp, err := be.UploadPart(ctx, &backend.UploadPartRequest{
			Bucket:     "test-bucket",
			Key:        "small-part-test.bin",
			UploadID:   uploadID,
			PartNumber: 1,
			Body:       bytes.NewReader(part1Data),
		})
		require.NoError(t, err)

		part2Resp, err := be.UploadPart(ctx, &backend.UploadPartRequest{
			Bucket:     "test-bucket",
			Key:        "small-part-test.bin",
			UploadID:   uploadID,
			PartNumber: 2,
			Body:       bytes.NewReader(part2Data),
		})
		require.NoError(t, err)

		// Try to complete - should fail due to small first part
		resp, err := be.CompleteMultipartUpload(ctx, &backend.CompleteMultipartUploadRequest{
			Bucket:   "test-bucket",
			Key:      "small-part-test.bin",
			UploadID: uploadID,
			Parts: []backend.CompletedPart{
				{PartNumber: 1, ETag: part1Resp.ETag},
				{PartNumber: 2, ETag: part2Resp.ETag},
			},
		})
		assert.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "too small")
	})
}

func TestDistributed_AbortMultipartUpload(t *testing.T) {
	be, _, cleanup := setupMultipartTestBackend(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("successful abort", func(t *testing.T) {
		// Create upload
		createResp, err := be.CreateMultipartUpload(ctx, &backend.CreateMultipartUploadRequest{
			Bucket: "test-bucket",
			Key:    "abort-test.bin",
		})
		require.NoError(t, err)
		uploadID := createResp.UploadID

		// Upload a part
		partData := make([]byte, multipart.MinPartSize)
		_, err = be.UploadPart(ctx, &backend.UploadPartRequest{
			Bucket:     "test-bucket",
			Key:        "abort-test.bin",
			UploadID:   uploadID,
			PartNumber: 1,
			Body:       bytes.NewReader(partData),
		})
		require.NoError(t, err)

		// Abort
		err = be.AbortMultipartUpload(ctx, "test-bucket", "abort-test.bin", uploadID)
		require.NoError(t, err)

		// Verify upload metadata is gone
		_, err = be.getUploadMetadata(uploadID)
		assert.Error(t, err)
	})

	t.Run("abort non-existent upload", func(t *testing.T) {
		err := be.AbortMultipartUpload(ctx, "test-bucket", "test.bin", "non-existent-upload-id")
		assert.Error(t, err)
	})

	t.Run("abort with wrong bucket", func(t *testing.T) {
		createResp, err := be.CreateMultipartUpload(ctx, &backend.CreateMultipartUploadRequest{
			Bucket: "test-bucket",
			Key:    "abort-bucket-test.bin",
		})
		require.NoError(t, err)

		err = be.AbortMultipartUpload(ctx, "wrong-bucket", "abort-bucket-test.bin", createResp.UploadID)
		assert.Error(t, err)
	})
}

func TestDistributed_MultipartUpload_FullWorkflow(t *testing.T) {
	be, _, cleanup := setupMultipartTestBackend(t)
	defer cleanup()

	ctx := context.Background()

	const bucket = "test-bucket"
	const key = "full-workflow-test.bin"

	// Create deterministic test data
	part1Size := multipart.MinPartSize     // 5MB
	part2Size := multipart.MinPartSize * 2 // 10MB
	part3Size := int64(1024 * 1024)        // 1MB (last part can be small)

	part1Data := make([]byte, part1Size)
	part2Data := make([]byte, part2Size)
	part3Data := make([]byte, part3Size)

	for i := range part1Data {
		part1Data[i] = byte((i + 0) % 251)
	}
	for i := range part2Data {
		part2Data[i] = byte((i + 100) % 251)
	}
	for i := range part3Data {
		part3Data[i] = byte((i + 200) % 251)
	}

	// Step 1: Create multipart upload
	createResp, err := be.CreateMultipartUpload(ctx, &backend.CreateMultipartUploadRequest{
		Bucket:      bucket,
		Key:         key,
		ContentType: "application/octet-stream",
	})
	require.NoError(t, err)
	uploadID := createResp.UploadID
	t.Logf("Created multipart upload: %s", uploadID)

	// Step 2: Upload parts (can be done in any order)
	part2Resp, err := be.UploadPart(ctx, &backend.UploadPartRequest{
		Bucket:     bucket,
		Key:        key,
		UploadID:   uploadID,
		PartNumber: 2,
		Body:       bytes.NewReader(part2Data),
	})
	require.NoError(t, err)
	t.Logf("Uploaded part 2: %s", part2Resp.ETag)

	part1Resp, err := be.UploadPart(ctx, &backend.UploadPartRequest{
		Bucket:     bucket,
		Key:        key,
		UploadID:   uploadID,
		PartNumber: 1,
		Body:       bytes.NewReader(part1Data),
	})
	require.NoError(t, err)
	t.Logf("Uploaded part 1: %s", part1Resp.ETag)

	part3Resp, err := be.UploadPart(ctx, &backend.UploadPartRequest{
		Bucket:     bucket,
		Key:        key,
		UploadID:   uploadID,
		PartNumber: 3,
		Body:       bytes.NewReader(part3Data),
	})
	require.NoError(t, err)
	t.Logf("Uploaded part 3: %s", part3Resp.ETag)

	// Step 3: Complete upload (parts must be in order)
	completeResp, err := be.CompleteMultipartUpload(ctx, &backend.CompleteMultipartUploadRequest{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
		Parts: []backend.CompletedPart{
			{PartNumber: 1, ETag: part1Resp.ETag},
			{PartNumber: 2, ETag: part2Resp.ETag},
			{PartNumber: 3, ETag: part3Resp.ETag},
		},
	})
	require.NoError(t, err)
	t.Logf("Completed upload: ETag=%s, Location=%s", completeResp.ETag, completeResp.Location)

	// Verify ETag format
	assert.Contains(t, completeResp.ETag, "-3", "Multipart ETag should contain part count")

	// Step 4: Verify the object
	getResp, err := be.GetObject(ctx, &backend.GetObjectRequest{
		Bucket:     bucket,
		Key:        key,
		RangeStart: -1,
		RangeEnd:   -1,
	})
	require.NoError(t, err)
	defer getResp.Body.Close()

	readData, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)

	expectedTotalSize := part1Size + part2Size + part3Size
	assert.Equal(t, expectedTotalSize, int64(len(readData)), "Total size mismatch")

	// Verify content of each part
	offset := int64(0)
	assert.Equal(t, part1Data, readData[offset:offset+part1Size], "Part 1 content mismatch")
	offset += part1Size
	assert.Equal(t, part2Data, readData[offset:offset+part2Size], "Part 2 content mismatch")
	offset += part2Size
	assert.Equal(t, part3Data, readData[offset:offset+part3Size], "Part 3 content mismatch")

	t.Logf("Full workflow test passed: %d bytes verified", len(readData))
}

func TestDistributed_MultipartUpload_LargeNumberOfParts(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large parts test in short mode")
	}

	be, _, cleanup := setupMultipartTestBackend(t)
	defer cleanup()

	ctx := context.Background()

	const bucket = "test-bucket"
	const key = "many-parts-test.bin"
	const numParts = 10 // Use 10 parts for quick test

	// Create upload
	createResp, err := be.CreateMultipartUpload(ctx, &backend.CreateMultipartUploadRequest{
		Bucket: bucket,
		Key:    key,
	})
	require.NoError(t, err)
	uploadID := createResp.UploadID

	// Upload parts
	parts := make([]backend.CompletedPart, numParts)
	partSizes := make([]int64, numParts)

	for i := range numParts {
		var partData []byte
		if i < numParts-1 {
			// Non-last parts must be at least 5MB
			partData = make([]byte, multipart.MinPartSize)
		} else {
			// Last part can be smaller
			partData = make([]byte, 1024)
		}

		// Fill with recognizable pattern
		for j := range partData {
			partData[j] = byte((i + j) % 256)
		}

		resp, err := be.UploadPart(ctx, &backend.UploadPartRequest{
			Bucket:     bucket,
			Key:        key,
			UploadID:   uploadID,
			PartNumber: i + 1,
			Body:       bytes.NewReader(partData),
		})
		require.NoError(t, err, "Failed to upload part %d", i+1)

		parts[i] = backend.CompletedPart{
			PartNumber: i + 1,
			ETag:       resp.ETag,
		}
		partSizes[i] = int64(len(partData))
	}

	// Complete upload
	completeResp, err := be.CompleteMultipartUpload(ctx, &backend.CompleteMultipartUploadRequest{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
		Parts:    parts,
	})
	require.NoError(t, err)

	// Verify ETag has correct part count
	assert.Contains(t, completeResp.ETag, fmt.Sprintf("-%d", numParts))

	// Verify total size
	getResp, err := be.GetObject(ctx, &backend.GetObjectRequest{
		Bucket:     bucket,
		Key:        key,
		RangeStart: -1,
		RangeEnd:   -1,
	})
	require.NoError(t, err)
	defer getResp.Body.Close()

	readData, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)

	var expectedTotal int64
	for _, size := range partSizes {
		expectedTotal += size
	}
	assert.Equal(t, expectedTotal, int64(len(readData)))

	t.Logf("Large parts test passed: %d parts, %d total bytes", numParts, len(readData))
}

func TestDistributed_MultipartUpload_PartOverwrite(t *testing.T) {
	be, _, cleanup := setupMultipartTestBackend(t)
	defer cleanup()

	ctx := context.Background()

	const bucket = "test-bucket"
	const key = "overwrite-test.bin"

	// Create upload
	createResp, err := be.CreateMultipartUpload(ctx, &backend.CreateMultipartUploadRequest{
		Bucket: bucket,
		Key:    key,
	})
	require.NoError(t, err)
	uploadID := createResp.UploadID

	// Upload part 1 first time
	part1DataV1 := make([]byte, multipart.MinPartSize)
	for i := range part1DataV1 {
		part1DataV1[i] = 0xAA // First version
	}

	_, err = be.UploadPart(ctx, &backend.UploadPartRequest{
		Bucket:     bucket,
		Key:        key,
		UploadID:   uploadID,
		PartNumber: 1,
		Body:       bytes.NewReader(part1DataV1),
	})
	require.NoError(t, err)

	// Upload part 1 again with different data (overwrite)
	part1DataV2 := make([]byte, multipart.MinPartSize)
	for i := range part1DataV2 {
		part1DataV2[i] = 0xBB // Second version
	}

	part1RespV2, err := be.UploadPart(ctx, &backend.UploadPartRequest{
		Bucket:     bucket,
		Key:        key,
		UploadID:   uploadID,
		PartNumber: 1,
		Body:       bytes.NewReader(part1DataV2),
	})
	require.NoError(t, err)

	// Upload part 2
	part2Data := []byte("part 2 data")
	part2Resp, err := be.UploadPart(ctx, &backend.UploadPartRequest{
		Bucket:     bucket,
		Key:        key,
		UploadID:   uploadID,
		PartNumber: 2,
		Body:       bytes.NewReader(part2Data),
	})
	require.NoError(t, err)

	// Complete with second version of part 1
	completeResp, err := be.CompleteMultipartUpload(ctx, &backend.CompleteMultipartUploadRequest{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
		Parts: []backend.CompletedPart{
			{PartNumber: 1, ETag: part1RespV2.ETag}, // Use V2 ETag
			{PartNumber: 2, ETag: part2Resp.ETag},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, completeResp)

	// Verify the second version is used
	getResp, err := be.GetObject(ctx, &backend.GetObjectRequest{
		Bucket:     bucket,
		Key:        key,
		RangeStart: -1,
		RangeEnd:   -1,
	})
	require.NoError(t, err)
	defer getResp.Body.Close()

	readData, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)

	// Check that the data contains 0xBB (V2), not 0xAA (V1)
	assert.Equal(t, byte(0xBB), readData[0], "Should contain V2 data, not V1")
	assert.Equal(t, byte(0xBB), readData[multipart.MinPartSize-1], "Should contain V2 data, not V1")
}

// TestDistributed_MultipartUpload_ConcurrentParts_Contention is the end-to-end
// reproducer for the AWS-CLI-style access pattern documented in
// docs/development/bugs/multipart-upload-deadlock.md: one CreateMultipartUpload
// followed by N UploadPart calls fired concurrently from N goroutines. This
// is the scenario that wedges against a real spinifex-predastore deployment
// when `aws s3 cp` (default 10 concurrent threads) is used on any file large
// enough to split into parts that individually exceed the QUIC connection
// window.
//
// The wedge's necessary conditions — shared pooled QUIC connection per node,
// many streams per connection, per-part bodies on the order of the connection
// window (15 MiB default), and server-side handlers that hold wal.mu across
// the stream read — are all present in this harness because setupMultipart-
// TestBackend wires up a 5-node QUIC topology with the production code path.
//
// Acceptance:
//   - On `main` today: at least some UploadPart calls hang past their per-
//     request context deadline and the overall test trips its timeout.
//   - Post-fix (7df9645 + 9dcd3dd + 43e20f6): all 10 concurrent parts
//     complete and the data reconstructs correctly end-to-end. This is
//     now the end-to-end regression gate for those fixes.
//
// Scope note: the 90 s total-elapsed bound is a deadlock detector, not a
// performance regression. A per-part benchmark belongs in s3_bench_test.go.
func TestDistributed_MultipartUpload_ConcurrentParts_Contention(t *testing.T) {
	be, _, cleanup := setupMultipartTestBackend(t)
	defer cleanup()

	ctx := context.Background()

	const bucket = "test-bucket"
	const key = "concurrent-parts.bin"
	const numParts = 10
	// Part size above quic-go's default MaxConnectionReceiveWindow (15 MiB).
	// This is the minimal condition for the head-of-line wedge to form when
	// multiple streams share one pooled connection.
	const partSize = 16 * 1024 * 1024

	createResp, err := be.CreateMultipartUpload(ctx, &backend.CreateMultipartUploadRequest{
		Bucket: bucket,
		Key:    key,
	})
	require.NoError(t, err)
	uploadID := createResp.UploadID

	type partResult struct {
		partNumber int
		etag       string
		elapsed    time.Duration
		err        error
	}
	results := make(chan partResult, numParts)

	overallStart := time.Now()
	var wg sync.WaitGroup
	for p := 1; p <= numParts; p++ {
		wg.Add(1)
		go func(pn int) {
			defer wg.Done()

			// Deterministic per-part payload so the eventual reassembled
			// object has a known byte pattern.
			data := make([]byte, partSize)
			for i := range data {
				data[i] = byte((pn*31 + i) % 256)
			}

			partCtx, partCancel := context.WithTimeout(ctx, 2*time.Minute)
			defer partCancel()

			start := time.Now()
			resp, perr := be.UploadPart(partCtx, &backend.UploadPartRequest{
				Bucket:     bucket,
				Key:        key,
				UploadID:   uploadID,
				PartNumber: pn,
				Body:       bytes.NewReader(data),
			})
			elapsed := time.Since(start)
			if perr != nil {
				results <- partResult{partNumber: pn, elapsed: elapsed, err: perr}
				return
			}
			results <- partResult{partNumber: pn, etag: resp.ETag, elapsed: elapsed}
		}(p)
	}

	wg.Wait()
	close(results)
	overallElapsed := time.Since(overallStart)

	completed := make([]backend.CompletedPart, 0, numParts)
	for r := range results {
		require.NoError(t, r.err, "part %d failed after %v", r.partNumber, r.elapsed)
		t.Logf("part %d completed in %v", r.partNumber, r.elapsed)
		completed = append(completed, backend.CompletedPart{
			PartNumber: r.partNumber,
			ETag:       r.etag,
		})
	}

	// Deadlock detector. 10 × 16 MiB serialized through five nodes on
	// loopback should finish well under the bound; the wedge blows through
	// this via the per-part 2-minute deadline.
	require.Less(t, overallElapsed, 90*time.Second,
		"suspected multipart wedge (Bug C): overall elapsed %v exceeded budget", overallElapsed)

	// Sort completed parts by part number before CompleteMultipartUpload —
	// the results channel delivers out of order.
	sortPartsByNumber(completed)

	completeResp, err := be.CompleteMultipartUpload(ctx, &backend.CompleteMultipartUploadRequest{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
		Parts:    completed,
	})
	require.NoError(t, err)
	require.NotNil(t, completeResp)

	// End-to-end sanity: the reassembled object reads back at the expected
	// total size. Per-byte verification would double runtime for little
	// extra signal; the multipart lifecycle tests cover that already.
	getResp, err := be.GetObject(ctx, &backend.GetObjectRequest{
		Bucket:     bucket,
		Key:        key,
		RangeStart: -1,
		RangeEnd:   -1,
	})
	require.NoError(t, err)
	defer getResp.Body.Close()

	total, err := io.Copy(io.Discard, getResp.Body)
	require.NoError(t, err)
	require.Equal(t, int64(numParts)*int64(partSize), total,
		"reassembled object size mismatch")

	t.Logf("10 concurrent parts × 16 MiB reassembled in %v", overallElapsed)
}

// sortPartsByNumber orders CompletedPart entries by PartNumber in place.
// CompleteMultipartUpload requires parts to be in order.
func sortPartsByNumber(parts []backend.CompletedPart) {
	for i := 1; i < len(parts); i++ {
		for j := i; j > 0 && parts[j-1].PartNumber > parts[j].PartNumber; j-- {
			parts[j-1], parts[j] = parts[j], parts[j-1]
		}
	}
}

// Verification case 1: CompleteMultipartUpload issues a QUIC shard-delete for every
// part shard (data + parity).
func TestDistributed_CompleteMultipartUpload_DeletesPartShards(t *testing.T) {
	be, _, cleanup := setupMultipartTestBackend(t)
	defer cleanup()
	ctx := context.Background()
	const bucket, key = "test-bucket", "complete-deletes-shards.bin"

	part1 := bytes.Repeat([]byte{0x11}, int(multipart.MinPartSize))
	part2 := []byte("final small part")
	uploadID, parts := createUploadWithParts(t, be, bucket, key, [][]byte{part1, part2})
	shardMaps := captureShardMaps(t, be, uploadID, parts)

	_, err := be.CompleteMultipartUpload(ctx, &backend.CompleteMultipartUploadRequest{
		Bucket: bucket, Key: key, UploadID: uploadID, Parts: parts,
	})
	require.NoError(t, err)

	for _, p := range parts {
		assertAllPartShardsDeleted(t, be, bucket, key, uploadID, p.PartNumber, shardMaps[p.PartNumber])
	}
}

// Verification case 2: AbortMultipartUpload issues a QUIC shard-delete for every part shard.
func TestDistributed_AbortMultipartUpload_DeletesPartShards(t *testing.T) {
	be, _, cleanup := setupMultipartTestBackend(t)
	defer cleanup()
	ctx := context.Background()
	const bucket, key = "test-bucket", "abort-deletes-shards.bin"

	part1 := bytes.Repeat([]byte{0x22}, int(multipart.MinPartSize))
	part2 := []byte("another small part")
	uploadID, parts := createUploadWithParts(t, be, bucket, key, [][]byte{part1, part2})
	shardMaps := captureShardMaps(t, be, uploadID, parts)

	require.NoError(t, be.AbortMultipartUpload(ctx, bucket, key, uploadID))

	for _, p := range parts {
		assertAllPartShardsDeleted(t, be, bucket, key, uploadID, p.PartNumber, shardMaps[p.PartNumber])
	}
}

// Verification case 3: the completed object's own shards survive cleanup (regression guard
// that cleanup deletes part shards, not the assembled object).
func TestDistributed_CompleteMultipartUpload_PreservesAssembledObject(t *testing.T) {
	be, _, cleanup := setupMultipartTestBackend(t)
	defer cleanup()
	ctx := context.Background()
	const bucket, key = "test-bucket", "preserve-assembled.bin"

	part1 := bytes.Repeat([]byte{0x33}, int(multipart.MinPartSize))
	part2 := []byte("tail bytes")
	uploadID, parts := createUploadWithParts(t, be, bucket, key, [][]byte{part1, part2})

	_, err := be.CompleteMultipartUpload(ctx, &backend.CompleteMultipartUploadRequest{
		Bucket: bucket, Key: key, UploadID: uploadID, Parts: parts,
	})
	require.NoError(t, err)

	getResp, err := be.GetObject(ctx, &backend.GetObjectRequest{Bucket: bucket, Key: key, RangeStart: -1, RangeEnd: -1})
	require.NoError(t, err)
	defer getResp.Body.Close()

	readData, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)
	assert.Equal(t, append(append([]byte{}, part1...), part2...), readData)
}

// Verification case 4: upload + part metadata are removed from globalState after
// complete and after abort.
func TestDistributed_MultipartCleanup_RemovesMetadata(t *testing.T) {
	be, _, cleanup := setupMultipartTestBackend(t)
	defer cleanup()
	ctx := context.Background()
	const bucket = "test-bucket"

	assertMetadataGone := func(t *testing.T, uploadID string) {
		t.Helper()
		_, err := be.getUploadMetadata(uploadID)
		assert.Error(t, err)

		stored, err := be.getStoredParts(uploadID)
		require.NoError(t, err)
		assert.Empty(t, stored)
	}

	t.Run("complete", func(t *testing.T) {
		const key = "removes-metadata-complete.bin"
		part1 := bytes.Repeat([]byte{0x44}, int(multipart.MinPartSize))
		part2 := []byte("done")
		uploadID, parts := createUploadWithParts(t, be, bucket, key, [][]byte{part1, part2})

		_, err := be.CompleteMultipartUpload(ctx, &backend.CompleteMultipartUploadRequest{
			Bucket: bucket, Key: key, UploadID: uploadID, Parts: parts,
		})
		require.NoError(t, err)
		assertMetadataGone(t, uploadID)
	})

	t.Run("abort", func(t *testing.T) {
		const key = "removes-metadata-abort.bin"
		part1 := bytes.Repeat([]byte{0x55}, int(multipart.MinPartSize))
		uploadID, _ := createUploadWithParts(t, be, bucket, key, [][]byte{part1})

		require.NoError(t, be.AbortMultipartUpload(ctx, bucket, key, uploadID))
		assertMetadataGone(t, uploadID)
	})
}

// Verification case 5: with one node unreachable during cleanup the request still
// succeeds and the reachable nodes' shards are still deleted.
func TestDistributed_MultipartCleanup_NodeUnreachableStillSucceeds(t *testing.T) {
	be, quicServers, cleanup := setupMultipartTestBackend(t)
	defer cleanup()
	ctx := context.Background()
	const bucket, key = "test-bucket", "node-unreachable.bin"

	part1 := bytes.Repeat([]byte{0x66}, int(multipart.MinPartSize))
	part2 := []byte("small tail")
	uploadID, parts := createUploadWithParts(t, be, bucket, key, [][]byte{part1, part2})
	shardMaps := captureShardMaps(t, be, uploadID, parts)

	// Drop one node before cleanup; its shard deletes will fail and be skipped.
	// Evict the pooled connection so the next dial gets connection-refused, faithfully
	// simulating a down node rather than a half-open connection.
	const downNode uint32 = 0
	require.NoError(t, quicServers[downNode].Close())
	quicclient.InvalidatePooled(be.getNodeAddr(int(downNode)))

	require.NoError(t, be.AbortMultipartUpload(ctx, bucket, key, uploadID))

	for _, p := range parts {
		nodes := shardMaps[p.PartNumber]
		partObjKey := partObjectKey(bucket, key, uploadID, p.PartNumber)
		assertReachableShardGone := func(node uint32, shardIndex int) {
			if node == downNode {
				return
			}
			assert.False(t, shardExistsOnNode(t, be, node, bucket, partObjKey, shardIndex),
				"shard %d for part %d should be deleted on reachable node %d", shardIndex, p.PartNumber, node)
		}
		for i, node := range nodes.DataShardNodes {
			assertReachableShardGone(node, i)
		}
		for i, node := range nodes.ParityShardNodes {
			assertReachableShardGone(node, len(nodes.DataShardNodes)+i)
		}
	}
}

// Verification case 6: a missing part shard-map entry is skipped during cleanup and the
// upload metadata is still removed.
func TestDistributed_MultipartCleanup_MissingShardMapSkipsPart(t *testing.T) {
	be, _, cleanup := setupMultipartTestBackend(t)
	defer cleanup()
	ctx := context.Background()
	const bucket, key = "test-bucket", "missing-shard-map.bin"

	part1 := bytes.Repeat([]byte{0x77}, int(multipart.MinPartSize))
	part2 := []byte("tail")
	uploadID, _ := createUploadWithParts(t, be, bucket, key, [][]byte{part1, part2})

	// Remove part 1's shard-location map so cleanup must skip its shard delete.
	require.NoError(t, be.globalState.Delete(TableObjects, fmt.Appendf(nil, "part:%s:%05d", uploadID, 1)))

	require.NoError(t, be.AbortMultipartUpload(ctx, bucket, key, uploadID))

	_, err := be.getUploadMetadata(uploadID)
	assert.Error(t, err)
}
