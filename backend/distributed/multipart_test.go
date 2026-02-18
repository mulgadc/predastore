package distributed

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/backend/multipart"
	"github.com/mulgadc/predastore/quic/quicserver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// multipartPortCounter gives each test invocation a unique port range to avoid
// bind conflicts when the OS hasn't fully released UDP ports from the prior test.
var multipartPortCounter atomic.Int32

// setupMultipartTestBackend creates a distributed backend with QUIC servers for testing
func setupMultipartTestBackend(t *testing.T) (*Backend, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp(os.TempDir(), fmt.Sprintf("multipart-test-%d", time.Now().UnixNano()))
	require.NoError(t, err)

	// Each invocation gets a unique port range to avoid bind conflicts
	testBasePort := 39991 + int(multipartPortCounter.Add(1)-1)*10

	cfg := &Config{
		BadgerDir:      tmpDir,
		PartitionCount: 5,
		UseQUIC:        true,
		QuicBasePort:   testBasePort,
	}
	b, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, b)

	be := b.(*Backend)
	dataDir := filepath.Join(tmpDir, "nodes")
	be.SetDataDir(dataDir)

	// Start QUIC servers
	quicServers := make([]*quicserver.QuicServer, 5)
	for i := 0; i < 5; i++ {
		nodeDir := filepath.Join(dataDir, fmt.Sprintf("node-%d", i))
		require.NoError(t, os.MkdirAll(nodeDir, 0750))

		qs, err := quicserver.NewWithRetry(nodeDir, fmt.Sprintf("127.0.0.1:%d", testBasePort+i), 5)
		require.NoError(t, err, "Failed to start QUIC server for node %d", i)
		quicServers[i] = qs
	}

	// Give QUIC servers time to start
	time.Sleep(200 * time.Millisecond)

	cleanup := func() {
		for _, qs := range quicServers {
			if qs != nil {
				_ = qs.Close()
			}
		}
		be.Close()
		os.RemoveAll(tmpDir)
	}

	return be, cleanup
}

func TestDistributed_CreateMultipartUpload(t *testing.T) {
	be, cleanup := setupMultipartTestBackend(t)
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
	be, cleanup := setupMultipartTestBackend(t)
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
	be, cleanup := setupMultipartTestBackend(t)
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
	be, cleanup := setupMultipartTestBackend(t)
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
	be, cleanup := setupMultipartTestBackend(t)
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

	be, cleanup := setupMultipartTestBackend(t)
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

	for i := 0; i < numParts; i++ {
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
	be, cleanup := setupMultipartTestBackend(t)
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
