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

	s3backend "github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/quic/quicserver"
	"github.com/stretchr/testify/require"
)

// TestPutObjectWithTempFile tests the production code path where PutObject
// receives a request with a different key than the temp file path used internally.
// This is the actual code path used when data comes in via the S3 HTTP API.
func TestPutObjectWithTempFile(t *testing.T) {
	const bucket = "test-bucket"
	const objectKey = "vol-xxx/chunks/chunk.00000000.bin" // Simulates viperblock S3 key
	const objectSize = 128 * 1024                         // 128KB

	tmpDir, err := os.MkdirTemp(os.TempDir(), fmt.Sprintf("unit-test-%d", time.Now().UnixNano()))
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Use high ports to avoid conflicts with running services
	testBasePort := 29991

	cfg := &Config{
		BadgerDir:      tmpDir,
		PartitionCount: 5,
		UseQUIC:        true,
		QuicBasePort:   testBasePort,
	}
	b, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, b)
	defer b.Close()

	backend := b.(*Backend)

	// Set data directory for nodes
	dataDir := filepath.Join(tmpDir, "nodes")
	backend.SetDataDir(dataDir)

	// Start QUIC servers for nodes 0-4 on test ports
	quicServers := make([]*quicserver.QuicServer, 5)
	for i := range 5 {
		nodeDir := filepath.Join(dataDir, fmt.Sprintf("node-%d", i))
		require.NoError(t, os.MkdirAll(nodeDir, 0750))

		qs, err := quicserver.NewWithRetry(nodeDir, fmt.Sprintf("127.0.0.1:%d", testBasePort+i), 5)
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

	// Give QUIC servers time to start
	time.Sleep(200 * time.Millisecond)

	// Create deterministic content (simulating viperblock chunk with header)
	orig := make([]byte, objectSize)
	// First 10 bytes: chunk header (VBCH magic + version + block size)
	copy(orig[0:4], []byte("VBCH")) // Magic
	orig[4] = 0x01                  // Version high byte
	orig[5] = 0x00                  // Version low byte
	orig[6] = 0x00                  // Block size bytes
	orig[7] = 0x10
	orig[8] = 0x00
	orig[9] = 0x00
	// Rest: block data with recognizable pattern
	for i := 10; i < objectSize; i++ {
		orig[i] = byte((i + objectSize) % 251)
	}

	ctx := context.Background()

	// PUT the object via the production PutObject path (uses temp file internally)
	putReq := &s3backend.PutObjectRequest{
		Bucket: bucket,
		Key:    objectKey,
		Body:   bytes.NewReader(orig),
	}
	putResp, err := backend.PutObject(ctx, putReq)
	require.NoError(t, err, "PutObject should succeed")
	require.NotNil(t, putResp)
	t.Logf("PutObject succeeded: ETag=%s", putResp.ETag)

	// Test range requests (simulating viperblock reading block 0)
	testCases := []struct {
		name       string
		rangeStart int64
		rangeEnd   int64
	}{
		{"First 512 bytes at offset 0", 0, 511},
		{"Block 0 data (offset 10, len 4096)", 10, 4105}, // This is what viperblock requests
		{"Middle range", 4096, 8191},
		{"Last 1024 bytes", int64(objectSize - 1024), int64(objectSize - 1)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := &s3backend.GetObjectRequest{
				Bucket:     bucket,
				Key:        objectKey,
				RangeStart: tc.rangeStart,
				RangeEnd:   tc.rangeEnd,
			}

			resp, err := backend.GetObject(ctx, req)
			require.NoError(t, err, "GetObject with range should succeed")
			require.NotNil(t, resp)
			require.Equal(t, 206, resp.StatusCode, "Should return 206 Partial Content")
			defer resp.Body.Close()

			// Read the response body
			data, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			// Verify size
			expectedLen := tc.rangeEnd - tc.rangeStart + 1
			require.Equal(t, expectedLen, int64(len(data)), "Response size should match requested range")

			// Verify content matches original
			expectedData := orig[tc.rangeStart : tc.rangeEnd+1]
			require.Equal(t, expectedData, data, "Response data should match original bytes at specified range")

			t.Logf("Range %d-%d: got %d bytes, content verified", tc.rangeStart, tc.rangeEnd, len(data))

			// For the viperblock case, verify it's not all zeros
			if tc.name == "Block 0 data (offset 10, len 4096)" {
				allZeros := true
				for _, b := range data {
					if b != 0 {
						allZeros = false
						break
					}
				}
				require.False(t, allZeros, "Block 0 data should NOT be all zeros")
				t.Logf("Block 0 first 32 bytes: %x", data[:32])
			}
		})
	}
}
