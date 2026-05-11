package s3

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awss3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/mulgadc/predastore/auth"
	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/backend/distributed"
	"github.com/mulgadc/predastore/internal/storetest"
	"github.com/mulgadc/predastore/quic/quicserver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	S3_ENDPOINT = "https://localhost:6443"
	S3_BUCKET   = "test-bucket01"
)

// setupServer starts an S3 server backed by a distributed backend for testing.
// Returns the per-node data directory so callers can scan segment files on
// disk for ciphertext assertions.
func setupServer(t *testing.T) (cancel context.CancelFunc, wg *sync.WaitGroup, nodeDataDir string) {
	t.Helper()

	// These are integration tests that spin up a TLS server on a fixed port.
	// In restricted environments (e.g. sandboxed CI) binding to ports may be disallowed.
	ln, lerr := net.Listen("tcp4", "127.0.0.1:6443")
	if lerr != nil {
		t.Skipf("skipping integration tests: cannot listen on 127.0.0.1:6443: %v", lerr)
	}
	_ = ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	wg = &sync.WaitGroup{}
	wg.Add(1)

	tmpDir := t.TempDir()
	nodeDataDir = filepath.Join(tmpDir, "nodes")
	badgerDir := filepath.Join(tmpDir, "db")
	require.NoError(t, os.MkdirAll(nodeDataDir, 0750))
	require.NoError(t, os.MkdirAll(badgerDir, 0750))

	s3config := loadTestConfig(t)
	s3config.BadgerDir = badgerDir

	nodeCount := len(s3config.Nodes)
	if nodeCount == 0 {
		nodeCount = 5
	}

	basePort := 10000 + (int(time.Now().UnixNano()/1000000) % 5000)

	be, err := distributed.New(&distributed.Config{
		BadgerDir:      badgerDir,
		DataDir:        nodeDataDir,
		DataShards:     s3config.RS.Data,
		ParityShards:   s3config.RS.Parity,
		PartitionCount: nodeCount,
		QuicBasePort:   basePort,
	})
	require.NoError(t, err, "Should create distributed backend")

	quicServers := make([]*quicserver.QuicServer, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodeDir := filepath.Join(nodeDataDir, fmt.Sprintf("node-%d", i))
		require.NoError(t, os.MkdirAll(nodeDir, 0750))
		addr := fmt.Sprintf("127.0.0.1:%d", basePort+i)
		quicServers[i] = quicserver.New(nodeDir, addr, quicserver.WithMasterKey(storetest.TestMasterKey))
	}
	time.Sleep(100 * time.Millisecond)

	// Create test bucket via API so it appears in globalState (and thus in ListBuckets).
	// AccountID must match the auth config so ListBuckets filtering works.
	ownerID := ""
	accountID := ""
	if len(s3config.Auth) > 0 {
		ownerID = s3config.Auth[0].AccessKeyID
		accountID = s3config.Auth[0].AccountID
	}
	_, cbErr := be.CreateBucket(context.Background(), &backend.CreateBucketRequest{
		Bucket:    S3_BUCKET,
		Region:    s3config.Region,
		OwnerID:   ownerID,
		AccountID: accountID,
	})
	require.NoError(t, cbErr, "CreateBucket %q should succeed", S3_BUCKET)

	// Upload fixtures so dev's tests (which previously read these straight from
	// the filesystem backend) can find them in the distributed backend.
	for _, f := range []string{"test.txt", "binary.dat"} {
		data, ferr := os.ReadFile(filepath.Join("testdata", "test-bucket01", f))
		require.NoError(t, ferr, "reading fixture %s", f)
		_, perr := be.PutObject(context.Background(), &backend.PutObjectRequest{
			Bucket:        S3_BUCKET,
			Key:           f,
			Body:          bytes.NewReader(data),
			ContentLength: int64(len(data)),
		})
		require.NoError(t, perr, "PutObject fixture %s", f)
	}

	server := NewHTTP2ServerWithBackend(s3config, be, NewConfigProvider(s3config.Auth))

	// Start the server in a goroutine
	go func() {
		defer wg.Done()
		if err := server.ListenAndServe(":6443", "../certs/server.pem", "../certs/server.key"); err != nil && err != http.ErrServerClosed {
			t.Logf("Server error: %v", err)
		}
	}()

	// Setup graceful shutdown
	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			fmt.Printf("Error shutting down server: %v\n", err)
		}
		for _, qs := range quicServers {
			if qs != nil {
				_ = qs.Close()
			}
		}
		be.Close()
	}()

	// Wait for server to start
	time.Sleep(2 * time.Second)

	return cancel, wg, nodeDataDir
}

// loadTestConfig reads the shared test config once per test
func loadTestConfig(t *testing.T) *Config {
	s3config := New(&Config{
		ConfigPath: filepath.Join("..", "config", "7node.toml"),
	})
	err := s3config.ReadConfig()
	require.NoError(t, err, "Failed to read config file")
	return s3config
}

// createS3Client creates an AWS SDK v2 S3 client configured to use our local server
func createS3Client(t *testing.T) *awss3.Client {
	// Configure to skip SSL verification for our self-signed cert
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	httpClient := &http.Client{Transport: tr}

	cfg := loadTestConfig(t)

	return awss3.New(awss3.Options{
		Region:       cfg.Region,
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.Auth[0].AccessKeyID, cfg.Auth[0].SecretAccessKey, ""),
		HTTPClient:   httpClient,
		BaseEndpoint: aws.String(S3_ENDPOINT),
		UsePathStyle: true,
	})
}

func runIntegrationSuite(t *testing.T, client *awss3.Client, nodeDataDir string) {
	t.Helper()
	ctx := context.Background()

	t.Run("ListBuckets", func(t *testing.T) {
		result, err := client.ListBuckets(ctx, &awss3.ListBucketsInput{})
		require.NoError(t, err, "ListBuckets should not error")

		var buckets []string
		for _, bucket := range result.Buckets {
			buckets = append(buckets, aws.ToString(bucket.Name))
		}
		assert.Contains(t, buckets, S3_BUCKET, "TestBucket should be in the list of buckets")
	})

	t.Run("ListObjects", func(t *testing.T) {
		result, err := client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
			Bucket: aws.String(S3_BUCKET),
		})
		require.NoError(t, err, "ListObjectsV2 should not error")

		var keys []string
		for _, item := range result.Contents {
			keys = append(keys, aws.ToString(item.Key))
		}
		assert.Contains(t, keys, "test.txt", "test.txt should be in the bucket")
		assert.Contains(t, keys, "binary.dat", "binary.dat should be in the bucket")
	})

	t.Run("GetObject", func(t *testing.T) {
		textBytes := getObject(t, client, S3_BUCKET, "test.txt")
		expectedText := mustReadFile(t, "./testdata/test-bucket01/test.txt")
		assert.Equal(t, expectedText, textBytes, "Text file content should match")

		binaryBytes := getObject(t, client, S3_BUCKET, "binary.dat")
		expectedBinary := mustReadFile(t, "./testdata/test-bucket01/binary.dat")
		assert.Equal(t, expectedBinary, binaryBytes, "Binary file content should match")
	})

	t.Run("PutObject", func(t *testing.T) {
		testContent := []byte("This is a new test file created during integration testing")

		_, err := client.PutObject(ctx, &awss3.PutObjectInput{
			Bucket: aws.String(S3_BUCKET),
			Key:    aws.String("new_test_file.txt"),
			Body:   bytes.NewReader(testContent),
		})
		require.NoError(t, err, "PutObject should not error")

		downloadedBytes := getObject(t, client, S3_BUCKET, "new_test_file.txt")
		assert.Equal(t, testContent, downloadedBytes, "Downloaded content should match uploaded content")

		localPath := filepath.Join("tests", "data", "new_test_file.txt")
		if _, err := os.Stat(localPath); err == nil {
			err = os.Remove(localPath)
			assert.NoError(t, err, "Removing test file should not error")
		}
	})

	t.Run("MultiPartUpload", func(t *testing.T) {
		const totalSize = 20 * 1024 * 1024 // 20MB
		const partCount = 4
		const partSize = totalSize / partCount

		data := make([]byte, totalSize)
		for i := range data {
			data[i] = byte(i % 251) // deterministic pattern
		}

		const key = "multipart_test.bin"

		initOut, err := client.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
			Bucket: aws.String(S3_BUCKET),
			Key:    aws.String(key),
		})
		require.NoError(t, err, "CreateMultipartUpload should not error")

		var completedParts []awss3types.CompletedPart
		for i := range partCount {
			partNumber := int32(i + 1)
			uploadResp, err := client.UploadPart(ctx, &awss3.UploadPartInput{
				Bucket:     aws.String(S3_BUCKET),
				Key:        aws.String(key),
				UploadId:   initOut.UploadId,
				PartNumber: aws.Int32(partNumber),
				Body:       bytes.NewReader(data[i*partSize : (i+1)*partSize]),
			})
			require.NoErrorf(t, err, "UploadPart %d should not error", partNumber)

			completedParts = append(completedParts, awss3types.CompletedPart{
				ETag:       uploadResp.ETag,
				PartNumber: aws.Int32(partNumber),
			})
		}

		_, err = client.CompleteMultipartUpload(ctx, &awss3.CompleteMultipartUploadInput{
			Bucket:   aws.String(S3_BUCKET),
			Key:      aws.String(key),
			UploadId: initOut.UploadId,
			MultipartUpload: &awss3types.CompletedMultipartUpload{
				Parts: completedParts,
			},
		})
		require.NoError(t, err, "CompleteMultipartUpload should not error")

		downloaded := getObject(t, client, S3_BUCKET, key)
		assert.Equal(t, data, downloaded, "Multipart uploaded content should match original data")
	})

	t.Run("CiphertextOnDisk", func(t *testing.T) {
		// Distinctive 16-byte marker repeated to fill 256 bytes — large enough
		// to survive Reed-Solomon sharding boundaries and land contiguously in
		// at least one data shard's fragment body. If encryption is broken, the
		// marker bytes will appear verbatim on disk.
		marker := []byte("MULGA-CIPHERTEXT")
		payload := bytes.Repeat(marker, 16)

		const key = "ciphertext_check.bin"
		_, err := client.PutObject(ctx, &awss3.PutObjectInput{
			Bucket: aws.String(S3_BUCKET),
			Key:    aws.String(key),
			Body:   bytes.NewReader(payload),
		})
		require.NoError(t, err, "PutObject for ciphertext check should not error")

		// Round-trip first to confirm the object is actually retrievable —
		// the on-disk scan only proves a negative, so the GET protects against
		// false-positives from objects that never reached the store.
		got := getObject(t, client, S3_BUCKET, key)
		assert.Equal(t, payload, got, "round-trip plaintext must match")

		var segFiles []string
		err = filepath.WalkDir(nodeDataDir, func(path string, d os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if !d.IsDir() && strings.HasSuffix(path, ".seg") {
				segFiles = append(segFiles, path)
			}
			return nil
		})
		require.NoError(t, err, "walking node data dir for segment files should not error")
		require.NotEmpty(t, segFiles, "expected at least one segment file after a PUT — test cannot prove encryption otherwise")

		for _, path := range segFiles {
			data, err := os.ReadFile(path)
			require.NoError(t, err, "reading segment file %s", path)
			assert.NotContains(t, string(data), string(marker),
				"plaintext marker leaked into segment file %s — encryption is not sealing fragment bodies", path)
		}
	})

	t.Run("ZeroByteRoundTrip", func(t *testing.T) {
		// The store-level 0-byte short-circuit (Stage 1) is correct, but the
		// distributed backend's Reed-Solomon sharding rejects empty bodies
		// before reaching the store. Tracked by mulga-bm-13; un-skip when fixed.
		t.Skip("blocked on mulga-bm-13: distributed PutObject rejects 0-byte bodies")

		const key = "empty.bin"

		_, err := client.PutObject(ctx, &awss3.PutObjectInput{
			Bucket: aws.String(S3_BUCKET),
			Key:    aws.String(key),
			Body:   bytes.NewReader(nil),
		})
		require.NoError(t, err, "PutObject with empty body should not error")

		result, err := client.GetObject(ctx, &awss3.GetObjectInput{
			Bucket: aws.String(S3_BUCKET),
			Key:    aws.String(key),
		})
		require.NoError(t, err, "GetObject for 0-byte object should not error")
		body := readBody(t, result.Body)
		assert.Empty(t, body, "0-byte round-trip must yield 0 bytes")
	})
}

func getObject(t *testing.T, client *awss3.Client, bucket, key string) []byte {
	t.Helper()
	result, err := client.GetObject(context.Background(), &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err, "GetObject should not error")
	return readBody(t, result.Body)
}

func readBody(t *testing.T, body io.ReadCloser) []byte {
	t.Helper()
	defer body.Close()

	data, err := io.ReadAll(body)
	require.NoError(t, err, "Reading response body should not error")
	return data
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err, "Reading local file should not error")
	return data
}

func TestS3Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Start the server
	cancel, wg, nodeDataDir := setupServer(t)
	defer func() {
		cancel()
		wg.Wait()
	}()

	runIntegrationSuite(t, createS3Client(t), nodeDataDir)
}

// TestGetObjectHead tests the HEAD request for an object
func TestGetObjectHeadIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Start the server
	cancel, wg, _ := setupServer(t)
	defer func() {
		cancel()
		wg.Wait()
	}()

	// Create a custom HTTP client that doesn't verify TLS certs
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}

	// Create HEAD request
	req, err := http.NewRequest(http.MethodHead, S3_ENDPOINT+"/"+S3_BUCKET+"/test.txt", nil)
	require.NoError(t, err, "Creating HEAD request should not error")

	cfg := loadTestConfig(t)
	timestamp := time.Now().UTC().Format("20060102T150405Z")
	err = auth.GenerateAuthHeaderReq(cfg.Auth[0].AccessKeyID, cfg.Auth[0].SecretAccessKey, timestamp, cfg.Region, "s3", req)
	require.NoError(t, err, "Generating SigV4 auth header should not error")

	// Send request
	resp, err := client.Do(req)
	require.NoError(t, err, "HEAD request should not error")
	defer resp.Body.Close()

	// Check response
	assert.Equal(t, http.StatusOK, resp.StatusCode, "HEAD request should return 200")
	assert.NotEmpty(t, resp.Header.Get("Content-Type"), "Content-Type should be set")
	assert.NotEmpty(t, resp.Header.Get("Content-Length"), "Content-Length should be set")
	assert.NotEmpty(t, resp.Header.Get("ETag"), "ETag should be set")

	// The body should be empty for HEAD requests
	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err, "Reading HEAD response body should not error")
	assert.Empty(t, body, "HEAD response body should be empty")
}

// TestByteRangeRequests tests the Range header functionality
func TestByteRangeRequests(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Start the server
	cancel, wg, _ := setupServer(t)
	defer func() {
		cancel()
		wg.Wait()
	}()

	// Create a custom HTTP client that doesn't verify TLS certs
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}

	// Get the full text file content for comparison
	fullText, err := os.ReadFile("./testdata/test-bucket01/test.txt")
	require.NoError(t, err, "Reading text file should not error")

	// Test range request
	req, err := http.NewRequest(http.MethodGet, S3_ENDPOINT+"/"+S3_BUCKET+"/test.txt", nil)
	require.NoError(t, err, "Creating range request should not error")

	// Request first 10 bytes
	req.Header.Set("Range", "bytes=0-9")

	cfg := loadTestConfig(t)
	timestamp := time.Now().UTC().Format("20060102T150405Z")
	err = auth.GenerateAuthHeaderReq(cfg.Auth[0].AccessKeyID, cfg.Auth[0].SecretAccessKey, timestamp, cfg.Region, "s3", req)
	require.NoError(t, err, "Generating SigV4 auth header should not error")

	// Send request
	resp, err := client.Do(req)
	require.NoError(t, err, "Range request should not error")
	defer resp.Body.Close()

	// Check response - 206 Partial Content is correct per RFC 7233 for range requests
	assert.Equal(t, http.StatusPartialContent, resp.StatusCode, "Range request should return 206 Partial Content")
	assert.NotEmpty(t, resp.Header.Get("Content-Range"), "Content-Range should be set for partial content")

	// Read the partial content
	partialContent, err := io.ReadAll(resp.Body)
	assert.NoError(t, err, "Reading partial content should not error")

	// Verify we got the right bytes
	assert.Equal(t, fullText[:10], partialContent, "Partial content should match first 10 bytes")
}
