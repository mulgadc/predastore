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
	"sync"
	"testing"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	v2credentials "github.com/aws/aws-sdk-go-v2/credentials"
	awss3v2 "github.com/aws/aws-sdk-go-v2/service/s3"
	awss3v2types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	awsv1 "github.com/aws/aws-sdk-go/aws"
	v1credentials "github.com/aws/aws-sdk-go/aws/credentials"
	v1session "github.com/aws/aws-sdk-go/aws/session"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type s3Adapter struct {
	listBuckets     func(t *testing.T) []string
	listObjects     func(t *testing.T, bucket string) []string
	getObject       func(t *testing.T, bucket, key string) []byte
	putObject       func(t *testing.T, bucket, key string, body []byte)
	multiPartUpload func(t *testing.T, bucket, key string, parts [][]byte)
}

const (
	S3_ENDPOINT     = "https://localhost:6443"
	S3_BUCKET       = "test-bucket01"
	TEXT_FILE_SHA   = "8a71e72cef7867d59c08ee233df6d2b7c35369734d0b6dae702857176a1d69f8"
	BINARY_FILE_SHA = "ce9d16a33fc9a53f592206c0cd23497632e78d3f6219dcd077ec9a11f50e6e4e"
)

// setupServer starts the S3 server for testing
func setupServer(t *testing.T) (cancel context.CancelFunc, wg *sync.WaitGroup) {
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

	// Create and configure the S3 server
	s3config := New(&Config{
		ConfigPath: "./tests/config/server.toml",
	})
	err := s3config.ReadConfig()
	require.NoError(t, err, "Failed to read config file")

	// Setup HTTP/2 server
	server := NewHTTP2Server(s3config)

	// Start the server in a goroutine
	go func() {
		defer wg.Done()

		// Start the HTTP/2 server with TLS
		err := server.ListenAndServe(":6443", "../config/server.pem", "../config/server.key")

		// Only report errors other than server closed
		if err != nil && err != http.ErrServerClosed {
			t.Logf("Server error: %v", err)
		}
	}()

	// Setup graceful shutdown
	go func() {
		<-ctx.Done()
		// Give the server up to 5 seconds to shutdown
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()

		// Shutdown gracefully
		if err := server.Shutdown(shutdownCtx); err != nil {
			fmt.Printf("Error shutting down server: %v\n", err)
		}
	}()

	// Wait for server to start
	time.Sleep(2 * time.Second)

	return cancel, wg
}

// loadTestConfig reads the shared test config once per test
func loadTestConfig(t *testing.T) *Config {
	s3config := New(&Config{
		ConfigPath: "./tests/config/server.toml",
	})
	err := s3config.ReadConfig()
	require.NoError(t, err, "Failed to read config file")
	return s3config
}

// createS3ClientV1 creates an AWS SDK v1 S3 client configured to use our local server
func createS3ClientV1(t *testing.T) *awss3.S3 {
	// Configure to skip SSL verification for our self-signed cert
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	httpClient := &http.Client{Transport: tr}

	cfg := loadTestConfig(t)

	sess, err := v1session.NewSession(&awsv1.Config{
		Region:           awsv1.String(cfg.Region),
		Endpoint:         awsv1.String(S3_ENDPOINT),
		S3ForcePathStyle: awsv1.Bool(true),
		Credentials:      v1credentials.NewStaticCredentials(cfg.Auth[0].AccessKeyID, cfg.Auth[0].SecretAccessKey, ""),
		HTTPClient:       httpClient,
	})
	require.NoError(t, err, "Failed to create AWS session")

	// Create S3 service client
	return awss3.New(sess)
}

// createS3ClientV2 creates an AWS SDK v2 S3 client configured to use our local server
func createS3ClientV2(t *testing.T) *awss3v2.Client {
	// Configure to skip SSL verification for our self-signed cert
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	httpClient := &http.Client{Transport: tr}

	cfg := loadTestConfig(t)

	// Create S3 client directly with options to avoid LoadDefaultConfig issues
	// with custom HTTP clients that don't implement WithTransportOptions
	client := awss3v2.New(awss3v2.Options{
		Region:       cfg.Region,
		Credentials:  v2credentials.NewStaticCredentialsProvider(cfg.Auth[0].AccessKeyID, cfg.Auth[0].SecretAccessKey, ""),
		HTTPClient:   httpClient,
		BaseEndpoint: awsv2.String(S3_ENDPOINT),
		UsePathStyle: true,
	})
	return client
}

func newS3AdapterV1(t *testing.T) s3Adapter {
	client := createS3ClientV1(t)

	return s3Adapter{
		listBuckets: func(t *testing.T) []string {
			result, err := client.ListBuckets(&awss3.ListBucketsInput{})
			require.NoError(t, err, "ListBuckets should not error")

			var buckets []string
			for _, bucket := range result.Buckets {
				buckets = append(buckets, awsv1.StringValue(bucket.Name))
			}
			return buckets
		},
		listObjects: func(t *testing.T, bucket string) []string {
			result, err := client.ListObjectsV2(&awss3.ListObjectsV2Input{
				Bucket: awsv1.String(bucket),
			})
			require.NoError(t, err, "ListObjectsV2 should not error")

			var keys []string
			for _, item := range result.Contents {
				keys = append(keys, awsv1.StringValue(item.Key))
			}
			return keys
		},
		getObject: func(t *testing.T, bucket, key string) []byte {
			result, err := client.GetObject(&awss3.GetObjectInput{
				Bucket: awsv1.String(bucket),
				Key:    awsv1.String(key),
			})
			require.NoError(t, err, "GetObject should not error")
			return readBody(t, result.Body)
		},
		putObject: func(t *testing.T, bucket, key string, body []byte) {
			_, err := client.PutObject(&awss3.PutObjectInput{
				Bucket: awsv1.String(bucket),
				Key:    awsv1.String(key),
				Body:   bytes.NewReader(body),
			})
			require.NoError(t, err, "PutObject should not error")
		},
		multiPartUpload: func(t *testing.T, bucket, key string, parts [][]byte) {
			initOut, err := client.CreateMultipartUpload(&awss3.CreateMultipartUploadInput{
				Bucket: awsv1.String(bucket),
				Key:    awsv1.String(key),
			})
			require.NoError(t, err, "CreateMultipartUpload should not error")

			var completedParts []*awss3.CompletedPart

			for idx, part := range parts {
				partNumber := int64(idx + 1)
				uploadResp, err := client.UploadPart(&awss3.UploadPartInput{
					Bucket:     awsv1.String(bucket),
					Key:        awsv1.String(key),
					UploadId:   initOut.UploadId,
					PartNumber: awsv1.Int64(partNumber),
					Body:       bytes.NewReader(part),
				})
				require.NoErrorf(t, err, "UploadPart %d should not error", partNumber)
				require.NotNil(t, uploadResp.ETag, "UploadPart %d ETag should not be nil", partNumber)

				completedParts = append(completedParts, &awss3.CompletedPart{
					ETag:       uploadResp.ETag,
					PartNumber: awsv1.Int64(partNumber),
				})
			}

			_, err = client.CompleteMultipartUpload(&awss3.CompleteMultipartUploadInput{
				Bucket:   awsv1.String(bucket),
				Key:      awsv1.String(key),
				UploadId: initOut.UploadId,
				MultipartUpload: &awss3.CompletedMultipartUpload{
					Parts: completedParts,
				},
			})
			require.NoError(t, err, "CompleteMultipartUpload should not error")
		},
	}
}

func newS3AdapterV2(t *testing.T) s3Adapter {
	client := createS3ClientV2(t)

	return s3Adapter{
		listBuckets: func(t *testing.T) []string {
			result, err := client.ListBuckets(context.Background(), &awss3v2.ListBucketsInput{})
			require.NoError(t, err, "ListBuckets should not error")

			var buckets []string
			for _, bucket := range result.Buckets {
				buckets = append(buckets, awsv2.ToString(bucket.Name))
			}
			return buckets
		},
		listObjects: func(t *testing.T, bucket string) []string {
			result, err := client.ListObjectsV2(context.Background(), &awss3v2.ListObjectsV2Input{
				Bucket: awsv2.String(bucket),
			})
			require.NoError(t, err, "ListObjectsV2 should not error")

			var keys []string
			for _, item := range result.Contents {
				keys = append(keys, awsv2.ToString(item.Key))
			}
			return keys
		},
		getObject: func(t *testing.T, bucket, key string) []byte {
			result, err := client.GetObject(context.Background(), &awss3v2.GetObjectInput{
				Bucket: awsv2.String(bucket),
				Key:    awsv2.String(key),
			})
			require.NoError(t, err, "GetObject should not error")
			return readBody(t, result.Body)
		},
		putObject: func(t *testing.T, bucket, key string, body []byte) {
			_, err := client.PutObject(context.Background(), &awss3v2.PutObjectInput{
				Bucket: awsv2.String(bucket),
				Key:    awsv2.String(key),
				Body:   bytes.NewReader(body),
			})
			require.NoError(t, err, "PutObject should not error")
		},
		multiPartUpload: func(t *testing.T, bucket, key string, parts [][]byte) {
			initOut, err := client.CreateMultipartUpload(context.Background(), &awss3v2.CreateMultipartUploadInput{
				Bucket: awsv2.String(bucket),
				Key:    awsv2.String(key),
			})
			require.NoError(t, err, "CreateMultipartUpload should not error")

			var completedParts []awss3v2types.CompletedPart

			for idx, part := range parts {
				partNumber := int32(idx + 1)
				uploadResp, err := client.UploadPart(context.Background(), &awss3v2.UploadPartInput{
					Bucket:     awsv2.String(bucket),
					Key:        awsv2.String(key),
					UploadId:   initOut.UploadId,
					PartNumber: awsv2.Int32(partNumber),
					Body:       bytes.NewReader(part),
				})
				require.NoErrorf(t, err, "UploadPart %d should not error", partNumber)

				completedParts = append(completedParts, awss3v2types.CompletedPart{
					ETag:       uploadResp.ETag,
					PartNumber: awsv2.Int32(partNumber),
				})
			}

			_, err = client.CompleteMultipartUpload(context.Background(), &awss3v2.CompleteMultipartUploadInput{
				Bucket:   awsv2.String(bucket),
				Key:      awsv2.String(key),
				UploadId: initOut.UploadId,
				MultipartUpload: &awss3v2types.CompletedMultipartUpload{
					Parts: completedParts,
				},
			})
			require.NoError(t, err, "CompleteMultipartUpload should not error")
		},
	}
}

func runIntegrationSuite(t *testing.T, client s3Adapter) {
	t.Helper()

	t.Run("ListBuckets", func(t *testing.T) {
		buckets := client.listBuckets(t)
		assert.Contains(t, buckets, S3_BUCKET, "TestBucket should be in the list of buckets")
	})

	t.Run("ListObjects", func(t *testing.T) {
		keys := client.listObjects(t, S3_BUCKET)
		assert.Contains(t, keys, "test.txt", "test.txt should be in the bucket")
		assert.Contains(t, keys, "binary.dat", "binary.dat should be in the bucket")
	})

	t.Run("GetObject", func(t *testing.T) {
		textBytes := client.getObject(t, S3_BUCKET, "test.txt")
		expectedText := mustReadFile(t, "./tests/data/test-bucket01/test.txt")
		assert.Equal(t, expectedText, textBytes, "Text file content should match")

		binaryBytes := client.getObject(t, S3_BUCKET, "binary.dat")
		expectedBinary := mustReadFile(t, "./tests/data/test-bucket01/binary.dat")
		assert.Equal(t, expectedBinary, binaryBytes, "Binary file content should match")
	})

	t.Run("PutObject", func(t *testing.T) {
		testContent := []byte("This is a new test file created during integration testing")

		client.putObject(t, S3_BUCKET, "new_test_file.txt", testContent)

		downloadedBytes := client.getObject(t, S3_BUCKET, "new_test_file.txt")
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

		var parts [][]byte
		for i := 0; i < partCount; i++ {
			start := i * partSize
			end := start + partSize
			parts = append(parts, data[start:end])
		}

		const key = "multipart_test.bin"
		client.multiPartUpload(t, S3_BUCKET, key, parts)

		downloaded := client.getObject(t, S3_BUCKET, key)
		assert.Equal(t, data, downloaded, "Multipart uploaded content should match original data")
	})
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
	cancel, wg := setupServer(t)
	defer func() {
		cancel()
		wg.Wait()
	}()

	for _, tc := range []struct {
		name   string
		client func(t *testing.T) s3Adapter
	}{
		{
			name:   "aws-sdk-go-v1",
			client: newS3AdapterV1,
		},
		{
			name:   "aws-sdk-go-v2",
			client: newS3AdapterV2,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			runIntegrationSuite(t, tc.client(t))
		})
	}
}

// TestGetObjectHead tests the HEAD request for an object
func TestGetObjectHeadIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Start the server
	cancel, wg := setupServer(t)
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
	req, err := http.NewRequest("HEAD", S3_ENDPOINT+"/"+S3_BUCKET+"/test.txt", nil)
	require.NoError(t, err, "Creating HEAD request should not error")

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
	cancel, wg := setupServer(t)
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
	fullText, err := os.ReadFile("./tests/data/test-bucket01/test.txt")
	require.NoError(t, err, "Reading text file should not error")

	// Test range request
	req, err := http.NewRequest("GET", S3_ENDPOINT+"/"+S3_BUCKET+"/test.txt", nil)
	require.NoError(t, err, "Creating range request should not error")

	// Request first 10 bytes
	req.Header.Set("Range", "bytes=0-9")

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
