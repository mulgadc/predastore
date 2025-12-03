package s3

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	v2config "github.com/aws/aws-sdk-go-v2/config"
	v2credentials "github.com/aws/aws-sdk-go-v2/credentials"
	awss3v2 "github.com/aws/aws-sdk-go-v2/service/s3"
	awss3v2types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
)

func startBenchServer(tb testing.TB) (context.CancelFunc, *sync.WaitGroup) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)

	s3server := New(&Config{
		ConfigPath: "./tests/config/server.toml",
	})
	require.NoError(tb, s3server.ReadConfig(), "Failed to read config file")

	s3server.DisableLogging = true
	app := s3server.SetupRoutes()

	go func() {
		defer wg.Done()
		if err := app.ListenTLS(":8443", "../config/server.pem", "../config/server.key"); err != nil {
			tb.Logf("Server error: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := app.ShutdownWithContext(shutdownCtx); err != nil {
			tb.Logf("Error shutting down server: %v", err)
		}
	}()

	time.Sleep(2 * time.Second)
	return cancel, wg
}

func createBenchClientV2(tb testing.TB) *awss3v2.Client {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	httpClient := &http.Client{Transport: tr}

	cfg := New(&Config{
		ConfigPath: "./tests/config/server.toml",
	})
	require.NoError(tb, cfg.ReadConfig(), "Failed to read config file")

	awsCfg, err := v2config.LoadDefaultConfig(
		context.Background(),
		v2config.WithRegion(cfg.Region),
		v2config.WithHTTPClient(httpClient),
		v2config.WithEndpointResolverWithOptions(
			awsv2.EndpointResolverWithOptionsFunc(func(service, region string, _ ...interface{}) (awsv2.Endpoint, error) {
				if service == awss3v2.ServiceID {
					return awsv2.Endpoint{
						URL:               S3_ENDPOINT,
						HostnameImmutable: true,
					}, nil
				}
				return awsv2.Endpoint{}, &awsv2.EndpointNotFoundError{}
			}),
		),
		v2config.WithCredentialsProvider(v2credentials.NewStaticCredentialsProvider(
			cfg.Auth[0].AccessKeyID,
			cfg.Auth[0].SecretAccessKey,
			"",
		)),
	)
	require.NoError(tb, err, "Failed to create AWS v2 config")

	return awss3v2.NewFromConfig(awsCfg, func(o *awss3v2.Options) {
		o.UsePathStyle = true
	})
}

func multipartUpload(tb testing.TB, client *awss3v2.Client, bucket, key string, data []byte) {
	initOut, err := client.CreateMultipartUpload(context.Background(), &awss3v2.CreateMultipartUploadInput{
		Bucket: awsv2.String(bucket),
		Key:    awsv2.String(key),
	})
	require.NoError(tb, err, "CreateMultipartUpload should not error")

	var completedParts []awss3v2types.CompletedPart
	const chunkSize = 5 * 1024 * 1024
	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}
		partNumber := int32(len(completedParts) + 1)
		uploadResp, err := client.UploadPart(context.Background(), &awss3v2.UploadPartInput{
			Bucket:     awsv2.String(bucket),
			Key:        awsv2.String(key),
			UploadId:   initOut.UploadId,
			PartNumber: awsv2.Int32(partNumber),
			Body:       bytes.NewReader(data[i:end]),
		})
		require.NoErrorf(tb, err, "UploadPart %d should not error", partNumber)
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
	require.NoError(tb, err, "CompleteMultipartUpload should not error")
}

func BenchmarkS3PutV2(b *testing.B) {
	sizes := []int{
		128 * 1024,
		256 * 1024,
		1 * 1024 * 1024,
		5 * 1024 * 1024,
		8 * 1024 * 1024,
		16 * 1024 * 1024,
		32 * 1024 * 1024,
		64 * 1024 * 1024,
	}

	for _, size := range sizes {
		size := size
		b.Run(fmt.Sprintf("BenchmarkS3PutV2-%s", prettySize(size)), func(b *testing.B) {
			cancel, wg := startBenchServer(b)
			defer func() {
				cancel()
				wg.Wait()
			}()

			client := createBenchClientV2(b)
			data := make([]byte, size)
			for i := range data {
				data[i] = byte(i % 251)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("bench-put-%d-%d", size, i)
				if size > 5*1024*1024 {
					multipartUpload(b, client, S3_BUCKET, key, data)
				} else {
					_, err := client.PutObject(context.Background(), &awss3v2.PutObjectInput{
						Bucket: awsv2.String(S3_BUCKET),
						Key:    awsv2.String(key),
						Body:   bytes.NewReader(data),
					})
					require.NoError(b, err, "PutObject should not error")
				}
			}
		})
	}
}

func BenchmarkS3GetV2(b *testing.B) {
	sizes := []int{
		128 * 1024,
		256 * 1024,
		1 * 1024 * 1024,
		5 * 1024 * 1024,
		8 * 1024 * 1024,
		16 * 1024 * 1024,
		32 * 1024 * 1024,
		64 * 1024 * 1024,
	}

	for _, size := range sizes {
		size := size
		b.Run(fmt.Sprintf("BenchmarkS3GetV2-%s", prettySize(size)), func(b *testing.B) {
			cancel, wg := startBenchServer(b)
			defer func() {
				cancel()
				wg.Wait()
			}()

			client := createBenchClientV2(b)
			data := make([]byte, size)
			for i := range data {
				data[i] = byte(i % 251)
			}
			key := fmt.Sprintf("bench-get-%d", size)
			if size > 5*1024*1024 {
				multipartUpload(b, client, S3_BUCKET, key, data)
			} else {
				_, err := client.PutObject(context.Background(), &awss3v2.PutObjectInput{
					Bucket: awsv2.String(S3_BUCKET),
					Key:    awsv2.String(key),
					Body:   bytes.NewReader(data),
				})
				require.NoError(b, err, "PutObject should not error")
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resp, err := client.GetObject(context.Background(), &awss3v2.GetObjectInput{
					Bucket: awsv2.String(S3_BUCKET),
					Key:    awsv2.String(key),
				})
				require.NoError(b, err, "GetObject should not error")
				_, err = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				require.NoError(b, err, "Reading object should not error")
			}
		})
	}
}

func BenchmarkS3MultipartV2(b *testing.B) {
	sizes := []int{
		8 * 1024 * 1024,
		16 * 1024 * 1024,
		32 * 1024 * 1024,
		64 * 1024 * 1024,
	}

	for _, size := range sizes {
		size := size
		b.Run(fmt.Sprintf("BenchmarkS3MultipartV2-%s", prettySize(size)), func(b *testing.B) {
			cancel, wg := startBenchServer(b)
			defer func() {
				cancel()
				wg.Wait()
			}()

			client := createBenchClientV2(b)
			data := make([]byte, size)
			for i := range data {
				data[i] = byte(i % 251)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("bench-mpu-%d-%d", size, i)
				multipartUpload(b, client, S3_BUCKET, key, data)
			}
		})
	}
}

func prettySize(size int) string {
	switch {
	case size >= 1024*1024:
		return fmt.Sprintf("%dMB", size/(1024*1024))
	case size >= 1024:
		return fmt.Sprintf("%dKB", size/1024)
	default:
		return strconv.Itoa(size)
	}
}
