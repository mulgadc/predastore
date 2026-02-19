package filesystem

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/mulgadc/predastore/backend"
)

// HeadObject returns object metadata without the body
func (b *Backend) HeadObject(ctx context.Context, bucketName, key string) (*backend.HeadObjectResponse, error) {
	bucket, err := b.getBucket(bucketName)
	if err != nil {
		return nil, err
	}

	if err := validateKey(key); err != nil {
		return nil, err
	}

	pathname, err := b.resolvePath(bucket, key)
	if err != nil {
		return nil, err
	}

	info, err := os.Stat(pathname)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, backend.ErrNoSuchKeyError.WithResource(key)
		}
		return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	}

	// Detect content type
	contentType, err := detectContentType(pathname)
	if err != nil {
		contentType = "application/octet-stream"
	}

	// Generate ETag
	etag := generateETag(bucketName, info.Name(), info.ModTime())

	return &backend.HeadObjectResponse{
		ContentType:   contentType,
		ContentLength: info.Size(),
		ETag:          etag,
		LastModified:  info.ModTime(),
	}, nil
}

// GetObject retrieves an object with optional range support
func (b *Backend) GetObject(ctx context.Context, req *backend.GetObjectRequest) (*backend.GetObjectResponse, error) {
	bucket, err := b.getBucket(req.Bucket)
	if err != nil {
		return nil, err
	}

	if err := validateKey(req.Key); err != nil {
		return nil, err
	}

	pathname, err := b.resolvePath(bucket, req.Key)
	if err != nil {
		return nil, err
	}

	info, err := os.Stat(pathname)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, backend.ErrNoSuchKeyError.WithResource(req.Key)
		}
		slog.Error("Error stating file", "path", pathname, "error", err)
		return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	}

	file, err := os.Open(pathname)
	if err != nil {
		slog.Error("Error opening file", "path", pathname, "error", err)
		return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	}

	contentType, err := detectContentType(pathname)
	if err != nil {
		contentType = "application/octet-stream"
	}

	etag := generateETag(req.Bucket, info.Name(), info.ModTime())

	// Check if range request
	if req.RangeStart >= 0 || req.RangeEnd >= 0 {
		return b.handleRangeRequest(file, info, req, contentType, etag)
	}

	// Full object response
	return &backend.GetObjectResponse{
		Body:         file,
		ContentType:  contentType,
		Size:         info.Size(),
		ETag:         etag,
		LastModified: info.ModTime(),
		StatusCode:   200,
	}, nil
}

// handleRangeRequest handles byte-range requests
func (b *Backend) handleRangeRequest(file *os.File, info os.FileInfo, req *backend.GetObjectRequest, contentType, etag string) (*backend.GetObjectResponse, error) {
	fileSize := info.Size()
	start := req.RangeStart
	end := req.RangeEnd

	// Handle default values
	if start < 0 {
		start = 0
	}
	if end < 0 || end >= fileSize {
		end = fileSize - 1
	}

	// Validate range
	if start > end || start >= fileSize {
		file.Close()
		return nil, backend.ErrInvalidRangeError
	}

	// Seek to start position
	if _, err := file.Seek(start, io.SeekStart); err != nil {
		file.Close()
		return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	}

	length := end - start + 1
	contentRange := fmt.Sprintf("bytes %d-%d/%d", start, end, fileSize)

	// Create a limited reader for the range
	limitedReader := &limitedReadCloser{
		reader: io.LimitReader(file, length),
		closer: file,
	}

	return &backend.GetObjectResponse{
		Body:         limitedReader,
		ContentType:  contentType,
		ContentRange: contentRange,
		Size:         length,
		ETag:         etag,
		LastModified: info.ModTime(),
		StatusCode:   206, // Partial Content
	}, nil
}

// limitedReadCloser wraps a LimitReader with a Closer
type limitedReadCloser struct {
	reader io.Reader
	closer io.Closer
}

func (l *limitedReadCloser) Read(p []byte) (n int, err error) {
	return l.reader.Read(p)
}

func (l *limitedReadCloser) Close() error {
	return l.closer.Close()
}

// detectContentType reads the first 512 bytes to detect content type
func detectContentType(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	buffer := make([]byte, 512)
	n, err := file.Read(buffer)
	if err != nil && err != io.EOF {
		return "", err
	}

	return http.DetectContentType(buffer[:n]), nil
}

// generateETag creates an ETag for an object
func generateETag(bucket, name string, modTime time.Time) string {
	hash := md5.New()
	hash.Write(fmt.Appendf(nil, "%s/%s:(%s)", bucket, name, modTime.String()))
	return hex.EncodeToString(hash.Sum(nil))
}
