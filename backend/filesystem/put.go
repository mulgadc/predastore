package filesystem

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/s3/chunked"
)

// PutObject stores an object in the filesystem
func (b *Backend) PutObject(ctx context.Context, req *backend.PutObjectRequest) (*backend.PutObjectResponse, error) {
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

	// Ensure the directory exists
	if err := os.MkdirAll(filepath.Dir(pathname), 0755); err != nil {
		slog.Error("Error creating directories", "path", filepath.Dir(pathname), "error", err)
		return nil, backend.NewS3Error(backend.ErrInternalError, "Failed to create directory", 500)
	}

	// Create the file
	file, err := os.Create(pathname)
	if err != nil {
		slog.Error("Error creating file", "path", pathname, "error", err)
		return nil, backend.NewS3Error(backend.ErrInternalError, "Failed to create file", 500)
	}
	defer file.Close()

	// Setup reader - handle chunked encoding if needed
	reader := req.Body
	if req.IsChunked && req.ContentEncoding == "aws-chunked" {
		slog.Debug("Using chunked decoder", "decodedLength", req.DecodedLength)
		reader = chunked.NewDecoder(req.Body, req.DecodedLength)
	}

	// Calculate MD5 while writing
	md5Hash := md5.New()
	multiWriter := io.MultiWriter(file, md5Hash)

	// Stream the content to disk
	written, err := io.Copy(multiWriter, reader)
	if err != nil {
		slog.Error("Error writing file", "path", pathname, "error", err)
		// Clean up partial file
		os.Remove(pathname)
		return nil, backend.NewS3Error(backend.ErrInternalError, "Failed to write file", 500)
	}

	slog.Info("Object stored", "bucket", req.Bucket, "key", req.Key, "size", written)

	return &backend.PutObjectResponse{
		ETag: hex.EncodeToString(md5Hash.Sum(nil)),
	}, nil
}
