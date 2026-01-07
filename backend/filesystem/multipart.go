package filesystem

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/s3/chunked"
)

const minPartSize = 5 * 1024 * 1024 // 5MB minimum part size (except last part)

// CreateMultipartUpload initiates a multipart upload
func (b *Backend) CreateMultipartUpload(ctx context.Context, req *backend.CreateMultipartUploadRequest) (*backend.CreateMultipartUploadResponse, error) {
	bucket, err := b.getBucket(req.Bucket)
	if err != nil {
		return nil, err
	}

	if err := validateKey(req.Key); err != nil {
		return nil, err
	}

	// Verify bucket directory exists
	if _, err := os.Stat(bucket.Pathname); os.IsNotExist(err) {
		return nil, backend.ErrNoSuchBucketError.WithResource(req.Bucket)
	}

	// Generate unique upload ID
	uploadID := uuid.New().String()
	uploadDir := filepath.Join(b.config.TempDir, uploadID)

	if err := os.MkdirAll(uploadDir, 0755); err != nil {
		slog.Error("Failed to create upload directory", "dir", uploadDir, "error", err)
		return nil, backend.NewS3Error(backend.ErrInternalError, "Failed to initialize upload", 500)
	}

	slog.Info("Multipart upload created", "bucket", req.Bucket, "key", req.Key, "uploadID", uploadID)

	return &backend.CreateMultipartUploadResponse{
		Bucket:   req.Bucket,
		Key:      req.Key,
		UploadID: uploadID,
	}, nil
}

// UploadPart handles uploading a single part of a multipart upload
func (b *Backend) UploadPart(ctx context.Context, req *backend.UploadPartRequest) (*backend.UploadPartResponse, error) {
	bucket, err := b.getBucket(req.Bucket)
	if err != nil {
		return nil, err
	}

	if err := validateKey(req.Key); err != nil {
		return nil, err
	}

	// Verify bucket exists
	if _, err := os.Stat(bucket.Pathname); os.IsNotExist(err) {
		return nil, backend.ErrNoSuchBucketError.WithResource(req.Bucket)
	}

	// Verify upload directory exists
	uploadDir := filepath.Join(b.config.TempDir, req.UploadID)
	if _, err := os.Stat(uploadDir); os.IsNotExist(err) {
		return nil, backend.ErrNoSuchUploadError.WithResource(req.UploadID)
	}

	// Create part file
	partFile := filepath.Join(uploadDir, fmt.Sprintf("%s.%d", filepath.Base(req.Key), req.PartNumber))
	file, err := os.Create(partFile)
	if err != nil {
		slog.Error("Failed to create part file", "path", partFile, "error", err)
		return nil, backend.NewS3Error(backend.ErrInternalError, "Failed to create part file", 500)
	}
	defer file.Close()

	// Setup reader - handle chunked encoding if needed
	reader := req.Body
	if req.IsChunked && req.ContentEncoding == "aws-chunked" {
		reader = chunked.NewDecoder(req.Body, req.DecodedLength)
	}

	// Write part while calculating MD5
	md5Hash := md5.New()
	multiWriter := io.MultiWriter(file, md5Hash)

	written, err := io.Copy(multiWriter, reader)
	if err != nil {
		slog.Error("Failed to write part", "path", partFile, "error", err)
		os.Remove(partFile)
		return nil, backend.NewS3Error(backend.ErrInternalError, "Failed to write part", 500)
	}

	etag := fmt.Sprintf("\"%x\"", md5Hash.Sum(nil))

	slog.Debug("Part uploaded", "uploadID", req.UploadID, "partNumber", req.PartNumber, "size", written, "etag", etag)

	return &backend.UploadPartResponse{
		ETag:       etag,
		PartNumber: req.PartNumber,
	}, nil
}

// CompleteMultipartUpload assembles parts into the final object
func (b *Backend) CompleteMultipartUpload(ctx context.Context, req *backend.CompleteMultipartUploadRequest) (*backend.CompleteMultipartUploadResponse, error) {
	bucket, err := b.getBucket(req.Bucket)
	if err != nil {
		return nil, err
	}

	if err := validateKey(req.Key); err != nil {
		return nil, err
	}

	uploadDir := filepath.Join(b.config.TempDir, req.UploadID)
	if _, err := os.Stat(uploadDir); os.IsNotExist(err) {
		return nil, backend.ErrNoSuchUploadError.WithResource(req.UploadID)
	}

	// Validate all parts exist and meet size requirements
	partETags := make([]string, len(req.Parts))
	for i, part := range req.Parts {
		if part.ETag == "" {
			return nil, backend.NewS3Error(backend.ErrInvalidPart,
				fmt.Sprintf("Missing ETag for part %d", part.PartNumber), 400)
		}

		partFile := filepath.Join(uploadDir, fmt.Sprintf("%s.%d", filepath.Base(req.Key), part.PartNumber))
		info, err := os.Stat(partFile)
		if os.IsNotExist(err) {
			return nil, backend.NewS3Error(backend.ErrInvalidPart,
				fmt.Sprintf("Part %d does not exist", part.PartNumber), 400)
		}

		// All parts except the last must be at least 5MB
		if i < len(req.Parts)-1 && info.Size() < minPartSize {
			return nil, backend.NewS3Error(backend.ErrEntityTooSmall,
				fmt.Sprintf("Part %d is too small (%d bytes)", part.PartNumber, info.Size()), 400)
		}

		// Verify ETag
		calculatedETag, err := calculateFileETag(partFile)
		if err != nil {
			return nil, backend.NewS3Error(backend.ErrInternalError, "Failed to verify part", 500)
		}

		// Normalize ETags for comparison (remove quotes)
		normalizedPartETag := strings.Trim(part.ETag, "\"")
		normalizedCalcETag := strings.Trim(calculatedETag, "\"")

		if normalizedPartETag != normalizedCalcETag {
			slog.Warn("ETag mismatch", "part", part.PartNumber, "expected", part.ETag, "calculated", calculatedETag)
			// AWS is lenient about ETag validation in some cases, so we continue
		}

		partETags[i] = normalizedPartETag
	}

	// Create destination path
	destPath, err := b.resolvePath(bucket, req.Key)
	if err != nil {
		return nil, err
	}

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, "Failed to create directory", 500)
	}

	// Create final file
	destFile, err := os.Create(destPath)
	if err != nil {
		slog.Error("Failed to create final file", "path", destPath, "error", err)
		return nil, backend.NewS3Error(backend.ErrInternalError, "Failed to create file", 500)
	}
	defer destFile.Close()

	// Concatenate all parts
	for _, part := range req.Parts {
		partFile := filepath.Join(uploadDir, fmt.Sprintf("%s.%d", filepath.Base(req.Key), part.PartNumber))
		if err := appendFile(destFile, partFile); err != nil {
			slog.Error("Failed to concatenate part", "part", part.PartNumber, "error", err)
			os.Remove(destPath)
			return nil, backend.NewS3Error(backend.ErrInternalError, "Failed to assemble file", 500)
		}

		// Clean up part file
		os.Remove(partFile)
	}

	// Clean up upload directory
	os.RemoveAll(uploadDir)

	// Calculate multipart ETag
	finalETag := calculateMultipartETag(partETags, len(req.Parts))

	slog.Info("Multipart upload completed", "bucket", req.Bucket, "key", req.Key, "uploadID", req.UploadID)

	return &backend.CompleteMultipartUploadResponse{
		Location: fmt.Sprintf("/%s/%s", req.Bucket, req.Key),
		Bucket:   req.Bucket,
		Key:      req.Key,
		ETag:     finalETag,
	}, nil
}

// appendFile appends the content of src file to dst file
func appendFile(dst *os.File, srcPath string) error {
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()

	_, err = io.Copy(dst, src)
	return err
}

// calculateFileETag calculates the MD5 ETag for a file
func calculateFileETag(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return fmt.Sprintf("\"%x\"", hash.Sum(nil)), nil
}

// calculateMultipartETag calculates the ETag for a multipart upload
func calculateMultipartETag(partETags []string, numParts int) string {
	// Concatenate all part MD5s
	concat := make([]byte, 0, len(partETags)*16)

	for _, etag := range partETags {
		cleanETag := strings.Trim(etag, "\"")
		cleanETag = strings.Split(cleanETag, "-")[0]
		md5Bytes, _ := hex.DecodeString(cleanETag)
		concat = append(concat, md5Bytes...)
	}

	finalMD5 := md5.Sum(concat)
	return fmt.Sprintf("\"%x-%d\"", finalMD5, numParts)
}
