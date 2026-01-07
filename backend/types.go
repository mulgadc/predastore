package backend

import (
	"io"
	"time"
)

// ObjectInfo contains metadata about an object
type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
	ETag         string
	ContentType  string
	StorageClass string
	IsDir        bool
}

// BucketInfo contains metadata about a bucket
type BucketInfo struct {
	Name         string
	CreationDate time.Time
	Region       string
}

// GetObjectRequest contains parameters for GetObject operation
type GetObjectRequest struct {
	Bucket     string
	Key        string
	RangeStart int64 // -1 means not specified
	RangeEnd   int64 // -1 means not specified
}

// GetObjectResponse contains the result of GetObject operation
type GetObjectResponse struct {
	Body         io.ReadCloser
	ContentType  string
	ContentRange string
	Size         int64
	ETag         string
	LastModified time.Time
	StatusCode   int // 200 for full content, 206 for partial
}

// PutObjectRequest contains parameters for PutObject operation
type PutObjectRequest struct {
	Bucket          string
	Key             string
	Body            io.Reader
	ContentLength   int64
	ContentType     string
	ContentEncoding string
	IsChunked       bool
	DecodedLength   int64 // For aws-chunked encoding
}

// PutObjectResponse contains the result of PutObject operation
type PutObjectResponse struct {
	ETag string
}

// DeleteObjectRequest contains parameters for DeleteObject operation
type DeleteObjectRequest struct {
	Bucket string
	Key    string
}

// ListObjectsRequest contains parameters for ListObjectsV2 operation
type ListObjectsRequest struct {
	Bucket       string
	Prefix       string
	Delimiter    string
	MaxKeys      int
	StartAfter   string
	EncodingType string
}

// ListObjectsResponse contains the result of ListObjectsV2 operation
type ListObjectsResponse struct {
	Name           string
	Prefix         string
	Delimiter      string
	MaxKeys        int
	KeyCount       int
	IsTruncated    bool
	Contents       []ObjectInfo
	CommonPrefixes []string
}

// ListBucketsResponse contains the result of ListBuckets operation
type ListBucketsResponse struct {
	Buckets []BucketInfo
	Owner   OwnerInfo
}

// OwnerInfo contains owner information
type OwnerInfo struct {
	ID          string
	DisplayName string
}

// CreateMultipartUploadRequest contains parameters for InitiateMultipartUpload
type CreateMultipartUploadRequest struct {
	Bucket      string
	Key         string
	ContentType string
}

// CreateMultipartUploadResponse contains the result of InitiateMultipartUpload
type CreateMultipartUploadResponse struct {
	Bucket   string
	Key      string
	UploadID string
}

// UploadPartRequest contains parameters for UploadPart operation
type UploadPartRequest struct {
	Bucket          string
	Key             string
	UploadID        string
	PartNumber      int
	Body            io.Reader
	ContentLength   int64
	ContentEncoding string
	IsChunked       bool
	DecodedLength   int64
}

// UploadPartResponse contains the result of UploadPart operation
type UploadPartResponse struct {
	ETag       string
	PartNumber int
}

// CompletedPart represents a completed part in multipart upload
type CompletedPart struct {
	PartNumber int
	ETag       string
}

// CompleteMultipartUploadRequest contains parameters for CompleteMultipartUpload
type CompleteMultipartUploadRequest struct {
	Bucket   string
	Key      string
	UploadID string
	Parts    []CompletedPart
}

// CompleteMultipartUploadResponse contains the result of CompleteMultipartUpload
type CompleteMultipartUploadResponse struct {
	Location string
	Bucket   string
	Key      string
	ETag     string
}

// HeadObjectResponse contains the result of HeadObject operation
type HeadObjectResponse struct {
	ContentType  string
	ContentLength int64
	ETag         string
	LastModified time.Time
}
