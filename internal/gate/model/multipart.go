package model

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash"
	"strings"
	"time"
)

// S3 API Limits for multipart uploads
// Reference: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
const (
	// MinPartSize is the minimum size for any part except the last (5MB).
	MinPartSize int64 = 5 * 1024 * 1024

	// MaxPartSize is the maximum size for any single part (5GB).
	MaxPartSize int64 = 5 * 1024 * 1024 * 1024

	// MaxPartsCount is the maximum number of parts in a multipart upload (10,000).
	MaxPartsCount = 10000

	// MinPartNumber is the minimum valid part number.
	MinPartNumber = 1

	// MaxPartNumber is the maximum valid part number.
	MaxPartNumber = 10000

	// MaxObjectSize is the maximum size for the final object (5TB).
	MaxObjectSize int64 = 5 * 1024 * 1024 * 1024 * 1024
)

// UploadMetadata contains metadata about an active multipart upload.
type UploadMetadata struct {
	UploadID    string         `json:"upload_id"`
	Bucket      string         `json:"bucket"`
	Key         string         `json:"key"`
	ContentType string         `json:"content_type,omitempty"`
	CreatedAt   time.Time      `json:"created_at"`
	Parts       []PartMetadata `json:"parts,omitempty"`
}

// PartMetadata contains metadata about a single uploaded part.
type PartMetadata struct {
	PartNumber   int       `json:"part_number"`
	Size         int64     `json:"size"`
	ETag         string    `json:"etag"`
	LastModified time.Time `json:"last_modified"`
}

// ValidatePartNumber validates that a part number is within S3 API limits.
func ValidatePartNumber(partNumber int) error {
	if partNumber < MinPartNumber || partNumber > MaxPartNumber {
		return NewS3Error(
			ErrInvalidPart,
			fmt.Sprintf("Part number must be between %d and %d, got %d", MinPartNumber, MaxPartNumber, partNumber),
			400,
		)
	}
	return nil
}

// validatePartSize validates part size for non-last parts.
func validatePartSize(size int64, isLastPart bool) error {
	if size > MaxPartSize {
		return NewS3Error(
			ErrEntityTooLarge,
			fmt.Sprintf("Part size %d exceeds maximum %d bytes", size, MaxPartSize),
			400,
		)
	}
	if !isLastPart && size < MinPartSize {
		return NewS3Error(
			ErrEntityTooSmall,
			fmt.Sprintf("Part size %d is below minimum %d bytes for non-last parts", size, MinPartSize),
			400,
		)
	}
	return nil
}

// validatePartsCount validates the number of parts.
func validatePartsCount(count int) error {
	if count < 1 {
		return NewS3Error(
			ErrInvalidPart,
			"At least one part is required",
			400,
		)
	}
	if count > MaxPartsCount {
		return NewS3Error(
			ErrInvalidPart,
			fmt.Sprintf("Number of parts %d exceeds maximum %d", count, MaxPartsCount),
			400,
		)
	}
	return nil
}

// ValidatePartsForCompletion validates parts array for CompleteMultipartUpload
// Parts must be in ascending order by part number and all referenced parts must exist.
func ValidatePartsForCompletion(requestedParts []CompletedPart, storedParts []PartMetadata) error {
	if err := validatePartsCount(len(requestedParts)); err != nil {
		return err
	}

	// Create a map of stored parts for quick lookup
	storedMap := make(map[int]PartMetadata, len(storedParts))
	for _, p := range storedParts {
		storedMap[p.PartNumber] = p
	}

	// Validate ordering and existence
	var prevPartNumber int
	var totalSize int64

	for i, part := range requestedParts {
		if err := ValidatePartNumber(part.PartNumber); err != nil {
			return err
		}

		// Parts must be in ascending order
		if i > 0 && part.PartNumber <= prevPartNumber {
			return NewS3Error(
				ErrInvalidPart,
				fmt.Sprintf("Parts must be in ascending order: part %d follows part %d", part.PartNumber, prevPartNumber),
				400,
			)
		}
		prevPartNumber = part.PartNumber

		// Part must exist
		stored, exists := storedMap[part.PartNumber]
		if !exists {
			return NewS3Error(
				ErrInvalidPart,
				fmt.Sprintf("Part %d does not exist", part.PartNumber),
				400,
			)
		}

		// Validate part size (all except last must be >= MinPartSize)
		isLastPart := i == len(requestedParts)-1
		if err := validatePartSize(stored.Size, isLastPart); err != nil {
			return NewS3Error(
				ErrEntityTooSmall,
				fmt.Sprintf("Part %d is too small (%d bytes)", part.PartNumber, stored.Size),
				400,
			)
		}

		totalSize += stored.Size
	}

	// Validate total size
	if totalSize > MaxObjectSize {
		return NewS3Error(
			ErrEntityTooLarge,
			fmt.Sprintf("Total object size %d exceeds maximum %d bytes", totalSize, MaxObjectSize),
			400,
		)
	}

	return nil
}

// CalculatePartETag calculates the MD5-based ETag for a part
// Returns ETag in S3 format: "md5hex".
func CalculatePartETag(data []byte) string {
	hash := md5.Sum(data)
	return fmt.Sprintf("\"%x\"", hash)
}

// NewPartETagHasher returns the hash a part ETag is computed over. A caller
// that already reads the part end to end tees into this rather than buffering
// the part a second time to hash it.
func NewPartETagHasher() hash.Hash {
	return md5.New()
}

// PartETagFrom formats a finished part hash as an S3 ETag.
func PartETagFrom(h hash.Hash) string {
	return fmt.Sprintf("\"%x\"", h.Sum(nil))
}

// CalculateMultipartDigest computes the raw composite digest
// CalculateMultipartETag formats: md5(concat(md5(part1), md5(part2), ...)).
// Split out so a caller storing the digest in a placement record does not
// have to parse it back out of the "-N" string the ETag renders as.
func CalculateMultipartDigest(partETags []string) [16]byte {
	// Concatenate all part MD5s
	concat := make([]byte, 0, len(partETags)*16)

	for _, etag := range partETags {
		// Remove quotes and any suffix (e.g., "-1" from nested multipart)
		cleanETag := strings.Trim(etag, "\"")
		cleanETag = strings.Split(cleanETag, "-")[0]
		md5Bytes, err := hex.DecodeString(cleanETag)
		if err != nil {
			// If we can't decode, skip this part (shouldn't happen with valid ETags)
			continue
		}
		concat = append(concat, md5Bytes...)
	}

	return md5.Sum(concat)
}

// CalculateMultipartETag calculates the ETag for a completed multipart upload
// Format: "md5(concat(md5(part1), md5(part2), ...))-partCount".
func CalculateMultipartETag(partETags []string, numParts int) string {
	finalMD5 := CalculateMultipartDigest(partETags)
	return fmt.Sprintf("\"%x-%d\"", finalMD5, numParts)
}

// NormalizeETag removes quotes and normalizes an ETag for comparison.
func NormalizeETag(etag string) string {
	return strings.Trim(etag, "\"")
}

// CompareETags compares two ETags (case-insensitive, quote-insensitive).
func CompareETags(etag1, etag2 string) bool {
	return strings.EqualFold(NormalizeETag(etag1), NormalizeETag(etag2))
}
