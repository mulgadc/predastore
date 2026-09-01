package model

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidatePartNumber(t *testing.T) {
	tests := []struct {
		name       string
		partNumber int
		wantErr    bool
	}{
		{"valid min", 1, false},
		{"valid mid", 5000, false},
		{"valid max", 10000, false},
		{"invalid zero", 0, true},
		{"invalid negative", -1, true},
		{"invalid too large", 10001, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePartNumber(tt.partNumber)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidatePartSize(t *testing.T) {
	tests := []struct {
		name       string
		size       int64
		isLastPart bool
		wantErr    bool
	}{
		{"valid 5MB", MinPartSize, false, false},
		{"valid 100MB", 100 * 1024 * 1024, false, false},
		{"valid 5GB", MaxPartSize, false, false},
		{"invalid too small non-last", MinPartSize - 1, false, true},
		{"invalid too large", MaxPartSize + 1, false, true},
		{"valid small last part", 1024, true, false},
		{"valid 1 byte last part", 1, true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePartSize(tt.size, tt.isLastPart)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidatePartsCount(t *testing.T) {
	tests := []struct {
		name    string
		count   int
		wantErr bool
	}{
		{"valid one", 1, false},
		{"valid many", 1000, false},
		{"valid max", 10000, false},
		{"invalid zero", 0, true},
		{"invalid too many", 10001, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePartsCount(tt.count)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidatePartsForCompletion(t *testing.T) {
	// Helper to create stored parts
	createStoredParts := func(partNumbers []int, sizes []int64) []PartMetadata {
		parts := make([]PartMetadata, len(partNumbers))
		for i, num := range partNumbers {
			parts[i] = PartMetadata{
				PartNumber: num,
				Size:       sizes[i],
				ETag:       fmt.Sprintf("etag-%d", num),
			}
		}
		return parts
	}

	// Helper to create requested parts
	createRequestedParts := func(partNumbers []int) []CompletedPart {
		parts := make([]CompletedPart, len(partNumbers))
		for i, num := range partNumbers {
			parts[i] = CompletedPart{
				PartNumber: num,
				ETag:       fmt.Sprintf("etag-%d", num),
			}
		}
		return parts
	}

	tests := []struct {
		name           string
		requestedParts []int
		storedParts    []int
		storedSizes    []int64
		wantErr        bool
		errContains    string
	}{
		{
			name:           "valid two parts",
			requestedParts: []int{1, 2},
			storedParts:    []int{1, 2},
			storedSizes:    []int64{MinPartSize, 1024}, // 5MB + 1KB
			wantErr:        false,
		},
		{
			name:           "valid three parts",
			requestedParts: []int{1, 2, 3},
			storedParts:    []int{1, 2, 3},
			storedSizes:    []int64{MinPartSize, MinPartSize, 1024},
			wantErr:        false,
		},
		{
			name:           "valid non-sequential parts",
			requestedParts: []int{1, 5, 10},
			storedParts:    []int{1, 5, 10},
			storedSizes:    []int64{MinPartSize, MinPartSize, 1024},
			wantErr:        false,
		},
		{
			name:           "invalid part not in order",
			requestedParts: []int{2, 1},
			storedParts:    []int{1, 2},
			storedSizes:    []int64{MinPartSize, MinPartSize},
			wantErr:        true,
			errContains:    "ascending order",
		},
		{
			name:           "invalid duplicate part",
			requestedParts: []int{1, 1},
			storedParts:    []int{1, 2},
			storedSizes:    []int64{MinPartSize, MinPartSize},
			wantErr:        true,
			errContains:    "ascending order",
		},
		{
			name:           "invalid missing part",
			requestedParts: []int{1, 3},
			storedParts:    []int{1, 2},
			storedSizes:    []int64{MinPartSize, MinPartSize},
			wantErr:        true,
			errContains:    "does not exist",
		},
		{
			name:           "invalid non-last part too small",
			requestedParts: []int{1, 2},
			storedParts:    []int{1, 2},
			storedSizes:    []int64{1024, MinPartSize}, // First part too small
			wantErr:        true,
			errContains:    "too small",
		},
		{
			name:           "invalid part number too large",
			requestedParts: []int{1, 10001},
			storedParts:    []int{1, 10001},
			storedSizes:    []int64{MinPartSize, MinPartSize},
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requested := createRequestedParts(tt.requestedParts)
			stored := createStoredParts(tt.storedParts, tt.storedSizes)

			err := ValidatePartsForCompletion(requested, stored)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCalculatePartETag(t *testing.T) {
	data := []byte("hello world")
	expected := md5.Sum(data)
	expectedETag := fmt.Sprintf("\"%x\"", expected)

	etag := CalculatePartETag(data)
	assert.Equal(t, expectedETag, etag)
}

// The streaming hasher has to agree with the buffered one, because the two
// ETags are compared against each other when an upload completes.
func TestPartETagFromMatchesCalculatePartETag(t *testing.T) {
	data := []byte("test data for etag calculation")
	expectedETag := fmt.Sprintf("\"%x\"", md5.Sum(data))

	h := NewPartETagHasher()
	n, err := io.Copy(h, bytes.NewReader(data))
	require.NoError(t, err)
	assert.Equal(t, int64(len(data)), n)

	assert.Equal(t, expectedETag, PartETagFrom(h))
	assert.Equal(t, CalculatePartETag(data), PartETagFrom(h))
}

func TestMultipartETag(t *testing.T) {
	// Test with known part ETags
	part1Data := []byte("part 1 data")
	part2Data := []byte("part 2 data")
	part1MD5 := md5.Sum(part1Data)
	part2MD5 := md5.Sum(part2Data)

	composite := NewMultipartETag(2)
	require.NoError(t, composite.AddPart(fmt.Sprintf("\"%x\"", part1MD5)))
	require.NoError(t, composite.AddPart(fmt.Sprintf("\"%x\"", part2MD5)))

	result := composite.String()

	// Verify format: "md5-partcount"
	assert.Contains(t, result, "-2\"")
	assert.Equal(t, 2, composite.PartCount())

	// Calculate expected manually
	concat := append(part1MD5[:], part2MD5[:]...)
	expectedMD5 := md5.Sum(concat)
	assert.Equal(t, fmt.Sprintf("\"%x-%d\"", expectedMD5, 2), result)
	assert.Equal(t, expectedMD5, composite.Digest())
}

func TestMultipartETagAcceptsEveryPartETagForm(t *testing.T) {
	// ETags might come with or without quotes, and a nested multipart part
	// carries its own "-N" suffix.
	composite := NewMultipartETag(3)
	for _, etag := range []string{
		"\"d41d8cd98f00b204e9800998ecf8427e\"",
		"d41d8cd98f00b204e9800998ecf8427e",
		"\"d41d8cd98f00b204e9800998ecf8427e-1\"",
	} {
		require.NoError(t, composite.AddPart(etag))
	}

	assert.Contains(t, composite.String(), "-3\"")
}

// A part ETag that is not a hex MD5 cannot contribute to the composite, and
// skipping it would compose a digest that is wrong without saying so.
func TestMultipartETagRejectsAnUnusablePartETag(t *testing.T) {
	composite := NewMultipartETag(1)

	assert.Error(t, composite.AddPart("not-an-etag"))
	assert.Error(t, composite.AddPart("\"abc123\""))
	assert.Equal(t, 0, composite.PartCount())
}

func TestNormalizeETag(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"\"abc123\"", "abc123"},
		{"abc123", "abc123"},
		{"\"\"", ""},
		{"", ""},
	}

	for _, tt := range tests {
		result := NormalizeETag(tt.input)
		assert.Equal(t, tt.expected, result)
	}
}

func TestCompareETags(t *testing.T) {
	tests := []struct {
		etag1    string
		etag2    string
		expected bool
	}{
		{"\"abc123\"", "\"abc123\"", true},
		{"\"abc123\"", "abc123", true},
		{"abc123", "\"abc123\"", true},
		{"ABC123", "abc123", true},
		{"\"ABC123\"", "\"abc123\"", true},
		{"abc123", "def456", false},
		{"", "", true},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s_vs_%s", tt.etag1, tt.etag2), func(t *testing.T) {
			result := CompareETags(tt.etag1, tt.etag2)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestS3APILimits(t *testing.T) {
	// Verify the constants match S3 API documentation
	assert.Equal(t, MinPartSize, int64(5*1024*1024), "Min part size should be 5MB")
	assert.Equal(t, MaxPartSize, int64(5*1024*1024*1024), "Max part size should be 5GB")
	assert.Equal(t, 10000, MaxPartsCount, "Max parts should be 10000")
	assert.Equal(t, 1, MinPartNumber, "Min part number should be 1")
	assert.Equal(t, 10000, MaxPartNumber, "Max part number should be 10000")
	assert.Equal(t, MaxObjectSize, int64(5*1024*1024*1024*1024), "Max object size should be 5TB")
}

func TestMultipartETagFormat(t *testing.T) {
	// Verify the multipart ETag format matches AWS S3
	// Format: "<md5-of-concatenated-md5s>-<part-count>"

	// Create deterministic test data
	part1MD5 := make([]byte, 16)
	part2MD5 := make([]byte, 16)
	for i := range 16 {
		part1MD5[i] = byte(i)
		part2MD5[i] = byte(i + 16)
	}

	composite := NewMultipartETag(2)
	require.NoError(t, composite.AddPart(fmt.Sprintf("\"%s\"", hex.EncodeToString(part1MD5))))
	require.NoError(t, composite.AddPart(fmt.Sprintf("\"%s\"", hex.EncodeToString(part2MD5))))

	// Should match format: "hex-md5-of-concatenated-md5s-partcount"
	assert.Regexp(t, `^"[a-f0-9]{32}-2"$`, composite.String())
}
