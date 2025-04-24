package s3

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsValidKeyName(t *testing.T) {

	tests := []struct {
		name string
		key  []byte
		want error
	}{
		{
			name: "Valid ASCII",
			key:  []byte("Hello, World!"),
			want: nil,
		},
		{
			name: "Valid UTF-8",
			key:  []byte("Hello, 世界!"),
			want: nil,
		},
		{
			name: "Invalid sequence",
			key:  []byte{0xC0, 0x80},
			want: errors.New("key must be a valid UTF-8 string"),
		},
		{
			name: "Truncated sequence",
			key:  []byte{0xE2, 0x82},
			want: errors.New("key must be a valid UTF-8 string"),
		},
		{
			name: "Invalid continuation byte",
			key:  []byte{0xE2, 0x28, 0xA1},
			want: errors.New("key must be a valid UTF-8 string"),
		},
		{
			name: "Mixed valid and invalid",
			key:  []byte{'a', 0xFF, 'b'},
			want: errors.New("key must be a valid UTF-8 string"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := isValidKeyName(string(test.key))
			assert.Equal(t, test.want, err)
		})
	}
}
func TestIsValidBucketName(t *testing.T) {

	tests := []struct {
		name   string
		bucket string
		want   error
	}{
		{
			name:   "Valid bucket name",
			bucket: "my-bucket",
			want:   nil,
		},

		{
			name:   "Valid bucket name with alpha characters",
			bucket: "my-bucket-with-alpha-0123456789.bucket-name-with-hyphens",
			want:   nil,
		},

		{
			name:   "Valid bucket name with multiple periods",
			bucket: "my-bucket-with-multiple.periods.here.and.there",
			want:   nil,
		},

		{
			name:   "Invalud bucket name emoji",
			bucket: "my-bucket-👩‍💻-♗",
			want:   errors.New("bucket name must consist of lowercase letters, numbers, periods (.), and hyphens (-)"),
		},

		{
			name:   "Valid bucket name",
			bucket: "my-bucket-WITH-UPPERCASE-LETTERS",
			want:   errors.New("bucket name must consist of lowercase letters, numbers, periods (.), and hyphens (-)"),
		},

		// Test < 3 characters fails
		{
			name:   "Bucket name too short",
			bucket: "ab",
			want:   errors.New("bucket must be > 3 characters"),
		},

		// Test > 63 characters fails
		{
			name:   "Bucket name too long",
			bucket: "my-bucket-that-is-longer-than-63-characters-and-should-fail-validation-and-be-rejected",
			want:   errors.New("bucket must be < 63 characters"),
		},

		// Test invalid characters fails
		{
			name:   "Invalid characters",
			bucket: "my-bucket*",
			want:   errors.New("bucket name must consist of lowercase letters, numbers, periods (.), and hyphens (-)"),
		},

		// Test .. fails
		{
			name:   "Invalid characters",
			bucket: "my..bucket",
			want:   errors.New("bucket names must not contain two adjacent periods"),
		},

		// Test IP address fails
		{
			name:   "IP address",
			bucket: "192.168.1.1",
			want:   errors.New("bucket names must not be formatted as an IP address"),
		},

		// Test xn-- fails
		{
			name:   "xn--",
			bucket: "xn--my-bucket",
			want:   errors.New("bucket names must not start with the prefix xn--"),
		},

		// Test sthree- fails
		{
			name:   "sthree-",
			bucket: "sthree-my-bucket",
			want:   errors.New("bucket names must not start with the prefix sthree-"),
		},

		// Test amzn-s3-demo- fails
		{
			name:   "amzn-s3-demo-",
			bucket: "amzn-s3-demo-my-bucket",
			want:   errors.New("bucket names must not start with the prefix amzn-s3-demo-"),
		},

		// Test -s3alias fails
		{
			name:   "-s3alias",
			bucket: "my-bucket-s3alias",
			want:   errors.New("bucket names must not end with the suffix -s3alias"),
		},

		// Test --ol-s3 fails
		{
			name:   "--ol-s3",
			bucket: "my-bucket--ol-s3",
			want:   errors.New("bucket names must not end with the suffix --ol-s3"),
		},

		// Test .mrap fails
		{
			name:   ".mrap",
			bucket: "my-bucket.mrap",
			want:   errors.New("bucket names must not end with the suffix .mrap"),
		},

		// Test --x-s3 fails
		{
			name:   "--x-s3",
			bucket: "my-bucket--x-s3",
			want:   errors.New("bucket names must not end with the suffix --x-s3"),
		},

		// Test --table-s3 fails
		{
			name:   "--table-s3",
			bucket: "my-bucket--table-s3",
			want:   errors.New("bucket names must not end with the suffix --table-s3"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := isValidBucketName(test.bucket)

			// Use assert to check if the error is as expected
			assert.Equal(t, test.want, err)

		})
	}
}
