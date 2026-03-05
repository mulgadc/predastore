package backend

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsValidBucketName(t *testing.T) {
	tests := []struct {
		name    string
		bucket  string
		wantErr string
	}{
		// Valid names
		{"valid simple", "my-bucket", ""},
		{"valid with dots", "my.bucket.name", ""},
		{"valid with numbers", "bucket123", ""},
		{"valid minimum length", "abc", ""},
		{"valid 63 chars", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ""},

		// Length violations
		{"too short empty", "", "> 3 characters"},
		{"too short 1 char", "a", "> 3 characters"},
		{"too short 2 chars", "ab", "> 3 characters"},
		{"too long 64 chars", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "< 63 characters"},

		// Character rules
		{"uppercase letters", "MyBucket", "lowercase letters"},
		{"underscore", "my_bucket", "lowercase letters"},
		{"starts with hyphen", "-bucket", "lowercase letters"},
		{"ends with hyphen", "bucket-", "lowercase letters"},
		{"starts with dot", ".bucket", "lowercase letters"},
		{"ends with dot", "bucket.", "lowercase letters"},
		{"special chars", "my@bucket", "lowercase letters"},

		// Adjacent periods
		{"adjacent periods", "my..bucket", "two adjacent periods"},

		// IP address format
		{"ip address", "192.168.5.4", "IP address"},

		// Forbidden prefixes
		{"xn-- prefix", "xn--bucket", "xn--"},
		{"sthree- prefix", "sthree-bucket", "sthree-"},
		{"amzn-s3-demo- prefix", "amzn-s3-demo-bucket", "amzn-s3-demo-"},

		// Forbidden suffixes
		{"-s3alias suffix", "bucket-s3alias", "-s3alias"},
		{"--ol-s3 suffix", "bucket--ol-s3", "--ol-s3"},
		{".mrap suffix", "bucket.mrap", ".mrap"},
		{"--x-s3 suffix", "bucket--x-s3", "--x-s3"},
		{"--table-s3 suffix", "bucket--table-s3", "--table-s3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := IsValidBucketName(tt.bucket)
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}
