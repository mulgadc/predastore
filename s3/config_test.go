package s3

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadConfig(t *testing.T) {
	s3 := New()
	err := s3.ReadConfig("../tests/config/server.toml")

	assert.NoError(t, err, "Should read config without error")
	assert.Equal(t, "1.0", s3.Version, "Config version should match")
	assert.Equal(t, "ap-southeast-2", s3.Region, "Region should match")
	assert.Equal(t, 1, len(s3.Buckets), "Should have one bucket")
	assert.Equal(t, "testbucket", s3.Buckets[0].Name, "Bucket name should match")
	assert.Equal(t, "./tests/data", s3.Buckets[0].Pathname, "Path should match")
	assert.True(t, s3.Buckets[0].Public, "Bucket should be public")
}

func TestBucketConfig(t *testing.T) {
	s3 := New()
	err := s3.ReadConfig("../tests/config/server.toml")
	assert.NoError(t, err, "Should read config without error")

	bucket, err := s3.BucketConfig("testbucket")
	assert.NoError(t, err, "Should find bucket")
	assert.Equal(t, "testbucket", bucket.Name, "Bucket name should match")

	_, err = s3.BucketConfig("nonexistent")
	assert.Error(t, err, "Should return error for nonexistent bucket")
}
