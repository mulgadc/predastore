package s3

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadConfig(t *testing.T) {
	s3 := New(&Config{ConfigPath: filepath.Join("tests", "config", "server.toml")})
	err := s3.ReadConfig()

	assert.NoError(t, err, "Should read config without error")
	assert.Equal(t, "1.0", s3.Version, "Config version should match")
	assert.Equal(t, "ap-southeast-2", s3.Region, "Region should match")
	assert.Equal(t, 5, len(s3.Buckets), "Should have 4 buckets")

	assert.Equal(t, "test-bucket01", s3.Buckets[0].Name, "Bucket name should match")
	assert.Contains(t, s3.Buckets[0].Pathname, "tests/data/test-bucket01", "Path should match")
	assert.True(t, s3.Buckets[0].Public, "Bucket should be public")

	assert.Equal(t, "private", s3.Buckets[1].Name, "Bucket name should match")
	assert.Contains(t, s3.Buckets[1].Pathname, "tests/data/private", "Path should match")
	assert.False(t, s3.Buckets[1].Public, "Bucket should be private")

	assert.Equal(t, "secure", s3.Buckets[2].Name, "Bucket name should match")
	assert.Contains(t, s3.Buckets[2].Pathname, "tests/data/secure", "Path should match")
	assert.False(t, s3.Buckets[2].Public, "Bucket should be private")

	assert.Equal(t, "local", s3.Buckets[3].Name, "Bucket name should match")
	assert.Contains(t, s3.Buckets[3].Pathname, "tests/data/local", "Path should match")
	assert.False(t, s3.Buckets[3].Public, "Bucket should be private")

	assert.Equal(t, "predastore", s3.Buckets[4].Name, "Bucket name should match")
	assert.Contains(t, s3.Buckets[4].Pathname, "tests/data/predastore", "Path should match")
	assert.False(t, s3.Buckets[4].Public, "Bucket should be private")

}

func TestBucketConfig(t *testing.T) {
	s3 := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := s3.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	bucket, err := s3.BucketConfig("test-bucket01")
	assert.NoError(t, err, "Should find bucket")
	assert.Equal(t, "test-bucket01", bucket.Name, "Bucket name should match")

	_, err = s3.BucketConfig("nonexistent")
	assert.Error(t, err, "Should return error for nonexistent bucket")
}

func TestBasePathConfig(t *testing.T) {

	tmpDir := t.TempDir()

	s3 := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
		BasePath:   tmpDir,
	})
	err := s3.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	bucket, err := s3.BucketConfig("test-bucket01")
	assert.NoError(t, err, "Should find bucket")
	assert.Equal(t, "test-bucket01", bucket.Name, "Bucket name should match")

	_, err = s3.BucketConfig("nonexistent")
	assert.Error(t, err, "Should return error for nonexistent bucket")

	assert.Contains(t, bucket.Pathname, tmpDir, "Path should contain base path")

}

func TestReadInvalidConfig(t *testing.T) {
	s3 := New(&Config{ConfigPath: filepath.Join("tests", "config", "invalid.toml")})
	err := s3.ReadConfig()

	assert.NoError(t, err, "Should read config without error")
	assert.Equal(t, "1.0", s3.Version, "Config version should match")
	assert.Equal(t, "ap-southeast-2", s3.Region, "Region should match")

	// Test no buckets, config file all invalid
	assert.Zero(t, len(s3.Buckets))

}
