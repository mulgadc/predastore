package s3

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadConfig(t *testing.T) {
	s3 := New(&Config{ConfigPath: filepath.Join("..", "clusters", "7node", "cluster.toml")})
	err := s3.ReadConfig()

	assert.NoError(t, err, "Should read config without error")
	assert.Equal(t, "1.0", s3.Version, "Config version should match")
	assert.Equal(t, "ap-southeast-2", s3.Region, "Region should match")
	assert.Equal(t, 4, s3.RS.Data, "RS data shards should match")
	assert.Equal(t, 3, s3.RS.Parity, "RS parity shards should match")
	assert.Equal(t, 0, len(s3.Buckets), "Should have no buckets")
	assert.Equal(t, 7, len(s3.Nodes), "Should have 7 storage nodes")
	assert.Equal(t, 7, len(s3.DB), "Should have 7 database nodes")
	assert.Equal(t, 1, len(s3.Auth), "Should have 1 auth entry")
}

func TestReadInvalidConfig(t *testing.T) {
	s3 := New(&Config{ConfigPath: filepath.Join("testdata", "invalid.toml")})
	err := s3.ReadConfig()

	assert.NoError(t, err, "Should read config without error")
	assert.Equal(t, "1.0", s3.Version, "Config version should match")
	assert.Equal(t, "ap-southeast-2", s3.Region, "Region should match")

	// All bucket names are invalid — none should survive validation
	assert.Zero(t, len(s3.Buckets))
}
