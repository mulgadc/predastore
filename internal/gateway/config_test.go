package gateway

import (
	"path/filepath"
	"testing"

	"github.com/mulgadc/predastore/internal/topology"
	"github.com/stretchr/testify/assert"
)

func TestReadConfig(t *testing.T) {
	s3 := New(&Config{ConfigPath: filepath.Join("..", "..", "config", "7node.toml")})
	err := s3.ReadConfig()

	assert.NoError(t, err, "Should read config without error")
	assert.Equal(t, "1.0", s3.Version, "Config version should match")
	assert.Equal(t, "ap-southeast-2", s3.Region, "Region should match")
	assert.Equal(t, 4, s3.RS.Data, "RS data shards should match")
	assert.Equal(t, 3, s3.RS.Parity, "RS parity shards should match")
	assert.Empty(t, s3.Buckets, "Should have no buckets")
	assert.Len(t, s3.Auth, 1, "Should have 1 auth entry")
}

func TestReadInvalidConfig(t *testing.T) {
	s3 := New(&Config{ConfigPath: filepath.Join("testdata", "invalid.toml")})
	err := s3.ReadConfig()

	assert.NoError(t, err, "Should read config without error")
	assert.Equal(t, "1.0", s3.Version, "Config version should match")
	assert.Equal(t, "ap-southeast-2", s3.Region, "Region should match")

	// All bucket names are invalid — none should survive validation
	assert.Empty(t, s3.Buckets)
}

func TestReadConfig_AuthMissingAccountIDIsHardError(t *testing.T) {
	s3 := New(&Config{ConfigPath: filepath.Join("testdata", "missing_auth_account_id.toml")})
	err := s3.ReadConfig()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing account_id")
}

func TestReadConfig_BucketMissingAccountIDIsHardError(t *testing.T) {
	s3 := New(&Config{ConfigPath: filepath.Join("testdata", "missing_bucket_account_id.toml")})
	err := s3.ReadConfig()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing account_id")
}

func TestReadConfig_ClusterTopology(t *testing.T) {
	s3 := New(&Config{ConfigPath: filepath.Join("testdata", "cluster_topology.toml")})
	err := s3.ReadConfig()

	assert.NoError(t, err, "Should read config without error")
	assert.Len(t, s3.Hosts, 3, "Should have 3 hosts")
	assert.Len(t, s3.ClusterNodes, 6, "Should have 6 cluster nodes")
	assert.Equal(t, "10.11.12.1:6660", s3.Hosts[0].PublicAddr)
	assert.Equal(t, topology.RoleShardStorage, s3.ClusterNodes[0].Role)
	assert.Equal(t, topology.RoleStateReplica, s3.ClusterNodes[1].Role)
	assert.Len(t, s3.Auth, 1, "Should have 1 auth entry")
}

func TestReadConfig_InvalidClusterTopologyIsHardError(t *testing.T) {
	s3 := New(&Config{ConfigPath: filepath.Join("testdata", "invalid_cluster.toml")})
	err := s3.ReadConfig()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown host")
}
