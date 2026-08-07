package predastore

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	cfg, err := LoadConfig(filepath.Join("config", "7node.toml"))

	require.NoError(t, err, "Should read config without error")
	assert.Equal(t, "1.0", cfg.Version, "Config version should match")
	assert.Equal(t, "ap-southeast-2", cfg.Region, "Region should match")
	assert.Equal(t, 4, cfg.RS.Data, "RS data shards should match")
	assert.Equal(t, 3, cfg.RS.Parity, "RS parity shards should match")
	assert.Empty(t, cfg.Buckets, "Should have no buckets")
	assert.Len(t, cfg.Auth, 1, "Should have 1 auth entry")
}

func TestLoadConfig_InvalidBucketNamesAreDropped(t *testing.T) {
	cfg, err := LoadConfig(filepath.Join("testdata", "invalid.toml"))

	require.NoError(t, err, "Should read config without error")
	assert.Equal(t, "1.0", cfg.Version, "Config version should match")
	assert.Equal(t, "ap-southeast-2", cfg.Region, "Region should match")

	// All bucket names are invalid — none should survive validation
	assert.Empty(t, cfg.Buckets)
}

func TestLoadConfig_AuthMissingAccountIDIsHardError(t *testing.T) {
	_, err := LoadConfig(filepath.Join("testdata", "missing_auth_account_id.toml"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing account_id")
}

func TestLoadConfig_BucketMissingAccountIDIsHardError(t *testing.T) {
	_, err := LoadConfig(filepath.Join("testdata", "missing_bucket_account_id.toml"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing account_id")
}

func TestLoadConfig_ClusterTopology(t *testing.T) {
	cfg, err := LoadConfig(filepath.Join("testdata", "cluster_topology.toml"))

	require.NoError(t, err, "Should read config without error")
	assert.Len(t, cfg.Hosts, 3, "Should have 3 hosts")
	assert.Len(t, cfg.Nodes, 6, "Should have 6 cluster nodes")
	assert.Equal(t, "10.11.12.1:6660", cfg.Hosts[0].PublicAddr)
	assert.Equal(t, RoleShardStorage, cfg.Nodes[0].Role)
	assert.Equal(t, RoleStateReplica, cfg.Nodes[1].Role)
	assert.Len(t, cfg.Auth, 1, "Should have 1 auth entry")
}

func TestLoadConfig_InvalidClusterTopologyIsHardError(t *testing.T) {
	_, err := LoadConfig(filepath.Join("testdata", "invalid_cluster.toml"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown host")
}

// TestLoadConfig_MissingFile pins the error on the read step: an operator who
// mistypes -config must be told the path is unreadable, not handed a config of
// zero values.
func TestLoadConfig_MissingFile(t *testing.T) {
	_, err := LoadConfig(filepath.Join(t.TempDir(), "no-such.toml"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "read ")
}

// TestNodeSelection covers the two selections a launcher makes: the whole
// topology for a single-process cluster, or one host's nodes for a member of a
// distributed one.
func TestNodeSelection(t *testing.T) {
	cfg, err := LoadConfig(filepath.Join("testdata", "cluster_topology.toml"))
	require.NoError(t, err)

	assert.Equal(t, []int{1, 2, 3, 4, 5, 6}, cfg.AllNodeIDs())
	assert.Equal(t, []int{1, 2}, cfg.NodeIDsForHost(1))
	assert.Nil(t, cfg.NodeIDsForHost(99), "an unknown host owns no nodes")
}
