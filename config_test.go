package predastore

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	assert.Equal(t, "1.0", cfg.Version, "Config version should match")
	assert.Equal(t, "ap-southeast-2", cfg.Region, "Region should match")
	assert.Equal(t, 2, cfg.RS.Data, "RS data shards should match")
	assert.Equal(t, 1, cfg.RS.Parity, "RS parity shards should match")
	assert.Empty(t, cfg.Buckets, "Should have no buckets")
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

// TestClusterQueries covers what a launcher asks the configuration: which host
// it is, which nodes it runs, and where their state lives. The fixture pins
// nodes 1-2 to host 1, 3-4 to host 2 and 5-6 to host 3, odd ids being
// shard-storage and even ones state replicas.
func TestClusterQueries(t *testing.T) {
	cfg, err := LoadConfig(filepath.Join("testdata", "cluster_topology.toml"))
	require.NoError(t, err)

	host, ok := cfg.localHost(2)
	require.True(t, ok)
	assert.Equal(t, "10.11.12.2:6660", host.PublicAddr)

	_, ok = cfg.localHost(99)
	assert.False(t, ok, "an unknown host resolves to nothing")

	local := cfg.localNodes(1)
	require.Len(t, local, 2)
	assert.Equal(t, []NodeID{1, 2}, []NodeID{local[0].ID, local[1].ID})
	assert.Empty(t, cfg.localNodes(99), "an unknown host runs no nodes")

	// Raft treats an identically ordered replica set as idempotent across
	// replicas, so the sort is load-bearing rather than cosmetic.
	replicas := cfg.nodesByRole(RoleStateReplica)
	require.Len(t, replicas, 3)
	assert.Equal(t, []NodeID{2, 4, 6}, []NodeID{replicas[0].ID, replicas[1].ID, replicas[2].ID})
	assert.Equal(t, []NodeID{1, 3, 5}, cfg.storageNodeIDs())

	assert.Equal(t, filepath.Join("/var/lib/predastore", "node-3"), cfg.dataDir(3))
	assert.Empty(t, cfg.dataDir(99), "an unknown node has no data directory")
}
