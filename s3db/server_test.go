package s3db

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewServer_RejectsEmptyCredentials verifies the constructor fails closed
// when no credentials are configured. The previous behaviour silently bypassed
// the SigV4 middleware in this state, granting unauthenticated access to the
// Raft-backed IAM mutation endpoints.
func TestNewServer_RejectsEmptyCredentials(t *testing.T) {
	tmpDir := t.TempDir()

	clusterCfg := DefaultClusterConfig()
	clusterCfg.NodeID = 1
	clusterCfg.DataDir = tmpDir
	clusterCfg.Bootstrap = true
	clusterCfg.Nodes = []DBNodeConfig{
		{ID: 1, Host: "127.0.0.1", Port: 16670, RaftPort: 17670, Path: filepath.Join(tmpDir, "node-1")},
	}

	cases := []struct {
		name        string
		credentials map[string]string
	}{
		{name: "nil map", credentials: nil},
		{name: "empty map", credentials: map[string]string{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &ServerConfig{
				Addr:          "127.0.0.1:0",
				Credentials:   tc.credentials,
				ClusterConfig: clusterCfg,
			}

			server, err := NewServer(cfg)
			require.Error(t, err)
			assert.Nil(t, server)
			assert.Contains(t, err.Error(), "no credentials configured")
		})
	}
}

// TestNewServer_RejectsMissingClusterConfig keeps coverage on the existing
// nil-cluster-config guard so the credentials check above can't accidentally
// shadow it in future refactors.
func TestNewServer_RejectsMissingClusterConfig(t *testing.T) {
	cfg := &ServerConfig{
		Credentials: map[string]string{"key": "secret"},
	}

	server, err := NewServer(cfg)
	require.Error(t, err)
	assert.Nil(t, server)
	assert.Contains(t, err.Error(), "cluster config is required")
}
