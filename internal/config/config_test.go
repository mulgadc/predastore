package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_InvalidBucketNamesAreDropped(t *testing.T) {
	cfg, err := Load(filepath.Join("testdata", "invalid.toml"))

	require.NoError(t, err, "Should read config without error")
	assert.Equal(t, 1, cfg.Version, "Config version should match")
	assert.Equal(t, "ap-southeast-2", cfg.Region, "Region should match")

	// All bucket names are invalid — none should survive validation
	assert.Empty(t, cfg.Buckets)
}

func TestLoad_AuthMissingAccountIDIsHardError(t *testing.T) {
	_, err := Load(filepath.Join("testdata", "missing_auth_account_id.toml"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing account_id")
}

func TestLoad_BucketMissingAccountIDIsHardError(t *testing.T) {
	_, err := Load(filepath.Join("testdata", "missing_bucket_account_id.toml"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing account_id")
}

// TestLoad_MissingFile pins the error on the read step: an operator who
// mistypes -config must be told the path is unreadable, not handed a config of
// zero values.
func TestLoad_MissingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "no-such.toml"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "read ")
}

// TestNodeBindAddr covers the resolution a multi-homed host depends on: the
// gate may name the public interface while the cluster plane stays on the
// private one, and a host that names neither binds one address for both.
func TestNodeBindAddr(t *testing.T) {
	tests := []struct {
		name     string
		host     Host
		node     Node
		expected string
	}{
		{
			name:     "gate binds its own address",
			host:     Host{Addr: "10.0.0.1", BindAddr: "10.0.0.1"},
			node:     Node{Role: RoleGate, BindAddr: "0.0.0.0"},
			expected: "0.0.0.0",
		},
		{
			name:     "gate without one follows the host bind",
			host:     Host{Addr: "10.0.0.1", BindAddr: "0.0.0.0"},
			node:     Node{Role: RoleGate},
			expected: "0.0.0.0",
		},
		{
			name:     "a host that binds nothing falls back to its dialable address",
			host:     Host{Addr: "10.0.0.1"},
			node:     Node{Role: RoleGate},
			expected: "10.0.0.1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, NodeBindAddr(tt.host, tt.node))
		})
	}
}

// TestHostBindAddr pins the fallback the cluster plane depends on. An empty
// address is a wildcard once it reaches the network stack, so a host that
// names no bind_addr must resolve to the address its peers dial rather than
// publishing raft and blob traffic to every interface.
func TestHostBindAddr(t *testing.T) {
	assert.Equal(t, "10.0.0.1", HostBindAddr(Host{Addr: "10.0.0.1"}))
	assert.Equal(t, "192.168.1.1", HostBindAddr(Host{Addr: "10.0.0.1", BindAddr: "192.168.1.1"}))
	assert.Equal(t, "0.0.0.0", HostBindAddr(Host{Addr: "10.0.0.1", BindAddr: "0.0.0.0"}), "a wildcard is still the operator's to ask for")
}

// TestValidate_NodeBindAddr pins the two ways a bind_addr under [[host.node]]
// is a mistake: on a role with no listener of its own, and carrying a port
// that belongs to the node's own field.
func TestValidate_NodeBindAddr(t *testing.T) {
	cfg := func(n Node) *Config {
		return &Config{
			Version: Version,
			Region:  "ap-southeast-2",
			RS:      RS{Data: 1, Parity: 0},
			Hosts: []Host{{
				ID:   1,
				Addr: "10.0.0.1",
				Nodes: []Node{
					{ID: 1, Role: RoleGate, Port: 8443},
					{ID: 2, Role: RoleBlob, Port: 9991, DataDir: "/var/lib/predastore/blob"},
					n,
				},
			}},
		}
	}

	t.Run("rejected on a blob node", func(t *testing.T) {
		err := cfg(Node{ID: 3, Role: RoleBlob, Port: 9992, DataDir: "/var/lib/predastore/blob2", BindAddr: "0.0.0.0"}).Validate()

		require.Error(t, err)
		assert.Contains(t, err.Error(), "must not set bind_addr")
	})

	t.Run("rejected on a meta node", func(t *testing.T) {
		err := cfg(Node{ID: 3, Role: RoleMeta, Port: 6660, DataDir: "/var/lib/predastore/meta", BindAddr: "10.0.0.1"}).Validate()

		require.Error(t, err)
		assert.Contains(t, err.Error(), "must not set bind_addr")
	})

	t.Run("rejected when it carries a port", func(t *testing.T) {
		c := cfg(Node{ID: 3, Role: RoleMeta, Port: 6660, DataDir: "/var/lib/predastore/meta"})
		c.Hosts[0].Nodes[0].BindAddr = "0.0.0.0:8443"

		err := c.Validate()

		require.Error(t, err)
		assert.Contains(t, err.Error(), "must not carry a port")
	})

	t.Run("accepted on a gate", func(t *testing.T) {
		c := cfg(Node{ID: 3, Role: RoleMeta, Port: 6660, DataDir: "/var/lib/predastore/meta"})
		c.Hosts[0].Nodes[0].BindAddr = "0.0.0.0"

		require.NoError(t, c.Validate())
	})
}

// TestLoad_RepairTable proves the sweep's settings survive the file, and that a
// file saying nothing about repair leaves it off.
func TestLoad_RepairTable(t *testing.T) {
	write := func(t *testing.T, body string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "s3d.toml")
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
		return path
	}
	const base = "version = 1\nregion = \"ap-southeast-2\"\n\n[rs]\ndata = 2\nparity = 1\n"

	t.Run("set", func(t *testing.T) {
		cfg, err := Load(write(t, base+`
[repair]
enabled = true
workers = 4
page_size = 256
interval_seconds = 120
`))
		require.NoError(t, err)
		assert.Equal(t, Repair{Enabled: true, Workers: 4, PageSize: 256, IntervalSeconds: 120}, cfg.Repair)
	})

	t.Run("absent", func(t *testing.T) {
		cfg, err := Load(write(t, base))
		require.NoError(t, err)
		assert.Equal(t, Repair{}, cfg.Repair, "repair is opt-in")
	})

	t.Run("rs availability settings", func(t *testing.T) {
		cfg, err := Load(write(t, base+"degraded_writes = true\nhinted_handoff = true\n"))
		require.NoError(t, err)
		assert.True(t, cfg.RS.DegradedWrites)
		assert.True(t, cfg.RS.HintedHandoff)

		off, err := Load(write(t, base))
		require.NoError(t, err)
		assert.False(t, off.RS.DegradedWrites, "degraded writes are opt-in")
		assert.False(t, off.RS.HintedHandoff, "handoff is opt-in")
	})
}
