package s3db

import (
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDBNodeConfig_RaftAddr(t *testing.T) {
	tests := []struct {
		name string
		node DBNodeConfig
		want string
	}{
		{
			name: "explicit raft port",
			node: DBNodeConfig{Host: "localhost", Port: 6660, RaftPort: 7777},
			want: "localhost:7777",
		},
		{
			name: "default raft port (http + 1000)",
			node: DBNodeConfig{Host: "localhost", Port: 6660, RaftPort: 0},
			want: "localhost:7660",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.node.RaftAddr())
		})
	}
}

func TestDBNodeConfig_RaftAdvertiseAddr(t *testing.T) {
	tests := []struct {
		name string
		node DBNodeConfig
		want string
	}{
		{
			name: "advertise host set",
			node: DBNodeConfig{Host: "0.0.0.0", Port: 6660, AdvertiseHost: "10.0.0.1"},
			want: "10.0.0.1:7660",
		},
		{
			name: "no advertise host, normal host",
			node: DBNodeConfig{Host: "192.168.1.1", Port: 6660},
			want: "192.168.1.1:7660",
		},
		{
			name: "no advertise host, wildcard replaced with localhost",
			node: DBNodeConfig{Host: "0.0.0.0", Port: 6660},
			want: "127.0.0.1:7660",
		},
		{
			name: "explicit raft port with advertise",
			node: DBNodeConfig{Host: "0.0.0.0", Port: 6660, RaftPort: 9999, AdvertiseHost: "10.0.0.5"},
			want: "10.0.0.5:9999",
		},
		{
			name: "explicit raft port, wildcard, no advertise",
			node: DBNodeConfig{Host: "0.0.0.0", Port: 6660, RaftPort: 9999},
			want: "127.0.0.1:9999",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.node.RaftAdvertiseAddr())
		})
	}
}

func TestGenObjectHash(t *testing.T) {
	t.Run("deterministic", func(t *testing.T) {
		h1 := GenObjectHash("bucket", "key")
		h2 := GenObjectHash("bucket", "key")
		assert.Equal(t, h1, h2)
	})

	t.Run("matches manual sha256", func(t *testing.T) {
		expected := sha256.Sum256([]byte("mybucket/mykey"))
		assert.Equal(t, expected, GenObjectHash("mybucket", "mykey"))
	})

	t.Run("different inputs produce different hashes", func(t *testing.T) {
		h1 := GenObjectHash("bucket-a", "key")
		h2 := GenObjectHash("bucket-b", "key")
		assert.NotEqual(t, h1, h2)
	})

	t.Run("format is bucket/object", func(t *testing.T) {
		expected := sha256.Sum256([]byte("b/k"))
		assert.Equal(t, expected, GenObjectHash("b", "k"))
	})
}

func TestClusterConfig_GetNode(t *testing.T) {
	cfg := &ClusterConfig{
		Nodes: []DBNodeConfig{
			{ID: 1, Host: "host1"},
			{ID: 2, Host: "host2"},
		},
	}

	t.Run("found", func(t *testing.T) {
		node := cfg.GetNode(1)
		require.NotNil(t, node)
		assert.Equal(t, "host1", node.Host)
	})

	t.Run("not found", func(t *testing.T) {
		node := cfg.GetNode(99)
		assert.Nil(t, node)
	})
}
