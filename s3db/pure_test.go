package s3db

import (
	"crypto/sha256"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDBNodeConfig_HTTPAddr(t *testing.T) {
	tests := []struct {
		name string
		node DBNodeConfig
		want string
	}{
		{"localhost", DBNodeConfig{Host: "localhost", Port: 6660}, "localhost:6660"},
		{"ip address", DBNodeConfig{Host: "192.168.1.1", Port: 8080}, "192.168.1.1:8080"},
		{"wildcard", DBNodeConfig{Host: "0.0.0.0", Port: 443}, "0.0.0.0:443"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.node.HTTPAddr())
		})
	}
}

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

func TestEscapePathSegment(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"alphanumeric", "hello123", "hello123"},
		{"plus sign encoded", "a+b", "a%2Bb"},
		{"spaces encoded", "hello world", "hello%20world"},
		{"slashes encoded", "path/to/file", "path%2Fto%2Ffile"},
		{"empty string", "", ""},
		{"equals not encoded by PathEscape", "key=value&foo", "key=value&foo"},
		{"percent encoded", "100%done", "100%25done"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, escapePathSegment(tt.input))
		})
	}
}

func TestHexEncodeKey(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"simple ascii", "hello", "68656c6c6f"},
		{"empty", "", ""},
		{"binary-like", "\x00\x01\xff", "0001ff"},
		{"slash in key", "bucket/key", "6275636b65742f6b6579"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, hexEncodeKey(tt.input))
		})
	}
}

func TestDefaultClientConfig(t *testing.T) {
	cfg := DefaultClientConfig()
	require.NotNil(t, cfg)
	assert.Equal(t, "us-east-1", cfg.Region)
	assert.Equal(t, "s3db", cfg.Service)
	assert.Equal(t, 10*time.Second, cfg.Timeout)
	assert.Equal(t, 3, cfg.MaxRetries)
	assert.True(t, cfg.InsecureSkipVerify)
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

func TestServerUriEncode(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		encodeSlash bool
		want        string
	}{
		{"unreserved chars", "ABCabc012-._~", true, "ABCabc012-._~"},
		{"slash not encoded", "/path/to", false, "/path/to"},
		{"slash encoded", "/path/to", true, "%2Fpath%2Fto"},
		{"space encoded", "hello world", true, "hello%20world"},
		{"empty", "", true, ""},
		{"equals and ampersand", "a=b&c", true, "a%3Db%26c"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, UriEncode(tt.input, tt.encodeSlash))
		})
	}
}

func TestServerCanonicalQueryString(t *testing.T) {
	tests := []struct {
		name   string
		params url.Values
		want   string
	}{
		{"empty", url.Values{}, ""},
		{"single param", url.Values{"key": {"value"}}, "key=value"},
		{"sorted keys", url.Values{"z": {"1"}, "a": {"2"}}, "a=2&z=1"},
		{"sorted values", url.Values{"k": {"c", "a", "b"}}, "k=a&k=b&k=c"},
		{"special chars encoded", url.Values{"a b": {"c=d"}}, "a%20b=c%3Dd"},
		{"nil", nil, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, CanonicalQueryString(tt.params))
		})
	}
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

func TestNewClient(t *testing.T) {
	t.Run("with nil config uses defaults", func(t *testing.T) {
		client := NewClient(nil)
		require.NotNil(t, client)
	})

	t.Run("with config", func(t *testing.T) {
		client := NewClient(&ClientConfig{
			Nodes:           []string{"localhost:6660"},
			AccessKeyID:     "AKID",
			SecretAccessKey: "SECRET",
		})
		require.NotNil(t, client)
	})

	t.Run("empty region/service uses defaults", func(t *testing.T) {
		client := NewClient(&ClientConfig{})
		require.NotNil(t, client)
	})
}

func TestClient_AddRemoveNode(t *testing.T) {
	client := NewClient(&ClientConfig{
		Nodes: []string{"node1:6660"},
	})

	// Add new node
	client.AddNode("node2:6660")
	client.AddNode("node2:6660") // Duplicate - should be no-op

	// Remove node
	client.RemoveNode("node1:6660")
	client.RemoveNode("nonexistent:6660") // Should not panic
}

func TestDefaultServerConfig(t *testing.T) {
	cfg := DefaultServerConfig()
	require.NotNil(t, cfg)
	assert.Equal(t, "0.0.0.0:6660", cfg.Addr)
	assert.Equal(t, 30*time.Second, cfg.ReadTimeout)
	assert.Equal(t, 30*time.Second, cfg.WriteTimeout)
	assert.Equal(t, 120*time.Second, cfg.IdleTimeout)
	assert.Equal(t, "config/server.pem", cfg.TLSCert)
	assert.Equal(t, "config/server.key", cfg.TLSKey)
}
