package s3

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithConfigPath(t *testing.T) {
	s := &Server{}
	opt := WithConfigPath("/path/to/config.toml")
	require.NoError(t, opt(s))
	assert.Equal(t, "/path/to/config.toml", s.configPath)
}

func TestWithAddress(t *testing.T) {
	s := &Server{}
	opt := WithAddress("192.168.1.1", 9443)
	require.NoError(t, opt(s))
	assert.Equal(t, "192.168.1.1", s.host)
	assert.Equal(t, 9443, s.port)
}

func TestWithTLS(t *testing.T) {
	s := &Server{}
	opt := WithTLS("/path/cert.pem", "/path/key.pem")
	require.NoError(t, opt(s))
	assert.Equal(t, "/path/cert.pem", s.tlsCert)
	assert.Equal(t, "/path/key.pem", s.tlsKey)
}

func TestWithBasePath(t *testing.T) {
	s := &Server{}
	opt := WithBasePath("/var/data")
	require.NoError(t, opt(s))
	assert.Equal(t, "/var/data", s.basePath)
}

func TestWithDebug(t *testing.T) {
	s := &Server{}

	opt := WithDebug(true)
	require.NoError(t, opt(s))
	assert.True(t, s.debug)

	opt = WithDebug(false)
	require.NoError(t, opt(s))
	assert.False(t, s.debug)
}

func TestWithBackend(t *testing.T) {
	t.Run("distributed", func(t *testing.T) {
		s := &Server{}
		opt := WithBackend(BackendDistributed)
		require.NoError(t, opt(s))
		assert.Equal(t, BackendDistributed, s.backendType)
	})

	t.Run("empty string preserves default", func(t *testing.T) {
		s := &Server{backendType: BackendDistributed}
		opt := WithBackend("")
		require.NoError(t, opt(s))
		assert.Equal(t, BackendDistributed, s.backendType)
	})
}

func TestWithNodeID(t *testing.T) {
	s := &Server{}
	opt := WithNodeID(3)
	require.NoError(t, opt(s))
	assert.Equal(t, 3, s.nodeID)

	opt = WithNodeID(-1)
	require.NoError(t, opt(s))
	assert.Equal(t, -1, s.nodeID)
}

func TestWithPprof(t *testing.T) {
	t.Run("enabled with custom path", func(t *testing.T) {
		s := &Server{}
		opt := WithPprof(true, "/tmp/my-profile.prof")
		require.NoError(t, opt(s))
		assert.True(t, s.pprofEnabled)
		assert.Equal(t, "/tmp/my-profile.prof", s.pprofOutputPath)
	})

	t.Run("enabled with default path", func(t *testing.T) {
		s := &Server{}
		opt := WithPprof(true, "")
		require.NoError(t, opt(s))
		assert.True(t, s.pprofEnabled)
		assert.Equal(t, "/tmp/predastore-cpu.prof", s.pprofOutputPath)
	})

	t.Run("disabled", func(t *testing.T) {
		s := &Server{}
		opt := WithPprof(false, "")
		require.NoError(t, opt(s))
		assert.False(t, s.pprofEnabled)
	})
}
