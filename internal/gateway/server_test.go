package gateway

import (
	"testing"

	"github.com/mulgadc/predastore/pkg/masterkey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testKey is a throwaway master key; the gateway only records its fingerprint.
func testKey(t *testing.T) *masterkey.Key {
	t.Helper()
	key, err := masterkey.New(make([]byte, 32))
	require.NoError(t, err)
	return key
}

// TestNewServer_MissingConfig verifies NewServer refuses to run on zero values
// rather than serving an empty region with no buckets.
func TestNewServer_MissingConfig(t *testing.T) {
	_, err := NewServer(ServerConfig{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no configuration provided")
}

// TestNewServer_MissingMasterKey verifies that NewServer fails fast when no
// key is wired. The check happens before any backend init, so no goroutines or
// sockets are spawned by this test.
func TestNewServer_MissingMasterKey(t *testing.T) {
	_, err := NewServer(ServerConfig{Config: &Config{Region: "ap-southeast-2"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "master key is required",
		"error must direct the operator at the missing wiring")
}

// A wired key gets past the key check and lands on the clients check, which
// proves the gateway refuses to serve without a wired cluster runtime.
func TestNewServer_MissingClients(t *testing.T) {
	_, err := NewServer(ServerConfig{
		Config:    &Config{Region: "ap-southeast-2"},
		MasterKey: testKey(t),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no cluster clients provided")
}
