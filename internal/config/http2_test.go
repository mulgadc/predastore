// The [s3] enable_http2 tri-state. Absent and false differ: the gate offers h2
// unless a deployment says otherwise, so the key has to distinguish "not
// configured" from "configured off".
package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHTTP2EnabledDefaultsOnWhenUnset(t *testing.T) {
	require.True(t, S3{}.HTTP2Enabled(), "an absent key must leave h2 advertised")

	on, off := true, false
	require.True(t, S3{EnableHTTP2: &on}.HTTP2Enabled())
	require.False(t, S3{EnableHTTP2: &off}.HTTP2Enabled())
}

// loadWith writes a minimal valid config with body appended and loads it.
func loadWith(t *testing.T, body string) *Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "predastore.toml")
	base := `version = 1
region = "ap-southeast-2"

[rs]
data = 3
parity = 2
`
	require.NoError(t, os.WriteFile(path, []byte(base+body), 0o600))

	cfg, err := Load(path)
	require.NoError(t, err)
	return cfg
}

func TestLoad_HTTP2DefaultsOnWithNoS3Table(t *testing.T) {
	// Every config file predating this key is in exactly this state.
	require.Nil(t, loadWith(t, "").S3.EnableHTTP2)
	require.True(t, loadWith(t, "").S3.HTTP2Enabled())
}

func TestLoad_HTTP2CanBeTurnedOff(t *testing.T) {
	cfg := loadWith(t, "\n[s3]\nenable_http2 = false\n")

	require.NotNil(t, cfg.S3.EnableHTTP2)
	require.False(t, cfg.S3.HTTP2Enabled())
}

func TestLoad_HTTP2CanBeStatedExplicitlyOn(t *testing.T) {
	cfg := loadWith(t, "\n[s3]\nenable_http2 = true\n")

	require.NotNil(t, cfg.S3.EnableHTTP2)
	require.True(t, cfg.S3.HTTP2Enabled())
}
