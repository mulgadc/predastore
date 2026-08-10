package config

import (
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
