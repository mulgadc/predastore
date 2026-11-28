package config

import (
	"strings"
	"testing"

	"github.com/pelletier/go-toml/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The omitted, explicitly empty, and scoped forms of an [[auth]] policy must
// stay distinguishable through TOML decoding and the conversion to the gate's
// entry type: omitted means unrestricted, empty means deny-all, scoped means
// the rules.
func TestAuthEntryPolicyPresenceSurvivesTOML(t *testing.T) {
	tomlConfig := `
[[auth]]
access_key_id = "AKIAOMITTED"
secret_access_key = "s1"
account_id = "000000000001"

[[auth]]
access_key_id = "AKIAEMPTY"
secret_access_key = "s2"
account_id = "000000000002"
policy = []

[[auth]]
access_key_id = "AKIASCOPE"
secret_access_key = "s3"
account_id = "000000000003"
policy = [
  { bucket = "backup", actions = ["s3:GetObject"] },
]
`
	cfg := &Config{}
	dec := toml.NewDecoder(strings.NewReader(tomlConfig))
	dec.DisallowUnknownFields()
	require.NoError(t, dec.Decode(cfg))
	require.Len(t, cfg.Auth, 3)

	assert.Nil(t, cfg.Auth[0].Policy, "an omitted policy must decode as absent")
	require.NotNil(t, cfg.Auth[1].Policy, "an explicitly empty policy must decode as present")
	assert.Empty(t, cfg.Auth[1].Policy)
	require.NotNil(t, cfg.Auth[2].Policy)
	require.Len(t, cfg.Auth[2].Policy, 1)
}
