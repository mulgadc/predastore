// Package tests holds suites about the repository's shipped artefacts rather
// than about a Go package: the config profiles, the deployment scripts and the
// contract between them.
package tests

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/mulgadc/predastore"
	"github.com/stretchr/testify/require"
)

// The shipped profiles are the quick start. A change that leaves one of them
// unloadable is only found when somebody runs start.sh, so load them here.
func TestShippedProfilesLoad(t *testing.T) {
	profiles, err := filepath.Glob("../config/*.toml")
	require.NoError(t, err)
	require.NotEmpty(t, profiles, "no profiles under config/")

	for _, path := range profiles {
		t.Run(filepath.Base(path), func(t *testing.T) {
			cfg, err := predastore.LoadConfig(path)
			require.NoError(t, err)
			require.NotEmpty(t, cfg.Hosts)
			// Every host carries an admin port so a probe has a target
			// without the operator editing the file first.
			for _, h := range cfg.Hosts {
				require.NotZero(t, h.AdminPort, "host %d has no admin_port", h.ID)
			}
		})
	}
}

// render-config.sh writes the config a container runs on, and s3d only ever
// sees the result. Parsing its output here is the only check that the two agree
// short of starting the image.
func TestRenderedContainerConfigLoads(t *testing.T) {
	tests := []struct {
		name  string
		env   []string
		hosts int
	}{
		{name: "single", env: []string{"PREDA_PEERS=127.0.0.1"}, hosts: 1},
		{
			name: "cluster",
			env: []string{
				"PREDA_PEERS=10.11.12.1,10.11.12.2,10.11.12.3,10.11.12.4",
				"PREDA_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE",
				"PREDA_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			},
			hosts: 4,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command("sh", "../deploy/docker/render-config.sh")
			cmd.Env = append(os.Environ(), tc.env...)
			out, err := cmd.Output()
			require.NoError(t, err, "render-config.sh failed: %s", stderrOf(err))

			path := filepath.Join(t.TempDir(), "rendered.toml")
			require.NoError(t, os.WriteFile(path, out, 0o600))

			cfg, err := predastore.LoadConfig(path)
			require.NoError(t, err, "rendered config:\n%s", out)
			require.Len(t, cfg.Hosts, tc.hosts)
			for _, h := range cfg.Hosts {
				require.Equal(t, 9099, h.AdminPort)
			}
		})
	}
}

func stderrOf(err error) string {
	if exit, ok := errors.AsType[*exec.ExitError](err); ok {
		return string(exit.Stderr)
	}
	return ""
}
