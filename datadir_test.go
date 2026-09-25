package predastore_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mulgadc/predastore"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/testcerts"
	"github.com/stretchr/testify/require"
)

// dirHost is a gate, a meta replica and three blob nodes, each non-gate node on
// a directory of its own the way separately mounted drives would be.
func dirHost(t *testing.T) (*config.Config, map[config.NodeID]string) {
	t.Helper()
	certPath, keyPath, _ := testcerts.Generate(t)

	root := t.TempDir()
	dirs := map[config.NodeID]string{}
	nodes := []config.Node{{ID: 1, Role: config.RoleGate, Port: freePort(t), BindAddr: "127.0.0.1"}}
	for i, role := range []config.Role{config.RoleMeta, config.RoleBlob, config.RoleBlob, config.RoleBlob} {
		id := config.NodeID(2 + i)
		dirs[id] = filepath.Join(root, fmt.Sprintf("disk%d", i+1))
		require.NoError(t, os.MkdirAll(dirs[id], 0o700))
		nodes = append(nodes, config.Node{ID: id, Role: role, Port: 7200 + i, DataDir: dirs[id]})
	}

	cfg := &config.Config{
		Version: config.Version,
		Region:  "ap-southeast-2",
		RS:      config.RS{Data: 2, Parity: 1},
		Hosts: []config.Host{{
			ID:        1,
			Addr:      "127.0.0.1",
			DataDir:   root,
			TLSCert:   certPath,
			TLSKey:    keyPath,
			AdminPort: freePort(t),
			Nodes:     nodes,
		}},
	}
	require.NoError(t, cfg.Validate())
	return cfg, dirs
}

// startRun runs host 1 of cfg in the background until cancel is called.
func startRun(t *testing.T, cfg *config.Config) (<-chan error, context.CancelFunc) {
	t.Helper()
	key := newMasterKey(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- predastore.Run(ctx, predastore.Options{Config: cfg, HostID: 1, MasterKey: key})
	}()
	return done, cancel
}

// runUntilFailure runs host 1 and returns the error it stops with. A host that
// is still running after the deadline is the bug: it failed without saying so.
func runUntilFailure(t *testing.T, cfg *config.Config) error {
	t.Helper()
	done, cancel := startRun(t, cfg)
	defer cancel()
	select {
	case err := <-done:
		return err
	case <-time.After(15 * time.Second):
		cancel()
		<-done
		t.Fatal("host kept running with a node that could not open its store")
		return nil
	}
}

// skipAsRoot skips a case that relies on mode bits, which root bypasses.
func skipAsRoot(t *testing.T) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("running as root, which ignores directory mode bits; run as an unprivileged user to cover this")
	}
}

// A root-owned drive under an unprivileged service user stops the host before
// any node starts, naming the node, its role and its path, and no healthy node
// writes anything first.
func TestUnwritableDataDirStopsTheHostBeforeAnyNodeStarts(t *testing.T) {
	skipAsRoot(t)

	for _, tc := range []struct {
		role config.Role
		id   config.NodeID
	}{
		{config.RoleBlob, 3},
		{config.RoleMeta, 2},
	} {
		t.Run(string(tc.role), func(t *testing.T) {
			cfg, dirs := dirHost(t)
			require.NoError(t, os.Chmod(dirs[tc.id], 0o500))
			t.Cleanup(func() { _ = os.Chmod(dirs[tc.id], 0o700) })

			err := runUntilFailure(t, cfg)
			require.Error(t, err)
			msg := err.Error()
			require.Contains(t, msg, fmt.Sprintf("%s node %d", tc.role, tc.id))
			require.Contains(t, msg, dirs[tc.id])
			require.Contains(t, msg, "not writable")
			require.Contains(t, msg, "permission denied")

			for id, dir := range dirs {
				entries, rerr := os.ReadDir(dir)
				require.NoError(t, rerr)
				require.Empty(t, entries, "node %d wrote to %s before the host refused to start", id, dir)
			}
		})
	}
}

// Every unwritable drive is named at once, so an operator with several fixes
// them together rather than one restart at a time.
func TestEveryUnwritableDataDirIsReported(t *testing.T) {
	skipAsRoot(t)

	cfg, dirs := dirHost(t)
	for _, id := range []config.NodeID{3, 4, 5} {
		require.NoError(t, os.Chmod(dirs[id], 0o500))
		t.Cleanup(func() { _ = os.Chmod(dirs[id], 0o700) })
	}

	err := runUntilFailure(t, cfg)
	require.Error(t, err)
	for _, id := range []config.NodeID{3, 4, 5} {
		require.Contains(t, err.Error(), fmt.Sprintf("blob node %d data directory %s", id, dirs[id]))
	}
}

// A directory that does not exist yet is created, as the node would create it,
// when its parent is writable.
func TestMissingDataDirIsCreated(t *testing.T) {
	cfg, dirs := dirHost(t)
	require.NoError(t, os.Remove(dirs[4]))

	done, cancel := startRun(t, cfg)
	defer cancel()
	awaitProbe(t, fmt.Sprintf("http://127.0.0.1:%d/readyz", cfg.Hosts[0].AdminPort), 200)
	cancel()
	require.NoError(t, <-done)

	info, err := os.Stat(dirs[4])
	require.NoError(t, err)
	require.True(t, info.IsDir())
	entries, err := os.ReadDir(dirs[4])
	require.NoError(t, err)
	for _, e := range entries {
		require.False(t, strings.HasPrefix(e.Name(), ".write-probe-"), "probe file %s left behind", e.Name())
	}
}

// A store that fails to open in a writable directory ends the host with that
// error, attributed to the node, rather than leaving the rest serving.
func TestStoreThatFailsToOpenEndsTheHost(t *testing.T) {
	cfg, dirs := dirHost(t)
	require.NoError(t, os.WriteFile(filepath.Join(dirs[4], "state.json"), []byte("not json"), 0o600))

	err := runUntilFailure(t, cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "blob node 4")
	require.Contains(t, err.Error(), dirs[4])
}
