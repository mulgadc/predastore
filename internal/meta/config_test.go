package meta

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tunedServer builds a replica far enough to read its raft config back. New
// binds nothing and starts nothing, so this needs no cluster.
func tunedServer(t *testing.T, cfg Config) *raft.Config {
	t.Helper()

	cfg.NodeID = 1
	cfg.DataDir = t.TempDir()
	cfg.Resolver = &rpc.Resolver{}
	cfg.Listeners = []transport.Listener{nil}

	srv, err := New(cfg)
	require.NoError(t, err)

	return srv.raftConfig()
}

// The retention boundary is what decides whether a returning node replays the
// log or takes a whole snapshot, so a profile has to be able to move it. It
// could not before: predastore.go built a Config without these three fields and
// every replica ran the defaults, which put the snapshot path out of reach of
// any test.
func TestRaftTuningSurvivesToRaft(t *testing.T) {
	t.Parallel()

	got := tunedServer(t, Config{
		SnapshotInterval:  time.Second,
		SnapshotThreshold: 4,
		TrailingLogs:      8,
	})

	assert.Equal(t, time.Second, got.SnapshotInterval)
	assert.Equal(t, uint64(4), got.SnapshotThreshold)
	assert.Equal(t, uint64(8), got.TrailingLogs)
}

// A cluster that changes no configuration must not change behaviour, so an
// unset knob has to reach raft as the default rather than as a zero. A zero
// TrailingLogs would truncate the log to nothing on every snapshot.
func TestUnsetRaftTuningTakesTheDefaults(t *testing.T) {
	t.Parallel()

	got := tunedServer(t, Config{})

	assert.Equal(t, 120*time.Second, got.SnapshotInterval)
	assert.Equal(t, uint64(8192), got.SnapshotThreshold)
	assert.Equal(t, uint64(10240), got.TrailingLogs)
}
