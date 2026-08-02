package s3db

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// RecoverConfiguration rewrites the raft configuration persisted in dataDir to
// the given member set, leaving the replicated state untouched.
//
// It exists for the case where a cluster's members are renumbered or
// readdressed out of band: the persisted configuration then names servers that
// no longer exist, and since raft refuses to bootstrap over existing state, a
// node started against it is not a member of its own cluster and no leader is
// ever elected. Recovery replays the log into the FSM, snapshots it under the
// new configuration and truncates the log, which is what raft documents as the
// operator's escape hatch for exactly this.
//
// Returns false when dataDir holds no raft state, which is the fresh-directory
// case and not an error. The caller must not have the directory open.
func RecoverConfiguration(dataDir string, localID uint64, peers []RaftPeer) (bool, error) {
	if len(peers) == 0 {
		return false, fmt.Errorf("s3db recover: no peers given for node %d", localID)
	}

	// Nothing has ever run here, so there is nothing to recover.
	if _, err := os.Stat(filepath.Join(dataDir, "raft.db")); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("s3db recover: stat raft store: %w", err)
	}

	// The stores below are the ones NewRaftNode opens, in the same layout;
	// recovery reads and rewrites exactly what a running node would.
	badgerDB, err := badger.Open(badger.DefaultOptions(filepath.Join(dataDir, "badger")).
		WithLoggingLevel(badger.WARNING).
		WithSyncWrites(true))
	if err != nil {
		return false, fmt.Errorf("s3db recover: open badger: %w", err)
	}
	defer func() {
		if cerr := badgerDB.Close(); cerr != nil {
			slog.Warn("s3db recover: failed to close badger", "error", cerr)
		}
	}()

	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft.db"))
	if err != nil {
		return false, fmt.Errorf("s3db recover: open bolt store: %w", err)
	}
	defer func() {
		if cerr := boltStore.Close(); cerr != nil {
			slog.Warn("s3db recover: failed to close bolt store", "error", cerr)
		}
	}()

	snapshots, err := raft.NewFileSnapshotStore(filepath.Join(dataDir, "snapshots"), 2, os.Stderr)
	if err != nil {
		return false, fmt.Errorf("s3db recover: open snapshot store: %w", err)
	}

	// A store that exists but was never written to is still the fresh case;
	// RecoverCluster would reject it as operator error.
	hasState, err := raft.HasExistingState(boltStore, boltStore, snapshots)
	if err != nil {
		return false, fmt.Errorf("s3db recover: check existing state: %w", err)
	}
	if !hasState {
		return false, nil
	}

	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(strconv.FormatUint(localID, 10))

	servers := make([]raft.Server, 0, len(peers))
	for _, p := range peers {
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(strconv.FormatUint(p.ID, 10)),
			Address: raft.ServerAddress(p.Address),
		})
	}

	// RecoverCluster only uses the transport to encode the snapshot it writes,
	// so an in-memory one carries no traffic and needs no addressing.
	_, transport := raft.NewInmemTransport(raft.ServerAddress(cfg.LocalID))
	defer func() {
		if cerr := transport.Close(); cerr != nil {
			slog.Warn("s3db recover: failed to close transport", "error", cerr)
		}
	}()

	err = raft.RecoverCluster(cfg, NewFSM(badgerDB), boltStore, boltStore, snapshots, transport, raft.Configuration{Servers: servers})
	if err != nil {
		return false, fmt.Errorf("s3db recover: node %d: %w", localID, err)
	}

	slog.Info("Recovered raft configuration", "dataDir", dataDir, "nodeID", localID, "servers", len(servers))
	return true, nil
}
