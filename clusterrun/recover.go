package clusterrun

import (
	"fmt"

	"github.com/mulgadc/predastore/internal/state"
	"github.com/mulgadc/predastore/s3db"
)

// RaftPeers maps state replica node ids to the raft members they become. The
// address is the node key the stream layer resolves through the topology, so
// it stays valid however the replica is reached.
func RaftPeers(replicaIDs []int) []s3db.RaftPeer {
	peers := make([]s3db.RaftPeer, len(replicaIDs))
	for i, id := range replicaIDs {
		u := uint64(id) //nolint:gosec // G115: node ids are small positives from a validated topology.
		peers[i] = s3db.RaftPeer{ID: u, Address: state.RaftAddress(u)}
	}
	return peers
}

// RecoverStateReplica rewrites the raft configuration persisted under dataDir
// so the replica answers to nodeID within replicaIDs, without disturbing the
// state it holds.
//
// This is for upgrades that renumber or readdress the replica set: a node
// whose persisted configuration predates the change is not a member of its own
// cluster, and raft will not bootstrap over existing state to fix that. It
// reports whether there was any state to recover; a directory that has never
// been written to needs nothing and is not an error.
//
// The node must not be running.
func RecoverStateReplica(dataDir string, nodeID int, replicaIDs []int) (bool, error) {
	if nodeID <= 0 {
		return false, fmt.Errorf("clusterrun recover: node id %d must be positive", nodeID)
	}
	return s3db.RecoverConfiguration(dataDir, uint64(nodeID), RaftPeers(replicaIDs))
}
