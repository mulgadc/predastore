package clusterrun_test

import (
	"testing"

	"github.com/mulgadc/predastore/clusterrun"
	"github.com/mulgadc/predastore/internal/state"
)

// TestRaftPeersUseWireAddresses pins the recovered configuration to the same
// addressing a running replica advertises. A mismatch would leave a recovered
// node holding peers it cannot dial.
func TestRaftPeersUseWireAddresses(t *testing.T) {
	peers := clusterrun.RaftPeers([]int{4, 5, 6})
	if len(peers) != 3 {
		t.Fatalf("RaftPeers returned %d peers, want 3", len(peers))
	}
	for i, id := range []uint64{4, 5, 6} {
		if peers[i].ID != id {
			t.Errorf("peer %d id = %d, want %d", i, peers[i].ID, id)
		}
		if want := state.RaftAddress(id); peers[i].Address != want {
			t.Errorf("peer %d address = %q, want %q", i, peers[i].Address, want)
		}
	}
}

func TestRecoverStateReplicaRejectsNonPositiveID(t *testing.T) {
	if _, err := clusterrun.RecoverStateReplica(t.TempDir(), 0, []int{4}); err == nil {
		t.Fatal("RecoverStateReplica accepted node id 0")
	}
}

// TestRecoverStateReplicaFreshDirectory keeps a migration free of having to
// know which relocated directories were ever written to.
func TestRecoverStateReplicaFreshDirectory(t *testing.T) {
	recovered, err := clusterrun.RecoverStateReplica(t.TempDir(), 4, []int{4, 5, 6})
	if err != nil {
		t.Fatalf("RecoverStateReplica: %v", err)
	}
	if recovered {
		t.Fatal("RecoverStateReplica reported state in an empty directory")
	}
}
