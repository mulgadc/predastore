package meta

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/stretchr/testify/require"
)

func TestFreshAuthorityWaitsForActualApply(t *testing.T) {
	cluster := startMapHeadTestCluster(t)
	reader := cluster.client(cluster.voters...)
	leader := cluster.leader(t, reader)
	replica := cluster.replicas[leader]
	require.Empty(t, replica.authorityRead(t.Context()))

	replica.fsm.mu.Lock()
	release := sync.OnceFunc(replica.fsm.mu.Unlock)
	defer release()
	key := testHeadPrefix + "1"
	beforeWrite := replica.raft.CommitIndex()
	written := make(chan error, 1)
	go func() { written <- reader.Put(t.Context(), key, []byte("acknowledged")) }()
	require.Eventually(t, func() bool {
		return replica.raft.CommitIndex() > beforeWrite
	}, 2*time.Second, 5*time.Millisecond)

	writeIndex := replica.raft.CommitIndex()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	require.Equal(t, ErrCodeBehind, replica.authorityRead(ctx))
	require.Equal(t, writeIndex, replica.raft.LastIndex(), "a warm proof must leave the log unchanged")

	release()
	require.NoError(t, <-written)
	got, err := reader.LeaderLastKey(t.Context(), testHeadPrefix)
	require.NoError(t, err)
	require.Equal(t, key, got)
}

func TestFreshAuthorityQuorumRejectsAnOldTerm(t *testing.T) {
	cluster := startMapHeadTestCluster(t)
	reader := cluster.client(cluster.voters...)
	leader := cluster.leader(t, reader)
	replica := cluster.replicas[leader]
	require.Empty(t, replica.authorityRead(t.Context()))
	oldTerm := replica.raft.CurrentTerm()
	voters := replica.raft.GetConfiguration().Configuration().Servers
	require.NoError(t, replica.raft.LeadershipTransfer().Error())
	var successor config.NodeID
	require.Eventually(t, func() bool {
		for _, id := range cluster.voters {
			status, err := reader.Status(t.Context(), id)
			if err == nil && status.IsLeader && cluster.replicas[id].raft.CurrentTerm() > oldTerm {
				successor = id
				return true
			}
		}
		return false
	}, 3*time.Second, 10*time.Millisecond)
	require.NotEqual(t, leader, successor)

	key := testHeadPrefix + "9"
	require.NoError(t, reader.Put(t.Context(), key, []byte("successor write")))
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.False(t, replica.confirmAuthorityQuorum(ctx, oldTerm, voters))
	got, err := reader.LeaderLastKey(t.Context(), testHeadPrefix)
	require.NoError(t, err)
	require.Equal(t, key, got)
}
