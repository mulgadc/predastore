package meta

import (
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

// TestLogStore_PrefixTrimDoesNotBlockAppends trims the prefix a snapshot
// covers, as raft's compactLogs does, while the leader keeps appending. Every
// append waits on the trim's write transaction, so no append may wait long.
func TestLogStore_PrefixTrimDoesNotBlockAppends(t *testing.T) {
	if testing.Short() {
		t.Skip("fills a log of 200,000 entries")
	}
	const entries = 200_000
	store, err := openLogStore(filepath.Join(t.TempDir(), "raft.db"))
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	fillLog(t, store, 1, entries, 1<<10)

	var stop atomic.Bool
	var worst time.Duration
	appendErr := make(chan error, 1)
	go func() {
		for next := uint64(entries + 1); !stop.Load(); next++ {
			start := time.Now()
			if err := store.StoreLogs([]*raft.Log{{Index: next, Term: 1, Type: raft.LogCommand, Data: []byte("v")}}); err != nil {
				appendErr <- err
				return
			}
			worst = max(worst, time.Since(start))
		}
		appendErr <- nil
	}()
	require.NoError(t, store.DeleteRange(1, entries-1))
	stop.Store(true)
	require.NoError(t, <-appendErr)

	first, err := store.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(entries), first)
	t.Logf("worst append %v", worst)
	require.Less(t, worst, 250*time.Millisecond)
}
