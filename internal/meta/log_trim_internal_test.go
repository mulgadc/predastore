package meta

import (
	"bytes"
	"errors"
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
	appends := 0
	started := make(chan struct{})
	appendErr := make(chan error, 1)
	go func() {
		close(started)
		for next := uint64(entries + 1); !stop.Load(); next++ {
			start := time.Now()
			if err := store.StoreLogs([]*raft.Log{{Index: next, Term: 1, Type: raft.LogCommand, Data: []byte("v")}}); err != nil {
				appendErr <- err
				return
			}
			worst = max(worst, time.Since(start))
			appends++
		}
		appendErr <- nil
	}()
	<-started
	require.NoError(t, store.DeleteRange(1, entries-1))
	stop.Store(true)
	require.NoError(t, <-appendErr)

	first, err := store.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(entries), first)
	t.Logf("worst of %d appends %v", appends, worst)
	require.Positive(t, appends, "no append overlapped the trim")
	require.Less(t, worst, 250*time.Millisecond)
}

// recordDeletes makes store's deletes visible to the test, failing the one
// numbered failAt (from 1) and every one after it; zero never fails.
func recordDeletes(store *logStore, failAt int) *[][2]uint64 {
	calls := &[][2]uint64{}
	del := store.deleteRange
	store.deleteRange = func(from, to uint64) error {
		*calls = append(*calls, [2]uint64{from, to})
		if failAt > 0 && len(*calls) >= failAt {
			return errors.New("injected")
		}
		return del(from, to)
	}
	return calls
}

func TestLogStore_InterruptedPrefixTrimLeavesAShorterLog(t *testing.T) {
	store, err := openLogStore(filepath.Join(t.TempDir(), "raft.db"))
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	store.batch = 100
	fillLog(t, store, 1, 1_000, 16)

	recordDeletes(store, 3)
	require.Error(t, store.DeleteRange(1, 899))
	assertLogRange(t, store, 201, 1_000)
	want := bytes.Repeat([]byte{0x6c}, 16)
	for i := uint64(201); i <= 1_000; i++ {
		var entry raft.Log
		require.NoError(t, store.GetLog(i, &entry))
		require.Equal(t, raft.Log{Index: i, Term: 1, Type: raft.LogCommand, Data: want}, raft.Log{Index: entry.Index, Term: entry.Term, Type: entry.Type, Data: entry.Data})
	}

	store.deleteRange = store.BoltStore.DeleteRange
	require.NoError(t, store.DeleteRange(201, 899))
	assertLogRange(t, store, 900, 1_000)
}

func TestLogStore_OnlyAPrefixIsSplit(t *testing.T) {
	cases := []struct {
		name               string
		from, to           uint64
		calls              [][2]uint64
		wantFirst, wantEnd uint64
	}{
		{"prefix", 1, 250, [][2]uint64{{1, 100}, {101, 200}, {201, 250}}, 251, 1_000},
		{"conflict at the first entry, or the whole log", 1, 1_000, [][2]uint64{{1, 1_000}}, 0, 0},
		{"suffix", 500, 1_000, [][2]uint64{{500, 1_000}}, 1, 499},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, err := openLogStore(filepath.Join(t.TempDir(), "raft.db"))
			require.NoError(t, err)
			t.Cleanup(func() { store.Close() })
			store.batch = 100
			fillLog(t, store, 1, 1_000, 16)

			calls := recordDeletes(store, 0)
			require.NoError(t, store.DeleteRange(tc.from, tc.to))
			require.Equal(t, tc.calls, *calls)
			if tc.wantEnd == 0 {
				last, err := store.LastIndex()
				require.NoError(t, err)
				require.Zero(t, last)
				return
			}
			assertLogRange(t, store, tc.wantFirst, tc.wantEnd)
		})
	}
}

// A second compaction can read the first index before an earlier one moves
// it, and then asks for a prefix that starts below the log. It is still split.
func TestLogStore_StalePrefixIsSplit(t *testing.T) {
	store, err := openLogStore(filepath.Join(t.TempDir(), "raft.db"))
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	store.batch = 100
	fillLog(t, store, 1, 1_000, 16)
	require.NoError(t, store.DeleteRange(1, 300))

	calls := recordDeletes(store, 0)
	require.NoError(t, store.DeleteRange(1, 450))
	require.Equal(t, [][2]uint64{{301, 400}, {401, 450}}, *calls)
	assertLogRange(t, store, 451, 1_000)
}
