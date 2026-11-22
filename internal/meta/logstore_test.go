package meta

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writtenBytes is this process's write(2) byte count so far.
func writtenBytes(t *testing.T) int64 {
	t.Helper()
	raw, err := os.ReadFile("/proc/self/io")
	require.NoError(t, err)
	for line := range strings.SplitSeq(string(raw), "\n") {
		if v, ok := strings.CutPrefix(line, "wchar:"); ok {
			n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
			require.NoError(t, err)
			return n
		}
	}
	t.Fatal("no wchar in /proc/self/io")
	return 0
}

// fillLog appends entries [from, to] of size bytes in batches, the way a
// replica's log grows under load.
func fillLog(t *testing.T, store raft.LogStore, from, to uint64, size int) {
	t.Helper()
	data := bytes.Repeat([]byte{0x6c}, size)
	batch := make([]*raft.Log, 0, 1000)
	for i := from; i <= to; i++ {
		batch = append(batch, &raft.Log{Index: i, Term: 1, Type: raft.LogCommand, Data: data})
		if len(batch) == cap(batch) || i == to {
			require.NoError(t, store.StoreLogs(batch))
			batch = batch[:0]
		}
	}
}

// TestOpenLogStore_CommitCostDoesNotGrowWithFreedPages is the regression the
// store options exist for: after a snapshot truncates the front of the log, a
// one-entry commit must cost about a B-tree path, not a rewrite of every page
// the truncation freed. With bolt's synced freelist this truncation leaves a
// freelist several times the bound below, and each commit writes all of it.
func TestOpenLogStore_CommitCostDoesNotGrowWithFreedPages(t *testing.T) {
	if testing.Short() {
		t.Skip("fills a log of 40,000 entries")
	}
	store, err := openLogStore(filepath.Join(t.TempDir(), "raft.db"))
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })

	const entries, truncated, probes = 40_000, 30_000, 200
	fillLog(t, store, 1, entries, 700)
	require.NoError(t, store.DeleteRange(1, truncated))

	data := bytes.Repeat([]byte{0x6c}, 700)
	// Pages a transaction frees are released by the next one.
	require.NoError(t, store.StoreLog(&raft.Log{Index: entries + 1, Term: 1, Type: raft.LogCommand, Data: data}))
	require.Greater(t, store.Stats().FreePageN, 5_000, "the truncation must free enough pages to matter")

	before := writtenBytes(t)
	for i := uint64(2); i <= probes+1; i++ {
		require.NoError(t, store.StoreLog(&raft.Log{Index: entries + i, Term: 1, Type: raft.LogCommand, Data: data}))
	}
	perCommit := (writtenBytes(t) - before) / probes

	assert.Less(t, perCommit, int64(64<<10), "bytes written per one-entry commit after truncation")
}

// TestOpenLogStore_ReadsALogEitherBuildWrote covers upgrade and rollback: a log
// the previous build wrote opens under the new options with every entry, and a
// log the new options wrote opens again under bolt's defaults.
func TestOpenLogStore_ReadsALogEitherBuildWrote(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.db")

	old, err := raftboltdb.NewBoltStore(path)
	require.NoError(t, err)
	fillLog(t, old, 1, 2_000, 200)
	require.NoError(t, old.DeleteRange(1, 1_000))
	require.NoError(t, old.Close())

	current, err := openLogStore(path)
	require.NoError(t, err)
	assertLogRange(t, current, 1_001, 2_000)
	fillLog(t, current, 2_001, 3_000, 200)
	require.NoError(t, current.DeleteRange(1_001, 1_500))
	require.NoError(t, current.Close())

	rolledBack, err := raftboltdb.NewBoltStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { rolledBack.Close() })
	assertLogRange(t, rolledBack, 1_501, 3_000)
	fillLog(t, rolledBack, 3_001, 3_100, 200)
	assertLogRange(t, rolledBack, 1_501, 3_100)
}

// assertLogRange requires the store to hold exactly entries [first, last].
func assertLogRange(t *testing.T, store *raftboltdb.BoltStore, first, last uint64) {
	t.Helper()
	gotFirst, err := store.FirstIndex()
	require.NoError(t, err)
	gotLast, err := store.LastIndex()
	require.NoError(t, err)
	require.Equal(t, first, gotFirst)
	require.Equal(t, last, gotLast)
	for i := first; i <= last; i++ {
		var l raft.Log
		require.NoError(t, store.GetLog(i, &l), "entry %d", i)
		require.Equal(t, i, l.Index)
	}
}
