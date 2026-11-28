package meta_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Through a real replica and the client, a paged scan keeps its limit and an
// exclusive cursor, and no limit returns every key under the prefix.
func TestAPagedScanThroughTheReplicaKeepsLimitAndCursor(t *testing.T) {
	cli, _ := startReplayReplica(t, t.TempDir(), true)
	ctx := t.Context()
	for _, k := range []string{"p/a", "p/b", "p/c", "p/d", "p/e", "q/a"} {
		require.NoError(t, cli.Put(ctx, k, []byte("v")))
	}

	page := func(after string, limit int) []string {
		items, err := cli.LeaderScanFrom(ctx, "p/", after, limit)
		require.NoError(t, err)
		keys := make([]string, len(items))
		for i, it := range items {
			keys[i] = it.Key
		}
		return keys
	}
	assert.Equal(t, []string{"p/a", "p/b"}, page("", 2))
	assert.Equal(t, []string{"p/c", "p/d"}, page("p/b", 2), "a cursor equal to a key is exclusive")
	assert.Equal(t, []string{"p/e"}, page("p/d", 2))
	assert.Equal(t, []string{"p/a", "p/b", "p/c", "p/d", "p/e"}, page("", 0))
}
