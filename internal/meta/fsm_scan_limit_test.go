package meta

import (
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A limited scan returns once it has handed over the limit-th key, so it
// reads no key after it, and a cursor still starts strictly after itself.
func TestFSM_ScanFromLimitStopsOnTheLimitRow(t *testing.T) {
	fsm := NewFSM(newTestDB(t))
	var logs []*raft.Log
	for i, k := range []string{"p/a", "p/b", "p/c", "p/d", "q/a"} {
		logs = append(logs, commandLog(t, uint64(i+1), Command{Type: CommandPut, Key: []byte(k), Value: []byte("v")}))
	}
	fsm.ApplyBatch(logs)

	scan := func(after string, limit int) []string {
		var keys []string
		require.NoError(t, fsm.ScanFromLimit("p/", after, limit, func(key string, _ []byte) error {
			keys = append(keys, key)
			return nil
		}))
		return keys
	}
	assert.Equal(t, []string{"p/a", "p/b"}, scan("", 2))
	assert.Equal(t, []string{"p/c", "p/d"}, scan("p/b", 2))
	assert.Equal(t, []string{"p/d"}, scan("p/c", 5))
	assert.Equal(t, []string{"p/a", "p/b", "p/c", "p/d"}, scan("", 0), "zero is no limit")
}
