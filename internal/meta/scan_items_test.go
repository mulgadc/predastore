package meta

import (
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A replica's scan with a limit copies the values of the rows it returns and
// no other, from the start and from a cursor; without a limit it copies all.
func TestScanItemsCopiesNoValuePastTheLimit(t *testing.T) {
	fsm := NewFSM(newTestDB(t))
	var logs []*raft.Log
	for i, k := range []string{"p/a", "p/b", "p/c", "p/d", "p/e", "q/a"} {
		logs = append(logs, commandLog(t, uint64(i+1), Command{Type: CommandPut, Key: []byte(k), Value: []byte("v")}))
	}
	fsm.ApplyBatch(logs)
	var copies atomic.Int64
	fsm.copyValue = func(item *badger.Item) ([]byte, error) {
		copies.Add(1)
		return item.ValueCopy(nil)
	}
	s := &Server{fsm: fsm}

	scan := func(after string, limit int) []string {
		copies.Store(0)
		items, err := s.scanItems(MetaRequest{Key: []byte("p/"), After: []byte(after), Limit: limit})
		require.NoError(t, err)
		keys := make([]string, len(items))
		for i, it := range items {
			keys[i] = string(it.Key)
		}
		return keys
	}

	assert.Equal(t, []string{"p/a", "p/b"}, scan("", 2))
	assert.Equal(t, int64(2), copies.Load(), "a page of two copies two values")
	assert.Equal(t, []string{"p/c", "p/d"}, scan("p/b", 2))
	assert.Equal(t, int64(2), copies.Load(), "a page from a cursor copies two values")
	assert.Equal(t, []string{"p/a", "p/b", "p/c", "p/d", "p/e"}, scan("", 0))
	assert.Equal(t, int64(5), copies.Load(), "no limit copies every value under the prefix")
}
