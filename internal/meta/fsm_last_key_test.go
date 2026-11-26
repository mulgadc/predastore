package meta

import (
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"

	"github.com/stretchr/testify/require"
)

func TestLastKeyUsesExclusivePrefixEnd(t *testing.T) {
	db := newTestDB(t)
	for _, key := range []string{"before", "head.1", "head.9", "head/", "head/1", "prefix\xff"} {
		dbSet(t, db, []byte(key), []byte("value"))
	}
	fsm := NewFSM(db)
	for _, tc := range []struct{ prefix, want string }{
		{"head.", "head.9"},
		{"head/", "head/1"},
		{"head.40", ""},
		{"prefix\xff", "prefix\xff"},
		{"\xff", ""},
	} {
		t.Run(tc.prefix, func(t *testing.T) {
			key, err := fsm.LastKey(tc.prefix)
			require.NoError(t, err)
			require.Equal(t, tc.want, key)
		})
	}
}

func TestLastKeyFailsClosedOnPrunedRange(t *testing.T) {
	db := newTestDB(t)
	const prefix = "head."
	for n := range 130 {
		dbSet(t, db, fmt.Appendf(nil, "%s%04d", prefix, n), []byte("head"))
	}
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		for n := 1; n < 130; n++ {
			if err := txn.Delete(fmt.Appendf(nil, "%s%04d", prefix, n)); err != nil {
				return err
			}
		}
		return nil
	}))

	key, err := NewFSM(db).LastKey(prefix)
	require.Error(t, err, "the lookup must not traverse an unbounded deleted range and return %q", key)
	require.Empty(t, key)
}
