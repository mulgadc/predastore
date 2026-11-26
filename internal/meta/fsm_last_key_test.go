package meta

import (
	"testing"

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
