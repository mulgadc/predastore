package distributed

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupLocalState(t *testing.T) (*LocalState, func()) {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "localstate-test-*")
	require.NoError(t, err)

	dbDir := filepath.Join(tmpDir, "db")
	state, err := NewLocalState(dbDir)
	require.NoError(t, err)

	cleanup := func() {
		state.Close()
		os.RemoveAll(tmpDir)
	}
	return state, cleanup
}

func TestLocalState_Exists(t *testing.T) {
	state, cleanup := setupLocalState(t)
	defer cleanup()

	t.Run("not found", func(t *testing.T) {
		exists, err := state.Exists("tbl", []byte("missing"))
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("found", func(t *testing.T) {
		require.NoError(t, state.Set("tbl", []byte("key1"), []byte("val1")))
		exists, err := state.Exists("tbl", []byte("key1"))
		require.NoError(t, err)
		assert.True(t, exists)
	})
}

func TestLocalState_ListKeys(t *testing.T) {
	state, cleanup := setupLocalState(t)
	defer cleanup()

	require.NoError(t, state.Set("tbl", []byte("prefix/a"), []byte("va")))
	require.NoError(t, state.Set("tbl", []byte("prefix/b"), []byte("vb")))
	require.NoError(t, state.Set("tbl", []byte("other/c"), []byte("vc")))

	keys, err := state.ListKeys("tbl", []byte("prefix/"))
	require.NoError(t, err)
	assert.Len(t, keys, 2)

	// Keys should have table prefix stripped
	for _, k := range keys {
		assert.Contains(t, string(k), "prefix/")
	}
}

func TestLocalState_ListKeys_Empty(t *testing.T) {
	state, cleanup := setupLocalState(t)
	defer cleanup()

	keys, err := state.ListKeys("tbl", []byte("nothing"))
	require.NoError(t, err)
	assert.Empty(t, keys)
}

func TestLocalState_DB(t *testing.T) {
	state, cleanup := setupLocalState(t)
	defer cleanup()

	db := state.DB()
	assert.NotNil(t, db)
}
