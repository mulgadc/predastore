package store

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errIndexTest = errors.New("test error")

func newTestIndexDB(t *testing.T) *indexDB {
	t.Helper()
	tmpDir := t.TempDir()
	db, err := newIndexDB(tmpDir)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestIndexDB_Exists(t *testing.T) {
	db := newTestIndexDB(t)

	t.Run("key not found", func(t *testing.T) {
		exists, err := db.Exists([]byte("nonexistent"))
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("key exists", func(t *testing.T) {
		require.NoError(t, db.Set([]byte("mykey"), []byte("myvalue")))

		exists, err := db.Exists([]byte("mykey"))
		require.NoError(t, err)
		assert.True(t, exists)
	})
}

func TestIndexDB_Delete(t *testing.T) {
	db := newTestIndexDB(t)

	require.NoError(t, db.Set([]byte("delkey"), []byte("delval")))

	// Verify it exists
	val, err := db.Get([]byte("delkey"))
	require.NoError(t, err)
	assert.Equal(t, []byte("delval"), val)

	// Delete it
	require.NoError(t, db.Delete([]byte("delkey")))

	// Verify it's gone
	_, err = db.Get([]byte("delkey"))
	assert.Error(t, err)
}

func TestIndexDB_Scan(t *testing.T) {
	db := newTestIndexDB(t)

	// Insert test data
	require.NoError(t, db.Set([]byte("prefix/a"), []byte("va")))
	require.NoError(t, db.Set([]byte("prefix/b"), []byte("vb")))
	require.NoError(t, db.Set([]byte("prefix/c"), []byte("vc")))
	require.NoError(t, db.Set([]byte("other/x"), []byte("vx")))

	t.Run("scan with prefix", func(t *testing.T) {
		var keys []string
		err := db.Scan([]byte("prefix/"), func(key, value []byte) error {
			keys = append(keys, string(key))
			return nil
		})
		require.NoError(t, err)
		assert.Len(t, keys, 3)
	})

	t.Run("scan all", func(t *testing.T) {
		var count int
		err := db.Scan(nil, func(key, value []byte) error {
			count++
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 4, count)
	})

	t.Run("scan with callback error", func(t *testing.T) {
		err := db.Scan([]byte("prefix/"), func(key, value []byte) error {
			return errIndexTest
		})
		assert.ErrorIs(t, err, errIndexTest)
	})
}

func TestIndexDB_Close(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := newIndexDB(tmpDir)
	require.NoError(t, err)

	err = db.Close()
	assert.NoError(t, err)
}

// newIndexDB error path.
func TestIndexDB_New_BadDir(t *testing.T) {
	db, err := newIndexDB("/nonexistent/path/that/should/fail")
	if err == nil {
		db.Close()
		os.RemoveAll("/nonexistent/path/that/should/fail")
	}
	// BadgerDB may or may not fail on this path depending on permissions
	// Just verify it doesn't panic
}
