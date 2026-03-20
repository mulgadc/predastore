package distributed

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_Type(t *testing.T) {
	tmpDir := t.TempDir()

	b, err := New(&Config{BadgerDir: tmpDir})
	require.NoError(t, err)
	defer b.Close()

	assert.Equal(t, "distributed", b.(*Backend).Type())
}

func TestBackend_DB_LocalState(t *testing.T) {
	tmpDir := t.TempDir()

	b, err := New(&Config{BadgerDir: tmpDir})
	require.NoError(t, err)
	defer b.Close()

	backend := b.(*Backend)
	db := backend.DB()
	assert.NotNil(t, db, "DB() should return non-nil for LocalState backend")
}

func TestBackend_GlobalState(t *testing.T) {
	tmpDir := t.TempDir()

	b, err := New(&Config{BadgerDir: tmpDir})
	require.NoError(t, err)
	defer b.Close()

	backend := b.(*Backend)
	gs := backend.GlobalState()
	assert.NotNil(t, gs, "GlobalState() should return non-nil")

	// Verify it implements GlobalState interface by performing operations
	err = gs.Set("test-table", []byte("key1"), []byte("value1"))
	require.NoError(t, err)

	val, err := gs.Get("test-table", []byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value1"), val)

	exists, err := gs.Exists("test-table", []byte("key1"))
	require.NoError(t, err)
	assert.True(t, exists)

	exists, err = gs.Exists("test-table", []byte("nonexistent"))
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestBackend_DataDir(t *testing.T) {
	tmpDir := t.TempDir()

	b, err := New(&Config{BadgerDir: tmpDir})
	require.NoError(t, err)
	defer b.Close()

	backend := b.(*Backend)

	customDir := "/tmp/custom-data-dir"
	backend.SetDataDir(customDir)
	assert.Equal(t, customDir, backend.DataDir())
}

func TestBackend_RsShards(t *testing.T) {
	tmpDir := t.TempDir()

	b, err := New(&Config{BadgerDir: tmpDir, DataShards: 3, ParityShards: 2})
	require.NoError(t, err)
	defer b.Close()

	backend := b.(*Backend)
	assert.Equal(t, 3, backend.RsDataShard())
	assert.Equal(t, 2, backend.RsParityShard())
}

func TestBackend_HashRing(t *testing.T) {
	tmpDir := t.TempDir()

	b, err := New(&Config{BadgerDir: tmpDir})
	require.NoError(t, err)
	defer b.Close()

	backend := b.(*Backend)
	assert.NotNil(t, backend.HashRing())
}

func TestBackend_Close(t *testing.T) {
	tmpDir := t.TempDir()

	b, err := New(&Config{BadgerDir: tmpDir})
	require.NoError(t, err)

	err = b.Close()
	assert.NoError(t, err)
}
