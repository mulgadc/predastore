package s3

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCopyFile(t *testing.T) {
	t.Run("basic copy", func(t *testing.T) {
		srcDir := t.TempDir()
		dstDir := t.TempDir()

		srcPath := filepath.Join(srcDir, "source.txt")
		dstPath := filepath.Join(dstDir, "dest.txt")

		content := []byte("hello world")
		require.NoError(t, os.WriteFile(srcPath, content, 0644))

		err := copyFile(srcPath, dstPath)
		require.NoError(t, err)

		got, err := os.ReadFile(dstPath)
		require.NoError(t, err)
		assert.Equal(t, content, got)
	})

	t.Run("creates parent directories", func(t *testing.T) {
		srcDir := t.TempDir()
		dstDir := t.TempDir()

		srcPath := filepath.Join(srcDir, "source.txt")
		dstPath := filepath.Join(dstDir, "sub", "dir", "dest.txt")

		content := []byte("nested directory test")
		require.NoError(t, os.WriteFile(srcPath, content, 0644))

		err := copyFile(srcPath, dstPath)
		require.NoError(t, err)

		got, err := os.ReadFile(dstPath)
		require.NoError(t, err)
		assert.Equal(t, content, got)
	})

	t.Run("source does not exist", func(t *testing.T) {
		dstDir := t.TempDir()
		err := copyFile("/nonexistent/file.txt", filepath.Join(dstDir, "dest.txt"))
		require.Error(t, err)
	})

	t.Run("empty file", func(t *testing.T) {
		srcDir := t.TempDir()
		dstDir := t.TempDir()

		srcPath := filepath.Join(srcDir, "empty.txt")
		dstPath := filepath.Join(dstDir, "empty_copy.txt")

		require.NoError(t, os.WriteFile(srcPath, []byte{}, 0644))

		err := copyFile(srcPath, dstPath)
		require.NoError(t, err)

		info, err := os.Stat(dstPath)
		require.NoError(t, err)
		assert.Equal(t, int64(0), info.Size())
	})

	t.Run("large file", func(t *testing.T) {
		srcDir := t.TempDir()
		dstDir := t.TempDir()

		srcPath := filepath.Join(srcDir, "large.bin")
		dstPath := filepath.Join(dstDir, "large_copy.bin")

		content := make([]byte, 1024*1024) // 1MB
		for i := range content {
			content[i] = byte(i % 256)
		}
		require.NoError(t, os.WriteFile(srcPath, content, 0644))

		err := copyFile(srcPath, dstPath)
		require.NoError(t, err)

		got, err := os.ReadFile(dstPath)
		require.NoError(t, err)
		assert.Equal(t, content, got)
	})
}
