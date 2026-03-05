package distributed

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mulgadc/predastore/backend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestBackend creates a distributed backend with local WAL (no QUIC) for testing.
func setupTestBackend(t *testing.T) *Backend {
	t.Helper()
	tmpDir, err := os.MkdirTemp(os.TempDir(), fmt.Sprintf("unit-test-%d", time.Now().UnixNano()))
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	cfg := &Config{
		BadgerDir: tmpDir,
		Buckets: []BucketConfig{
			{Name: "test-bucket", Region: "us-east-1"},
		},
	}
	b, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { b.Close() })

	be := b.(*Backend)

	tmp := t.TempDir()
	be.SetDataDir(filepath.Join(tmp, "nodes"))

	for i := range 5 {
		require.NoError(t, os.MkdirAll(filepath.Join(be.DataDir(), fmt.Sprintf("node-%d", i)), 0750))
	}

	return be
}

// putTestObject writes a deterministic object using PutObject (stores ARN key for listing).
func putTestObject(t *testing.T, be *Backend, bucket, key string, size int) []byte {
	t.Helper()
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i + size) % 251)
	}

	// Write to temp file for PutObject
	tmpFile, err := os.CreateTemp("", "test-obj-*")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.Write(data)
	require.NoError(t, err)
	tmpFile.Close()

	ctx := context.Background()
	_, err = be.PutObject(ctx, &backend.PutObjectRequest{
		Bucket: bucket,
		Key:    key,
		Body:   bytes.NewReader(data),
	})
	require.NoError(t, err)

	return data
}

func TestHeadObject(t *testing.T) {
	be := setupTestBackend(t)
	ctx := context.Background()

	t.Run("existing object", func(t *testing.T) {
		orig := putTestObject(t, be, "test-bucket", "head-test.txt", 128*1024)

		resp, err := be.HeadObject(ctx, "test-bucket", "head-test.txt")
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, int64(len(orig)), resp.ContentLength)
		assert.Equal(t, "application/octet-stream", resp.ContentType)
		assert.NotEmpty(t, resp.ETag)
	})

	t.Run("nonexistent object", func(t *testing.T) {
		_, err := be.HeadObject(ctx, "test-bucket", "does-not-exist.txt")
		require.Error(t, err)
	})
}

func TestDeleteObject(t *testing.T) {
	be := setupTestBackend(t)
	ctx := context.Background()

	t.Run("delete existing object", func(t *testing.T) {
		putTestObject(t, be, "test-bucket", "delete-me.txt", 1024)

		// Verify it exists
		_, err := be.HeadObject(ctx, "test-bucket", "delete-me.txt")
		require.NoError(t, err)

		// Delete it
		err = be.DeleteObject(ctx, &backend.DeleteObjectRequest{
			Bucket: "test-bucket",
			Key:    "delete-me.txt",
		})
		require.NoError(t, err)

		// Verify it's gone
		_, err = be.HeadObject(ctx, "test-bucket", "delete-me.txt")
		require.Error(t, err)
	})

	t.Run("delete nonexistent object", func(t *testing.T) {
		err := be.DeleteObject(ctx, &backend.DeleteObjectRequest{
			Bucket: "test-bucket",
			Key:    "ghost.txt",
		})
		require.Error(t, err)
	})

	t.Run("empty bucket name", func(t *testing.T) {
		err := be.DeleteObject(ctx, &backend.DeleteObjectRequest{
			Bucket: "",
			Key:    "file.txt",
		})
		require.Error(t, err)
	})

	t.Run("empty key", func(t *testing.T) {
		err := be.DeleteObject(ctx, &backend.DeleteObjectRequest{
			Bucket: "test-bucket",
			Key:    "",
		})
		require.Error(t, err)
	})
}

func TestListObjects(t *testing.T) {
	be := setupTestBackend(t)
	ctx := context.Background()

	// Put several objects
	putTestObject(t, be, "test-bucket", "dir/file1.txt", 1024)
	putTestObject(t, be, "test-bucket", "dir/file2.txt", 2048)
	putTestObject(t, be, "test-bucket", "other.txt", 512)

	t.Run("list all objects", func(t *testing.T) {
		resp, err := be.ListObjects(ctx, &backend.ListObjectsRequest{
			Bucket: "test-bucket",
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, "test-bucket", resp.Name)
		assert.GreaterOrEqual(t, resp.KeyCount, 3)
	})

	t.Run("list with prefix", func(t *testing.T) {
		resp, err := be.ListObjects(ctx, &backend.ListObjectsRequest{
			Bucket: "test-bucket",
			Prefix: "dir/",
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.GreaterOrEqual(t, resp.KeyCount, 2)
		for _, obj := range resp.Contents {
			assert.True(t, len(obj.Key) >= 4 && obj.Key[:4] == "dir/",
				"Object key %q should start with dir/", obj.Key)
		}
	})

	t.Run("list with delimiter", func(t *testing.T) {
		resp, err := be.ListObjects(ctx, &backend.ListObjectsRequest{
			Bucket:    "test-bucket",
			Delimiter: "/",
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		// Should have common prefix "dir/" and direct object "other.txt"
		assert.GreaterOrEqual(t, len(resp.CommonPrefixes), 1, "Should have at least one common prefix")
	})

	t.Run("empty bucket", func(t *testing.T) {
		_, err := be.ListObjects(ctx, &backend.ListObjectsRequest{
			Bucket: "",
		})
		require.Error(t, err)
	})

	t.Run("nonexistent bucket", func(t *testing.T) {
		_, err := be.ListObjects(ctx, &backend.ListObjectsRequest{
			Bucket: "no-such-bucket",
		})
		require.Error(t, err)
	})
}

func TestLocalState_Operations(t *testing.T) {
	tmpDir := t.TempDir()
	ls, err := NewLocalState(tmpDir)
	require.NoError(t, err)
	defer ls.Close()

	t.Run("Set and Get", func(t *testing.T) {
		require.NoError(t, ls.Set("t1", []byte("k1"), []byte("v1")))
		val, err := ls.Get("t1", []byte("k1"))
		require.NoError(t, err)
		assert.Equal(t, []byte("v1"), val)
	})

	t.Run("Exists", func(t *testing.T) {
		require.NoError(t, ls.Set("t1", []byte("exists-key"), []byte("val")))

		exists, err := ls.Exists("t1", []byte("exists-key"))
		require.NoError(t, err)
		assert.True(t, exists)

		exists, err = ls.Exists("t1", []byte("nope"))
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("Delete", func(t *testing.T) {
		require.NoError(t, ls.Set("t1", []byte("del-key"), []byte("val")))
		require.NoError(t, ls.Delete("t1", []byte("del-key")))

		_, err := ls.Get("t1", []byte("del-key"))
		assert.Error(t, err)
	})

	t.Run("ListKeys", func(t *testing.T) {
		require.NoError(t, ls.Set("t2", []byte("prefix/a"), []byte("va")))
		require.NoError(t, ls.Set("t2", []byte("prefix/b"), []byte("vb")))
		require.NoError(t, ls.Set("t2", []byte("other/x"), []byte("vx")))

		keys, err := ls.ListKeys("t2", []byte("prefix/"))
		require.NoError(t, err)
		assert.Len(t, keys, 2)
	})

	t.Run("Scan", func(t *testing.T) {
		require.NoError(t, ls.Set("t3", []byte("scan/1"), []byte("v1")))
		require.NoError(t, ls.Set("t3", []byte("scan/2"), []byte("v2")))

		var count int
		err := ls.Scan("t3", []byte("scan/"), func(key, value []byte) error {
			count++
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 2, count)
	})

	t.Run("DB accessor", func(t *testing.T) {
		assert.NotNil(t, ls.DB())
	})
}
