package wal

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadFromWriteResultStream_ErrorCases(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := New("", tmpDir)
	require.NoError(t, err)
	defer w.Close()

	t.Run("nil WriteResult", func(t *testing.T) {
		reader, err := w.ReadFromWriteResultStream(nil)
		assert.Nil(t, reader)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nil WriteResult")
	})

	t.Run("no WAL files", func(t *testing.T) {
		reader, err := w.ReadFromWriteResultStream(&WriteResult{})
		assert.Nil(t, reader)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no WAL files")
	})

	t.Run("negative TotalSize", func(t *testing.T) {
		reader, err := w.ReadFromWriteResultStream(&WriteResult{
			TotalSize: -1,
			WALFiles:  []WALFileInfo{{WALNum: 1}},
		})
		assert.Nil(t, reader)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid TotalSize")
	})
}

func TestReadFromWriteResultStream_RoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := New("", tmpDir)
	require.NoError(t, err)
	defer w.Close()

	// Write data
	size := 256*1024 + 123
	origFile := make([]byte, size)
	for i := range origFile {
		origFile[i] = byte((i + size) % 256)
	}

	writeResult, err := w.Write(bytes.NewReader(origFile), len(origFile))
	require.NoError(t, err)
	require.NotNil(t, writeResult)

	// Read via stream
	reader, err := w.ReadFromWriteResultStream(writeResult)
	require.NoError(t, err)
	require.NotNil(t, reader)

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, len(origFile), len(data))
	assert.True(t, bytes.Equal(origFile, data), "Stream data should match original")
}

func TestReadFromWriteResultStream_VariousSizes(t *testing.T) {
	sizes := []struct {
		name string
		size int
	}{
		{"100 bytes", 100},
		{"1KB", 1024},
		{"8192 bytes (exact chunk)", int(ChunkSize)},
		{"8193 bytes (chunk+1)", int(ChunkSize) + 1},
		{"16KB", 16 * 1024},
		{"256KB", 256 * 1024},
	}

	if !testing.Short() {
		sizes = append(sizes, []struct {
			name string
			size int
		}{
			{"ShardSize-1", int(ShardSize) - 1},
			{"ShardSize+1", int(ShardSize) + 1},
			{"4MB", 4 * 1024 * 1024},
		}...)
	}

	for _, tc := range sizes {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			w, err := New("", tmpDir)
			require.NoError(t, err)
			defer w.Close()

			testData := make([]byte, tc.size)
			for i := range testData {
				testData[i] = byte((i + tc.size) % 256)
			}

			writeResult, err := w.Write(bytes.NewReader(testData), tc.size)
			require.NoError(t, err)

			reader, err := w.ReadFromWriteResultStream(writeResult)
			require.NoError(t, err)
			require.NotNil(t, reader)

			data, err := io.ReadAll(reader)
			require.NoError(t, err)
			assert.Equal(t, tc.size, len(data), "Read size should match")
			assert.True(t, bytes.Equal(testData, data), "Data should match original")
		})
	}
}

func TestReadFromWriteResultStream_CorruptedChecksum(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := New("", tmpDir)
	require.NoError(t, err)

	testData := []byte("Hello, World! Stream checksum test.")
	writeResult, err := w.Write(bytes.NewReader(testData), len(testData))
	require.NoError(t, err)

	w.Close()

	// Corrupt the checksum in the WAL file
	walNum := writeResult.WALFiles[0].WALNum
	walFile := filepath.Join(tmpDir, FormatWalFile(walNum))
	f, err := os.OpenFile(walFile, os.O_RDWR, 0640)
	require.NoError(t, err)

	walHeaderSize := (&WAL{}).WALHeaderSize()
	_, err = f.Seek(int64(walHeaderSize), 0)
	require.NoError(t, err)

	headerBuf := make([]byte, 32)
	_, err = f.Read(headerBuf)
	require.NoError(t, err)

	// Corrupt checksum bytes
	headerBuf[28] = ^headerBuf[28]
	headerBuf[29] = ^headerBuf[29]

	_, err = f.Seek(int64(walHeaderSize), 0)
	require.NoError(t, err)
	_, err = f.Write(headerBuf)
	require.NoError(t, err)
	f.Close()

	// Reopen and try to stream - should get error when reading
	w2, err := New("", tmpDir)
	require.NoError(t, err)
	defer w2.Close()

	reader, err := w2.ReadFromWriteResultStream(writeResult)
	require.NoError(t, err) // Stream creation succeeds
	require.NotNil(t, reader)

	_, err = io.ReadAll(reader)
	require.Error(t, err)
	// Error could be "checksum mismatch" or "wrote N bytes but expected M" depending on corruption
	assert.True(t, err != nil, "Should get an error from corrupted WAL data")
}

func TestReadFromWriteResultStream_MissingWALFile(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := New("", tmpDir)
	require.NoError(t, err)

	testData := make([]byte, 1024)
	writeResult, err := w.Write(bytes.NewReader(testData), len(testData))
	require.NoError(t, err)
	w.Close()

	// Delete the WAL file
	walFile := filepath.Join(tmpDir, FormatWalFile(writeResult.WALFiles[0].WALNum))
	os.Remove(walFile)

	w2, err := New("", tmpDir)
	require.NoError(t, err)
	defer w2.Close()

	reader, err := w2.ReadFromWriteResultStream(writeResult)
	require.NoError(t, err)
	require.NotNil(t, reader)

	_, err = io.ReadAll(reader)
	require.Error(t, err)
}

func TestReadFromWriteResultStream_MultipleWALFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-WAL stream test in short mode")
	}

	tmpDir := t.TempDir()
	w, err := New("", tmpDir)
	require.NoError(t, err)
	defer w.Close()

	// Write data large enough to span multiple WAL files
	size := int(ShardSize) + 10000
	testData := make([]byte, size)
	for i := range testData {
		testData[i] = byte((i + size) % 256)
	}

	writeResult, err := w.Write(bytes.NewReader(testData), size)
	require.NoError(t, err)
	require.Greater(t, len(writeResult.WALFiles), 1, "Should span multiple WAL files")

	reader, err := w.ReadFromWriteResultStream(writeResult)
	require.NoError(t, err)
	require.NotNil(t, reader)

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, size, len(data))
	assert.True(t, bytes.Equal(testData, data))
}

func TestReadFromWriteResultStream_EndOfShardMismatch(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := New("", tmpDir)
	require.NoError(t, err)
	defer w.Close()

	testData := make([]byte, 8192)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	writeResult, err := w.Write(bytes.NewReader(testData), 8192)
	require.NoError(t, err)

	// Corrupt TotalSize to be larger than actual data
	corruptedResult := *writeResult
	corruptedResult.TotalSize = 10000

	reader, err := w.ReadFromWriteResultStream(&corruptedResult)
	require.NoError(t, err)

	_, err = io.ReadAll(reader)
	require.Error(t, err)
	// Error will mention end-of-shard mismatch or byte count mismatch
	assert.True(t, err != nil, "Should get an error when TotalSize exceeds actual data")
}

func TestReadFromWriteResultStream_MultipleObjects(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := New("", tmpDir)
	require.NoError(t, err)
	defer w.Close()

	// Write multiple objects and verify each can be streamed independently
	objects := make([]struct {
		data   []byte
		result *WriteResult
	}, 5)

	for i := range objects {
		size := 1024 * (i + 1) // 1KB, 2KB, 3KB, 4KB, 5KB
		objects[i].data = make([]byte, size)
		for j := range objects[i].data {
			objects[i].data[j] = byte((j + size + i) % 256)
		}

		result, err := w.Write(bytes.NewReader(objects[i].data), size)
		require.NoError(t, err)
		objects[i].result = result
	}

	// Read each back via stream
	for i, obj := range objects {
		reader, err := w.ReadFromWriteResultStream(obj.result)
		require.NoError(t, err, "Object %d stream creation", i)

		data, err := io.ReadAll(reader)
		require.NoError(t, err, "Object %d read", i)
		assert.True(t, bytes.Equal(obj.data, data), "Object %d data mismatch", i)
	}
}
