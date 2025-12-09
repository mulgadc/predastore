package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {

	tmpDir := os.TempDir()

	// Remove the previous WAL for tesitng
	os.RemoveAll(filepath.Join(tmpDir, FormatWalFile(1)))

	wal, err := New("", tmpDir)

	t.Log("tmpDir", tmpDir)

	assert.NoError(t, err, "Should read config without error")
	assert.NotNil(t, wal)

	f, err := os.Open("/Users/benduncan/Desktop/vendors-mock.json")
	assert.NoError(t, err, "Could not open file")

	stat, err := os.Stat("/Users/benduncan/Desktop/vendors-mock.json")
	assert.NoError(t, err, "Could not stat file")

	origFile, err := os.ReadFile("/Users/benduncan/Desktop/vendors-mock.json")
	assert.NoError(t, err, "Could not read file")

	wal.Write(f, int(stat.Size()))

	d, err := wal.Read(wal.SeqNum.Load(), wal.ShardNum.Load(), uint32(stat.Size()))

	assert.NoError(t, err, "Read should not error")
	assert.NotEmpty(t, d, "Data empty")

	// Should match original

	assert.True(t, bytes.Equal(origFile, d), "Data should match original")

	// Diff the bytes
	if diff := cmp.Diff(origFile, d); diff != "" {
		t.Errorf("Bytes differ (-want +got):\n%s", diff)
	}

	//fmt.Println(string(d))

	wal.Close()

	//
}

func TestReadChecksumValidation(t *testing.T) {
	tmpDir := os.TempDir()

	// Remove all previous WAL files for testing to ensure clean state
	os.RemoveAll(filepath.Join(tmpDir, FormatWalFile(1)))
	os.RemoveAll(filepath.Join(tmpDir, FormatWalFile(2)))

	wal, err := New("", tmpDir)
	assert.NoError(t, err, "Should create WAL without error")
	assert.NotNil(t, wal)
	assert.Greater(t, len(wal.Shard.DB), 0, "WAL should have at least one file open")

	// Write some test data
	testData := []byte("Hello, World! This is test data for checksum validation.")
	testDataReader := bytes.NewReader(testData)

	_, err = wal.Write(testDataReader, len(testData))
	assert.NoError(t, err, "Write should not error")

	walNum := wal.SeqNum.Load()
	shardNum := wal.ShardNum.Load()

	// First, verify that valid data reads correctly
	data, err := wal.Read(walNum, shardNum, uint32(len(testData)))
	assert.NoError(t, err, "Read should not error with valid checksum")
	assert.True(t, bytes.Equal(testData, data), "Read data should match original")

	// Close the WAL to ensure all data is flushed to disk
	wal.Close()

	// Now corrupt the checksum in the WAL file
	walFile := filepath.Join(tmpDir, FormatWalFile(walNum))
	f, err := os.OpenFile(walFile, os.O_RDWR, 0640)
	assert.NoError(t, err, "Should open WAL file")

	// Skip WAL header
	walHeaderSize := wal.WALHeaderSize()
	_, err = f.Seek(int64(walHeaderSize), 0)
	assert.NoError(t, err, "Should seek past WAL header")

	// Read the first fragment header
	headerBuf := make([]byte, 32)
	_, err = f.Read(headerBuf)
	assert.NoError(t, err, "Should read fragment header")

	// Corrupt the checksum (last 4 bytes of header)
	headerBuf[28] = ^headerBuf[28] // Flip all bits
	headerBuf[29] = ^headerBuf[29]
	headerBuf[30] = ^headerBuf[30]
	headerBuf[31] = ^headerBuf[31]

	// Write the corrupted header back
	_, err = f.Seek(int64(walHeaderSize), 0) // Back to start of first fragment
	assert.NoError(t, err, "Should seek back to fragment start")
	_, err = f.Write(headerBuf)
	assert.NoError(t, err, "Should write corrupted header")
	f.Close()

	// Reopen WAL for reading (since we closed it)
	wal2, err := New("", tmpDir)
	assert.NoError(t, err, "Should reopen WAL without error")

	// Now try to read - should fail with checksum error
	_, err = wal2.Read(walNum, shardNum, uint32(len(testData)))
	assert.Error(t, err, "Read should error with corrupted checksum")
	assert.Contains(t, err.Error(), "checksum mismatch", "Error should mention checksum mismatch")

	wal2.Close()
}

func TestReadChecksumValidationCorruptedData(t *testing.T) {
	tmpDir := os.TempDir()

	// Remove all previous WAL files for testing to ensure clean state
	os.RemoveAll(filepath.Join(tmpDir, FormatWalFile(1)))
	os.RemoveAll(filepath.Join(tmpDir, FormatWalFile(2)))
	os.RemoveAll(filepath.Join(tmpDir, FormatWalFile(3)))

	wal, err := New("", tmpDir)
	assert.NoError(t, err, "Should create WAL without error")
	assert.NotNil(t, wal)
	assert.Greater(t, len(wal.Shard.DB), 0, "WAL should have at least one file open")

	// Write some test data
	testData := []byte("Test data for corruption test.")
	testDataReader := bytes.NewReader(testData)

	_, err = wal.Write(testDataReader, len(testData))
	assert.NoError(t, err, "Write should not error")

	walNum := wal.SeqNum.Load()
	shardNum := wal.ShardNum.Load()

	// Close the WAL to ensure all data is flushed to disk
	wal.Close()

	// Corrupt the data in the WAL file (not the checksum)
	walFile := filepath.Join(tmpDir, FormatWalFile(walNum))
	f, err := os.OpenFile(walFile, os.O_RDWR, 0640)
	assert.NoError(t, err, "Should open WAL file")

	// Skip WAL header + fragment header (32 bytes)
	walHeaderSize := wal.WALHeaderSize()
	fragmentHeaderSize := 32
	_, err = f.Seek(int64(walHeaderSize+fragmentHeaderSize), 0)
	assert.NoError(t, err, "Should seek to data section")

	// Corrupt the first byte of data
	corruptedByte := []byte{0xFF}
	_, err = f.Write(corruptedByte)
	assert.NoError(t, err, "Should write corrupted data")
	f.Close()

	// Reopen WAL for reading (since we closed it)
	wal2, err := New("", tmpDir)
	assert.NoError(t, err, "Should reopen WAL without error")

	// Now try to read - should fail with checksum error because data doesn't match checksum
	_, err = wal2.Read(walNum, shardNum, uint32(len(testData)))
	assert.Error(t, err, "Read should error with corrupted data")
	assert.Contains(t, err.Error(), "checksum mismatch", "Error should mention checksum mismatch")

	wal2.Close()
}
