package wal

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFragment_FragmentHeader(t *testing.T) {
	f := &Fragment{
		SeqNum:        1,
		ShardNum:      2,
		ShardFragment: 3,
		Length:        1024,
		Flags:         FlagEndOfShard,
	}

	header := f.FragmentHeader()
	assert.Len(t, header, FragmentHeaderBytes)

	// Verify fields are encoded correctly in big endian
	assert.Equal(t, uint64(1), binary.BigEndian.Uint64(header[0:8]))                // SeqNum
	assert.Equal(t, uint64(2), binary.BigEndian.Uint64(header[8:16]))               // ShardNum
	assert.Equal(t, uint32(3), binary.BigEndian.Uint32(header[16:20]))              // ShardFragment
	assert.Equal(t, uint32(1024), binary.BigEndian.Uint32(header[20:24]))           // Length
	assert.Equal(t, uint32(FlagEndOfShard), binary.BigEndian.Uint32(header[24:28])) // Flags
}

func TestFragment_FragmentHeader_ZeroValues(t *testing.T) {
	f := &Fragment{}
	header := f.FragmentHeader()
	assert.Len(t, header, FragmentHeaderBytes)

	// All fields should be zero
	for i := range 28 {
		assert.Equal(t, byte(0), header[i], "byte %d should be zero", i)
	}
}

func TestFragment_AppendChecksum(t *testing.T) {
	f := &Fragment{
		Checksum: 0xDEADBEEF,
	}

	// Create a payload with at least 32 bytes (header size)
	payload := make([]byte, FragmentHeaderBytes+100)

	result := f.AppendChecksum(payload)
	assert.Equal(t, payload, result) // Should return same slice

	// Verify checksum is at bytes 28-32
	checksum := binary.BigEndian.Uint32(result[28:32])
	assert.Equal(t, uint32(0xDEADBEEF), checksum)
}

func TestFragment_FragmentHeaderSize(t *testing.T) {
	f := &Fragment{}
	assert.Equal(t, FragmentHeaderBytes, f.FragmentHeaderSize())
}

func TestFragment_FragmentHeader_AllFlags(t *testing.T) {
	f := &Fragment{
		SeqNum:        100,
		ShardNum:      200,
		ShardFragment: 5,
		Length:        ChunkSize,
		Flags:         FlagEndOfShard | FlagShardHeader | FlagCompressed,
	}

	header := f.FragmentHeader()
	flags := binary.BigEndian.Uint32(header[24:28])
	assert.Equal(t, uint32(FlagEndOfShard|FlagShardHeader|FlagCompressed), flags)
}

func TestUpdateObjectToWAL(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := New("", tmpDir)
	require.NoError(t, err)
	defer w.Close()

	hash := [32]byte{1, 2, 3}
	result := &WriteResult{
		ShardNum:  1,
		TotalSize: 100,
		WALFiles:  []WALFileInfo{{WALNum: 1, Offset: 0, Size: 100}},
	}

	err = w.UpdateObjectToWAL(hash, result)
	require.NoError(t, err)

	// Verify it's stored in DB
	val, err := w.DB.Get(hash[:])
	require.NoError(t, err)
	assert.NotEmpty(t, val)
}

func TestReadFromWriteResultStream_NilResult(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := New("", tmpDir)
	require.NoError(t, err)
	defer w.Close()

	reader, err := w.ReadFromWriteResultStream(nil)
	assert.Nil(t, reader)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil WriteResult")
}

func TestReadFromWriteResultStream_NoWALFiles(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := New("", tmpDir)
	require.NoError(t, err)
	defer w.Close()

	reader, err := w.ReadFromWriteResultStream(&WriteResult{})
	assert.Nil(t, reader)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no WAL files")
}

func TestReadFromWriteResultStream_NegativeTotalSize(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := New("", tmpDir)
	require.NoError(t, err)
	defer w.Close()

	reader, err := w.ReadFromWriteResultStream(&WriteResult{
		TotalSize: -1,
		WALFiles:  []WALFileInfo{{WALNum: 1}},
	})
	assert.Nil(t, reader)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid TotalSize")
}

func TestUpdateShardToWAL(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := New("", tmpDir)
	require.NoError(t, err)
	defer w.Close()

	hash := [32]byte{4, 5, 6}
	shardKey := []byte("shard-key-001")
	result := &WriteResult{
		ShardNum:  2,
		TotalSize: 200,
		WALFiles:  []WALFileInfo{{WALNum: 1, Offset: 14, Size: 200}},
	}

	err = w.UpdateShardToWAL(shardKey, hash, result)
	require.NoError(t, err)

	// Verify stored
	val, err := w.DB.Get(shardKey)
	require.NoError(t, err)
	assert.NotEmpty(t, val)
}
