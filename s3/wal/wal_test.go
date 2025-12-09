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
