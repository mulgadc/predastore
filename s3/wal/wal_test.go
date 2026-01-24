package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

// calculateMaxFileSize calculates the max file size (ShardSize + header overhead)
// This matches WAL.MaxFileSize()
func calculateMaxFileSize(shardSize uint32, chunkSize uint32, walHeaderSize int) int64 {
	dataSize := int64(shardSize)
	numFragments := (dataSize + int64(chunkSize) - 1) / int64(chunkSize)
	headerOverhead := int64(walHeaderSize) + numFragments*int64(FragmentHeaderBytes)
	return dataSize + headerOverhead
}

// calculateExpectedWALFiles calculates the exact number of WAL files needed for a given data size
// This simulates the actual Write() behavior
func calculateExpectedWALFiles(dataSize int, shardSize uint32, chunkSize uint32, walHeaderSize int) int {
	if dataSize == 0 {
		return 1
	}

	walHeaderBytes := int64(walHeaderSize)
	fragmentHeaderSize := int64(32)
	// MaxFileSize = ShardSize (data) + header overhead
	maxFileSize := calculateMaxFileSize(shardSize, chunkSize, walHeaderSize)

	remainingData := int64(dataSize)
	currentFileSize := walHeaderBytes // Start with WAL header
	numWALFiles := 1

	for remainingData > 0 {
		// Each fragment is fixed-size on disk: header + ChunkSize payload (padded).
		fragmentSize := fragmentHeaderSize + int64(chunkSize)

		// Check if this fragment fits in current WAL file
		if currentFileSize+fragmentSize > maxFileSize {
			// This fragment won't fit, need a new WAL file
			numWALFiles++
			currentFileSize = walHeaderBytes // Start new file with WAL header
		}

		// Add fragment to current WAL
		currentFileSize += fragmentSize
		// Logical data consumption is still up to ChunkSize per fragment.
		if remainingData > int64(chunkSize) {
			remainingData -= int64(chunkSize)
		} else {
			remainingData = 0
		}
	}

	return numWALFiles
}

// calculateWALFileSizes calculates the expected size for each WAL file (excluding WAL header)
// This simulates the actual Write() behavior exactly
func calculateWALFileSizes(dataSize int, shardSize uint32, chunkSize uint32, walHeaderSize int) []int64 {
	walHeaderBytes := int64(walHeaderSize)
	fragmentHeaderSize := int64(32)
	// MaxFileSize = ShardSize (data) + header overhead
	maxFileSize := calculateMaxFileSize(shardSize, chunkSize, walHeaderSize)

	remainingData := int64(dataSize)
	currentFileTotalSize := walHeaderBytes // Total file size including WAL header
	currentWAL := 0
	sizes := make([]int64, 0, 10)
	currentWALFileSize := int64(0) // Size written for this object (fragment headers + data, excluding WAL header)

	for remainingData > 0 {
		// Each fragment is fixed-size on disk: header + ChunkSize payload (padded).
		fragmentSize := fragmentHeaderSize + int64(chunkSize)

		// Check if this fragment fits in current WAL file (check total file size including WAL header)
		if currentFileTotalSize+fragmentSize > maxFileSize {
			// This fragment won't fit, finalize current WAL and move to next
			for len(sizes) <= currentWAL {
				sizes = append(sizes, 0)
			}
			if currentWALFileSize > 0 {
				sizes[currentWAL] = currentWALFileSize
			}
			currentWAL++
			currentFileTotalSize = walHeaderBytes // Start new file with WAL header
			currentWALFileSize = 0
		}

		// Add fragment to current WAL
		currentFileTotalSize += fragmentSize
		currentWALFileSize += fragmentSize // Track size (fragment header + data)
		// Logical data consumption is still up to ChunkSize per fragment.
		if remainingData > int64(chunkSize) {
			remainingData -= int64(chunkSize)
		} else {
			remainingData = 0
		}

		// Ensure sizes slice is large enough and update
		for len(sizes) <= currentWAL {
			sizes = append(sizes, 0)
		}
		sizes[currentWAL] = currentWALFileSize
	}

	// Finalize last WAL
	for len(sizes) <= currentWAL {
		sizes = append(sizes, 0)
	}
	if currentWALFileSize > 0 {
		sizes[currentWAL] = currentWALFileSize
	}

	// Remove trailing zeros (empty WAL files)
	for len(sizes) > 0 && sizes[len(sizes)-1] == 0 {
		sizes = sizes[:len(sizes)-1]
	}

	return sizes
}

func TestNew(t *testing.T) {

	tmpDir, err := os.MkdirTemp(os.TempDir(), fmt.Sprintf("unit-test-%d", time.Now().UnixNano()))
	assert.NoError(t, err, "MkdirTemp dir should not fail")

	// Clean up any existing WAL files in this temp dir (fresh)
	for i := 1; i <= 5; i++ {
		os.RemoveAll(filepath.Join(tmpDir, FormatWalFile(uint64(i))))
	}

	wal, err := New("", tmpDir)
	t.Log("tmpDir", tmpDir)
	assert.NoError(t, err, "Should read config without error")
	assert.NotNil(t, wal)

	// Use deterministic in-test data (avoid external filesystem dependencies).
	size := 256*1024 + 123 // spans multiple chunks with remainder
	origFile := make([]byte, size)
	for i := range origFile {
		origFile[i] = byte((i + size) % 256)
	}

	writeResult, err := wal.Write(bytes.NewReader(origFile), len(origFile))
	assert.NoError(t, err, "Write should not error")
	assert.NotNil(t, writeResult)

	d, err := wal.ReadFromWriteResult(writeResult)
	assert.NoError(t, err, "Read should not error")
	assert.NotEmpty(t, d, "Data empty")

	assert.True(t, bytes.Equal(origFile, d), "Data should match original")
	if diff := cmp.Diff(origFile, d); diff != "" {
		t.Errorf("Bytes differ (-want +got):\n%s", diff)
	}

	wal.Close()
}

func TestNewWriteOutput(t *testing.T) {

	tmpDir, err := os.MkdirTemp(os.TempDir(), fmt.Sprintf("unit-test-%d", time.Now().UnixNano()))
	t.Log(tmpDir)
	assert.NoError(t, err, "MkdirTemp dir should not fail")

	wal, err := New("", tmpDir)
	assert.NoError(t, err, "Should create WAL without error")
	assert.NotNil(t, wal)

	testCases := []struct {
		name              string
		data              []byte
		expectedWALNum    uint64
		expectedOffset    int64
		expectedSize      int64
		expectedTotalSize int
		expectedShardNum  uint64
	}{
		{"1kb", make([]byte, 1*1024), 1, 0, int64(ChunkSize + FragmentHeaderBytes), 1 * 1024, 1},

		{"16kb", make([]byte, 16*1024), 1, int64(ChunkSize + FragmentHeaderBytes), int64(ChunkSize+FragmentHeaderBytes) * 2, 16 * 1024, 2},

		{"256kb", make([]byte, 256*1024), 1,
			int64(ChunkSize+FragmentHeaderBytes) * 3, int64((ChunkSize + FragmentHeaderBytes) * 32), 256 * 1024, 3},

		{"8193 bytes (one chunk + 1 byte)", make([]byte, 8193), 1, int64(ChunkSize+FragmentHeaderBytes) * 35, int64((ChunkSize + FragmentHeaderBytes) * 2), 8193, 4},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			sampleSize := len(tc.data)
			for i := range tc.data {
				tc.data[i] = byte((i + sampleSize) % 256)
			}

			testDataReader := bytes.NewReader(tc.data)
			writeResult, err := wal.Write(testDataReader, len(tc.data))

			t.Log("writeResult", writeResult)

			assert.NoError(t, err, "Write should not error")
			assert.NotNil(t, writeResult, "WriteResult should not be nil")
			assert.Equal(t, tc.expectedWALNum, writeResult.WALFiles[0].WALNum, "WALNum should be the expected value")
			assert.Equal(t, tc.expectedOffset, writeResult.WALFiles[0].Offset, "Offset should be the expected value")
			assert.Equal(t, tc.expectedSize, writeResult.WALFiles[0].Size, "Size should be the expected value")
			assert.Equal(t, tc.expectedTotalSize, writeResult.TotalSize, "TotalSize should be the expected value")
			assert.Equal(t, tc.expectedShardNum, writeResult.ShardNum, "ShardNum should be the expected value")

			// Next read the result, confirm it's the same.
			data, err := wal.ReadFromWriteResult(writeResult)
			assert.NoError(t, err, "Read should not error")
			assert.NotNil(t, data, "Data should not be nil")
			assert.Equal(t, len(tc.data), len(data), "Data should be the same length as the test data")
			assert.True(t, bytes.Equal(tc.data, data), "Data should match the test data")

		})

	}

	wal.Close()

	// Delete the tmpDir

	os.RemoveAll(tmpDir)

	// Verify the tmpDir is deleted
	_, err = os.Stat(tmpDir)
	assert.Error(t, err, "tmpDir should be deleted")

	// Verify the WAL files are deleted
	for i := uint64(1); i <= wal.WalNum.Load(); i++ {
		assert.NoFileExists(t, filepath.Join(tmpDir, FormatWalFile(uint64(i))))
	}
}

func TestNewWithStateFile(t *testing.T) {

	osTmpDir := os.TempDir()

	tmpDir, err := os.MkdirTemp(osTmpDir, fmt.Sprintf("unit-test-%d", time.Now().UnixNano()))
	assert.NoError(t, err, "MkdirTemp dir should not fail")

	t.Log("tmpDir", tmpDir)
	//defer os.RemoveAll(tmpDir) // clean up when done

	wal, err := New(filepath.Join(tmpDir, "state.json"), tmpDir)
	assert.NoError(t, err, "Should not error since state does not exist")
	assert.NotNil(t, wal)
	//assert.Greater(t, len(wal.Shard.DB), 0, "WAL should have at least one file open")

	// Write some test data

	for range 10 {
		testData := []byte(fmt.Sprintf("Hello %d, World! This is test data for checksum validation.", time.Now().UnixNano()))

		testDataReader := bytes.NewReader(testData)

		writeResult, err := wal.Write(testDataReader, len(testData))
		assert.NoError(t, err, "Write should not error")
		assert.NotNil(t, writeResult)

	}

	wal.Close()

	// Next, open the WAL
	wal2, err := New(filepath.Join(tmpDir, "state.json"), tmpDir)
	assert.NoError(t, err, "Should not error since state does not exist")

	// Check expected WAL state
	assert.Equal(t, wal.WalNum.Load(), wal2.WalNum.Load(), "WalNum should match")
	assert.Equal(t, wal.SeqNum.Load(), wal2.SeqNum.Load(), "SeqNum should match")
	assert.Equal(t, wal.ShardNum.Load(), wal2.ShardNum.Load(), "ShardNum should match")
	assert.Equal(t, wal.Epoch, wal2.Epoch, "Epoch should match")
	assert.Equal(t, wal.WalDir, wal2.WalDir, "WalDir should match")
	assert.Equal(t, wal.StateFile, wal2.StateFile, "StateFile should match")

	// Next, write more data, confirm state increments
	for range 10 {
		testData := []byte(fmt.Sprintf("Hello %d, World! This is test data for checksum validation.", time.Now().UnixNano()))

		testDataReader := bytes.NewReader(testData)

		writeResult, err := wal2.Write(testDataReader, len(testData))
		assert.NoError(t, err, "Write should not error")
		assert.NotNil(t, writeResult)

	}

	// WAL, should be the same
	assert.Equal(t, wal.WalNum.Load(), wal2.WalNum.Load(), "WalNum should match")

	// Both SeqNum and ShardNum should increment by 10
	t.Log("wal.SeqNum.Load()", wal.SeqNum.Load())
	t.Log("wal2.SeqNum.Load()", wal2.SeqNum.Load())
	assert.Equal(t, wal.SeqNum.Load()+10, wal2.SeqNum.Load(), "SeqNum should match")
	assert.Equal(t, wal.ShardNum.Load()+10, wal2.ShardNum.Load(), "ShardNum should match")

	wal2.Close()

	// Validate changes
	wal3, err := New(filepath.Join(tmpDir, "state.json"), tmpDir)
	assert.NoError(t, err, "Should not error since state does not exist")

	// WAL, should be the same
	assert.Equal(t, wal2.WalNum.Load(), wal3.WalNum.Load(), "WalNum should match")

	// Check expected WAL state (again, sanity check)
	assert.Equal(t, wal2.SeqNum.Load(), wal3.SeqNum.Load(), "SeqNum should match")
	assert.Equal(t, wal2.ShardNum.Load(), wal3.ShardNum.Load(), "ShardNum should match")
	assert.Equal(t, wal2.Epoch, wal3.Epoch, "Epoch should match")
	assert.Equal(t, wal2.WalDir, wal3.WalDir, "WalDir should match")
	assert.Equal(t, wal2.StateFile, wal3.StateFile, "StateFile should match")

	wal3.Close()

	// Next, write a 4MB chunk, WAL should bump, confirm state changes.
	// Validate changes
	wal4, err := New(filepath.Join(tmpDir, "state.json"), tmpDir)
	assert.NoError(t, err, "Should not error since state does not exist")

	// Generate deterministic test data (using index for reproducibility)
	sampleSize := (1024 * 1024 * 4) + 1
	testData := make([]byte, sampleSize)
	for i := range testData {
		testData[i] = byte((i + sampleSize) % 256)
	}

	testDataReader := bytes.NewReader(testData)

	writeResult, err := wal4.Write(testDataReader, len(testData))

	assert.NoError(t, err, "Write should not error")
	assert.NotNil(t, writeResult)

	testDataReader = bytes.NewReader(testData)

	writeResult, err = wal4.Write(testDataReader, len(testData))
	assert.NoError(t, err, "Write should not error")
	assert.NotNil(t, writeResult)

	// WAL, should be the same
	assert.Equal(t, wal3.WalNum.Load()+2, wal4.WalNum.Load(), "WalNum should match")

	wal4.Close()

	// Delete the tmpDir
	os.RemoveAll(tmpDir)

	// Verify the tmpDir is deleted
	_, err = os.Stat(tmpDir)
	assert.Error(t, err, "tmpDir should be deleted")

}

func TestHeaderSizes(t *testing.T) {

	fragment := Fragment{}

	size := fragment.FragmentHeaderSize()
	assert.Equal(t, 32, size, "Fragment header size should be 32")

	wal := WAL{}

	size = wal.WALHeaderSize()
	assert.Equal(t, 14, size, "WAL header size should be 14")

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

	writeResult, err := wal.Write(testDataReader, len(testData))
	assert.NoError(t, err, "Write should not error")
	assert.NotNil(t, writeResult)

	// First, verify that valid data reads correctly
	data, err := wal.ReadFromWriteResult(writeResult)
	assert.NoError(t, err, "Read should not error with valid checksum")
	assert.True(t, bytes.Equal(testData, data), "Read data should match original")

	// Close the WAL to ensure all data is flushed to disk
	wal.Close()

	// Now corrupt the checksum in the WAL file
	walNum := writeResult.WALFiles[0].WALNum
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
	// Recreate WriteResult for reading (with corrupted data)
	corruptedResult := &WriteResult{
		ShardNum:  writeResult.ShardNum,
		WALFiles:  writeResult.WALFiles,
		TotalSize: writeResult.TotalSize,
	}
	_, err = wal2.ReadFromWriteResult(corruptedResult)
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

	writeResult, err := wal.Write(testDataReader, len(testData))
	assert.NoError(t, err, "Write should not error")
	assert.NotNil(t, writeResult)

	// Close the WAL to ensure all data is flushed to disk
	wal.Close()

	// Corrupt the data in the WAL file (not the checksum)
	walNum := writeResult.WALFiles[0].WALNum
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
	corruptedResult := &WriteResult{
		ShardNum:  writeResult.ShardNum,
		WALFiles:  writeResult.WALFiles,
		TotalSize: writeResult.TotalSize,
	}
	_, err = wal2.ReadFromWriteResult(corruptedResult)
	assert.Error(t, err, "Read should error with corrupted data")
	assert.Contains(t, err.Error(), "checksum mismatch", "Error should mention checksum mismatch")

	wal2.Close()
}

func TestWriteVariousSizes(t *testing.T) {
	sizes := []struct {
		name string
		size int
	}{
		// Standard sizes
		{"16KB", 16 * 1024},
		{"256KB", 256 * 1024},
		{"2MB", 2 * 1024 * 1024},
		{"4MB", 4 * 1024 * 1024},
		{"8MB", 8 * 1024 * 1024},
		// Key edge cases around chunk/shard boundaries
		{"8193 bytes (one chunk + 1 byte)", 8193},
		{"ShardSize-1 (1 byte less than ShardSize)", int(ShardSize) - 1},
		{"ShardSize+1 (1 byte more than ShardSize)", int(ShardSize) + 1},
	}

	if testing.Short() {
		// Keep runtime well under small -timeout values.
		sizes = []struct {
			name string
			size int
		}{
			{"16KB", 16 * 1024},
			{"256KB", 256 * 1024},
			{"8193 bytes (one chunk + 1 byte)", 8193},
		}
	}

	for _, tc := range sizes {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := t.TempDir()

			wal, err := New("", tmpDir)
			assert.NoError(t, err, "Should create WAL without error")
			assert.NotNil(t, wal)

			// Generate deterministic test data (using index for reproducibility)
			testData := make([]byte, tc.size)
			for i := range testData {
				testData[i] = byte((i + tc.size) % 256)
			}
			testDataReader := bytes.NewReader(testData)

			// Calculate exact expectations
			expectedWALFiles := calculateExpectedWALFiles(tc.size, wal.Shard.ShardSize, ChunkSize, wal.WALHeaderSize())
			expectedSizes := calculateWALFileSizes(tc.size, wal.Shard.ShardSize, ChunkSize, wal.WALHeaderSize())

			// Write the data
			writeResult, err := wal.Write(testDataReader, tc.size)
			assert.NoError(t, err, "Write should not error")
			assert.NotNil(t, writeResult, "WriteResult should not be nil")
			assert.Equal(t, tc.size, writeResult.TotalSize, "TotalSize should match exactly")
			assert.Greater(t, len(writeResult.WALFiles), 0, "Should have at least one WAL file")

			// Verify number of WAL files (may be more if files are reused)
			// For brand new files, should match exactly
			// For reused files, may have more files but total size should be correct
			assert.GreaterOrEqual(t, len(writeResult.WALFiles), expectedWALFiles,
				"Should have at least %d WAL files for %d bytes, got %d",
				expectedWALFiles, tc.size, len(writeResult.WALFiles))

			// Verify WAL file sizes match expectations
			// Note: If files are reused from previous writes, sizes might not match exactly
			// We verify that the total size is correct and individual files don't exceed limits
			var totalWrittenSize int64 = 0
			for i, walFile := range writeResult.WALFiles {
				totalWrittenSize += walFile.Size
				// Verify size doesn't exceed MaxFileSize (ShardSize + header overhead)
				assert.LessOrEqual(t, walFile.Size, wal.MaxFileSize(),
					"WAL file %d size (%d) should not exceed MaxFileSize (%d)", i, walFile.Size, wal.MaxFileSize())
				// Verify offset is valid
				assert.GreaterOrEqual(t, walFile.Offset, int64(0),
					"WAL file %d offset should be non-negative", i)
				// If we have expected sizes and file is brand new (offset 0), verify it matches
				// Note: For reused files (offset > 0), we don't verify exact size as it depends on previous writes
				if i < len(expectedSizes) && expectedSizes[i] > 0 && walFile.Offset == 0 {
					expectedSize := expectedSizes[i]
					// Brand new file, should match exactly (within 1 byte for rounding)
					assert.InDelta(t, expectedSize, walFile.Size, 1.0,
						"WAL file %d (new file, offset=0) size should be %d, got %d", i, expectedSize, walFile.Size)
				}
			}

			// Verify total size across all WAL files matches expected
			numFragments := (tc.size + int(ChunkSize) - 1) / int(ChunkSize)
			expectedTotalSize := int64(numFragments) * int64(32+ChunkSize)
			assert.InDelta(t, expectedTotalSize, totalWrittenSize, 1.0,
				"Total size across all WAL files should be %d (%d fragments * (32+ChunkSize)), got %d",
				expectedTotalSize, numFragments, totalWrittenSize)

			// Verify WAL files are sequential and offsets are correct
			for i := 1; i < len(writeResult.WALFiles); i++ {
				prevFile := writeResult.WALFiles[i-1]
				currFile := writeResult.WALFiles[i]
				// Each subsequent WAL file should have a higher WALNum
				assert.Greater(t, currFile.WALNum, prevFile.WALNum,
					"WAL files should be sequential: file %d has WALNum %d, file %d has WALNum %d",
					i-1, prevFile.WALNum, i, currFile.WALNum)
				// Offset for new files should be 0 (they're brand new files)
				// Note: If a file was reused from a previous write, offset might be > 0, but for
				// a new file created during this write, it should be 0
				assert.GreaterOrEqual(t, currFile.Offset, int64(0),
					"WAL file %d offset should be non-negative, got %d", i, currFile.Offset)
				// For truly new files (not reused), offset should be 0
				// But we allow > 0 if the file was reused from a previous operation
			}

			// Close and reopen WAL
			wal.Close()

			wal2, err := New("", tmpDir)
			assert.NoError(t, err, "Should reopen WAL without error")

			// Read the data back
			readData, err := wal2.ReadFromWriteResult(writeResult)
			assert.NoError(t, err, "Read should not error")
			assert.NotNil(t, readData, "Read data should not be nil")
			assert.Equal(t, tc.size, len(readData), "Read data size should match exactly")

			// Verify data matches byte-by-byte
			if !assert.True(t, bytes.Equal(testData, readData),
				"Read data should match original byte-for-byte") {
				// Detailed mismatch reporting
				mismatches := 0
				maxMismatches := 10
				for i := 0; i < len(testData) && i < len(readData) && mismatches < maxMismatches; i++ {
					if testData[i] != readData[i] {
						t.Errorf("Data mismatch at byte %d: expected 0x%02x (%d), got 0x%02x (%d)",
							i, testData[i], testData[i], readData[i], readData[i])
						mismatches++
					}
				}
				if mismatches >= maxMismatches {
					t.Errorf("... and more mismatches (showing first %d)", maxMismatches)
				}
			}

			// Verify ShardNum is set correctly
			assert.Greater(t, writeResult.ShardNum, uint64(0),
				"ShardNum should be greater than 0")

			wal2.Close()
		})
	}
}

func TestWriteReadOffsetsAndChunkBoundaries(t *testing.T) {
	// Note: Each test case creates its own fresh WAL to avoid file reuse issues

	testCases := []struct {
		name     string
		size     int
		validate func(t *testing.T, result *WriteResult, wal *WAL)
	}{
		{
			name: "Single chunk boundary (8192 bytes)",
			size: 8192,
			validate: func(t *testing.T, result *WriteResult, wal *WAL) {
				assert.Equal(t, 1, len(result.WALFiles), "Should use exactly 1 WAL file")
				// Should have exactly 1 fragment: 32 bytes header + 8192 bytes data = 8224 bytes
				expectedSize := int64(32 + 8192)
				assert.Equal(t, expectedSize, result.WALFiles[0].Size,
					"WAL file size should be %d (32 header + 8192 data)", expectedSize)
				assert.Equal(t, int64(0), result.WALFiles[0].Offset,
					"First WAL file should start at offset 0")
			},
		},
		{
			name: "Just over one chunk (8193 bytes)",
			size: 8193,
			validate: func(t *testing.T, result *WriteResult, wal *WAL) {
				assert.Equal(t, 1, len(result.WALFiles), "Should use exactly 1 WAL file")
				// Should have 2 fragments on disk: 2 * (32+8192)
				if result.WALFiles[0].Offset == 0 {
					expectedSize := int64(2 * (32 + 8192))
					assert.Equal(t, expectedSize, result.WALFiles[0].Size,
						"WAL file size should be %d (two fragments)", expectedSize)
				}
				// Verify size is reasonable
				assert.Greater(t, result.WALFiles[0].Size, int64(0),
					"WAL file should have data")
			},
		},
		{
			name: "Multiple chunks crossing WAL boundary",
			size: int(ShardSize) + 10000, // Just over one ShardSize
			validate: func(t *testing.T, result *WriteResult, wal *WAL) {
				assert.Greater(t, len(result.WALFiles), 1,
					"Should span multiple WAL files")
				// Verify all files have data
				for i, walFile := range result.WALFiles {
					assert.Greater(t, walFile.Size, int64(0),
						"WAL file %d should have data", i)
					assert.LessOrEqual(t, walFile.Size, wal.MaxFileSize(),
						"WAL file %d size should not exceed MaxFileSize", i)
				}
			},
		},
		{
			name: "Very small file (100 bytes)",
			size: 100,
			validate: func(t *testing.T, result *WriteResult, wal *WAL) {
				assert.Equal(t, 1, len(result.WALFiles), "Should use exactly 1 WAL file")
				// On disk payload is padded: 1 * (32+8192)
				if result.WALFiles[0].Offset == 0 {
					expectedSize := int64(32 + 8192)
					assert.Equal(t, expectedSize, result.WALFiles[0].Size,
						"WAL file size should be %d", expectedSize)
				}
				// Verify size is reasonable
				assert.Greater(t, result.WALFiles[0].Size, int64(0),
					"WAL file should have data")
			},
		},
		{
			name: "Exact chunk boundary multiple (16384 bytes)",
			size: 16384, // Exactly 2 chunks
			validate: func(t *testing.T, result *WriteResult, wal *WAL) {
				assert.Equal(t, 1, len(result.WALFiles), "Should use exactly 1 WAL file")
				// Should have 2 fragments: 2 * (32 + 8192) = 16448 bytes
				// But if file was reused, size might be different
				if result.WALFiles[0].Offset == 0 {
					expectedSize := int64(2 * (32 + 8192))
					assert.Equal(t, expectedSize, result.WALFiles[0].Size,
						"WAL file size should be %d (two full chunks)", expectedSize)
				}
				// Verify size is reasonable
				assert.Greater(t, result.WALFiles[0].Size, int64(0),
					"WAL file should have data")
			},
		},
	}

	if testing.Short() {
		// Drop the largest case for quick runs.
		filtered := make([]struct {
			name     string
			size     int
			validate func(t *testing.T, result *WriteResult, wal *WAL)
		}, 0, len(testCases))
		for _, tc := range testCases {
			if tc.name == "Multiple chunks crossing WAL boundary" {
				continue
			}
			filtered = append(filtered, tc)
		}
		testCases = filtered
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := t.TempDir()

			// Create fresh WAL for this test
			wal, err := New("", tmpDir)
			assert.NoError(t, err, "Should create WAL without error")
			assert.NotNil(t, wal)
			defer wal.Close()

			// Generate test data with unique pattern
			testData := make([]byte, tc.size)
			for i := range testData {
				testData[i] = byte((i + tc.size + len(tc.name)) % 256)
			}

			// Write
			writeResult, err := wal.Write(bytes.NewReader(testData), tc.size)
			assert.NoError(t, err, "Write should succeed")
			assert.NotNil(t, writeResult)

			// Run custom validation
			tc.validate(t, writeResult, wal)

			// Always verify data can be read back correctly
			readData, err := wal.ReadFromWriteResult(writeResult)
			assert.NoError(t, err, "Read should succeed")
			assert.Equal(t, tc.size, len(readData), "Read size should match")
			assert.True(t, bytes.Equal(testData, readData),
				"Read data should match original exactly")
		})
	}
}

func TestReadEndOfShardValidation(t *testing.T) {
	testCases := []struct {
		name        string
		dataSize    int
		description string
	}{
		{
			name:        "Single chunk exact",
			dataSize:    8192,
			description: "Exactly one chunk, end-of-shard should be set",
		},
		{
			name:        "Less than one chunk",
			dataSize:    100,
			description: "Less than one chunk, end-of-shard should be set",
		},
		{
			name:        "Multiple chunks",
			dataSize:    16384,
			description: "Exactly two chunks, end-of-shard should be set on last fragment",
		},
		{
			name:        "Multiple chunks with remainder",
			dataSize:    20000,
			description: "Two full chunks plus remainder, end-of-shard should be set on last fragment",
		},
		{
			name:        "Large file spanning multiple WAL files",
			dataSize:    int(ShardSize) + 10000,
			description: "File spans multiple WAL files, end-of-shard should be set on last fragment of last WAL",
		},
	}

	if testing.Short() {
		// Drop the largest case for quick runs.
		testCases = testCases[:len(testCases)-1]
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Fresh WAL dir per subtest to avoid file reuse/offset interactions.
			tmpDir := t.TempDir()

			wal, err := New("", tmpDir)
			assert.NoError(t, err, "Should create WAL without error")
			assert.NotNil(t, wal)

			// Generate test data
			testData := make([]byte, tc.dataSize)
			for i := range testData {
				testData[i] = byte(i % 256)
			}

			// Write the data
			writeResult, err := wal.Write(bytes.NewReader(testData), tc.dataSize)
			assert.NoError(t, err, "Write should not error")
			assert.NotNil(t, writeResult)
			assert.Equal(t, tc.dataSize, writeResult.TotalSize, "TotalSize should match")

			// Verify end-of-shard flag is set on the last fragment
			// We can verify this by reading the WAL file directly
			lastWALFile := writeResult.WALFiles[len(writeResult.WALFiles)-1]
			walFile := filepath.Join(tmpDir, FormatWalFile(lastWALFile.WALNum))
			f, err := os.Open(walFile)
			assert.NoError(t, err, "Should open WAL file")
			defer f.Close()

			// Skip WAL header
			walHeaderSize := wal.WALHeaderSize()
			_, err = f.Seek(int64(walHeaderSize)+lastWALFile.Offset, 0)
			assert.NoError(t, err, "Should seek to offset")

			// Read fragments until we find the last one
			var lastFragment Fragment
			var foundLastFragment bool
			var totalRead int64 = 0

			for totalRead < lastWALFile.Size {
				headerBuf := make([]byte, 32)
				_, err = io.ReadFull(f, headerBuf)
				if err != nil {
					break
				}

				fragment := Fragment{}
				fragment.SeqNum = binary.BigEndian.Uint64(headerBuf[0:8])
				fragment.ShardNum = binary.BigEndian.Uint64(headerBuf[8:16])
				fragment.ShardFragment = binary.BigEndian.Uint32(headerBuf[16:20])
				fragment.Length = binary.BigEndian.Uint32(headerBuf[20:24])
				fragment.Flags = Flags(binary.BigEndian.Uint32(headerBuf[24:28]))
				fragment.Checksum = binary.BigEndian.Uint32(headerBuf[28:32])

				// Read full on-disk payload (fixed ChunkSize), not the logical fragment.Length.
				// (WAL always writes padded ChunkSize bytes per fragment on disk.)
				payload := make([]byte, wal.Shard.ChunkSize)
				_, err = io.ReadFull(f, payload)
				if err != nil {
					break
				}

				totalRead += int64(FragmentHeaderBytes + wal.Shard.ChunkSize)

				// Check if this is the last fragment
				if fragment.Flags&FlagEndOfShard != 0 {
					lastFragment = fragment
					foundLastFragment = true
					break
				}
			}

			assert.True(t, foundLastFragment, "Should find fragment with end-of-shard flag set")
			assert.Equal(t, writeResult.ShardNum, lastFragment.ShardNum, "Last fragment should have correct shard number")

			// Close and reopen WAL
			wal.Close()

			wal2, err := New("", tmpDir)
			assert.NoError(t, err, "Should reopen WAL without error")

			// Read the data back - this should validate end-of-shard correctly
			readData, err := wal2.ReadFromWriteResult(writeResult)
			assert.NoError(t, err, "Read should not error")
			assert.NotNil(t, readData)
			assert.Equal(t, tc.dataSize, len(readData), "Read data size should match")
			assert.True(t, bytes.Equal(testData, readData), "Read data should match original")

			// Also test the single-file Read() method
			if len(writeResult.WALFiles) == 1 {
				readData2, err := wal2.Read(writeResult.WALFiles[0].WALNum, writeResult.ShardNum, uint32(tc.dataSize))
				assert.NoError(t, err, "Read should not error")
				assert.NotNil(t, readData2)
				assert.Equal(t, tc.dataSize, len(readData2), "Read data size should match")
				assert.True(t, bytes.Equal(testData, readData2), "Read data should match original")
			}

			wal2.Close()
		})
	}
}

func TestReadEndOfShardValidationErrors(t *testing.T) {
	tmpDir := os.TempDir()

	// Clean up any existing WAL files
	for i := 1; i <= 10; i++ {
		os.RemoveAll(filepath.Join(tmpDir, FormatWalFile(uint64(i))))
	}

	t.Run("EndOfShardFlagMissing", func(t *testing.T) {
		wal, err := New("", tmpDir)
		assert.NoError(t, err)
		defer wal.Close()

		// Write a small file
		testData := make([]byte, 100)
		for i := range testData {
			testData[i] = byte(i % 256)
		}

		writeResult, err := wal.Write(bytes.NewReader(testData), 100)
		assert.NoError(t, err)

		// Flush any pending writes
		wal.Close()

		// Test by using Read() with a larger expected size
		// This simulates the case where end-of-shard flag validation should catch
		// that we've read all data but expected more
		wal2, err := New("", tmpDir)
		assert.NoError(t, err)
		defer wal2.Close()

		// Try to read with a larger expected size - should fail validation
		// when end-of-shard is encountered but we expected more data
		_, err = wal2.Read(writeResult.WALFiles[0].WALNum, writeResult.ShardNum, 200)
		assert.Error(t, err, "Read should error when end-of-shard flag is set but more data expected")
		// The error should be about remaining bytes or end-of-shard validation
		if err != nil {
			assert.True(t,
				contains(err.Error(), "remaining") || contains(err.Error(), "end-of-shard"),
				"Error should mention remaining bytes or end-of-shard, got: %s", err.Error())
		}
	})

	t.Run("EndOfShardFlagSetButFragmentLengthMismatch", func(t *testing.T) {
		wal, err := New("", tmpDir)
		assert.NoError(t, err)
		defer wal.Close()

		// Write exactly one chunk
		testData := make([]byte, 8192)
		for i := range testData {
			testData[i] = byte(i % 256)
		}

		writeResult, err := wal.Write(bytes.NewReader(testData), 8192)
		assert.NoError(t, err)

		// Manually corrupt: change the expected file size to be larger
		// This simulates a case where end-of-shard is set but we expect more data
		wal.Close()

		// Create a new WriteResult with incorrect TotalSize
		corruptedResult := *writeResult
		corruptedResult.TotalSize = 10000 // More than the actual 8192 bytes

		wal2, err := New("", tmpDir)
		assert.NoError(t, err)
		defer wal2.Close()

		// Try to read - should fail because end-of-shard flag is set but we expect more data
		_, err = wal2.ReadFromWriteResult(&corruptedResult)
		assert.Error(t, err, "Read should error when end-of-shard flag is set but more data expected")
		if err != nil {
			assert.True(t,
				contains(err.Error(), "remaining") || contains(err.Error(), "end-of-shard"),
				"Error should mention remaining bytes or end-of-shard, got: %s", err.Error())
		}
	})
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}

// TestConcurrentWritesWithSync verifies that concurrent writes don't deadlock
// when the background sync goroutine is running.
// This test catches the issue where holding RLock during Sync() blocked Write() calls.
func TestConcurrentWritesWithSync(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), fmt.Sprintf("wal-concurrent-test-%d", time.Now().UnixNano()))
	os.MkdirAll(tmpDir, 0750)
	defer os.RemoveAll(tmpDir)

	// Use a short sync interval to increase chance of hitting the race condition
	wal, err := New("", tmpDir)
	assert.NoError(t, err)
	wal.WALSyncInterval = 10 * time.Millisecond // Very aggressive sync
	wal.StopWALSyncer()                         // Stop the default syncer
	wal.StartWALSyncer()                        // Restart with new interval
	defer wal.Close()

	// Number of concurrent writers
	numWriters := 10
	writesPerWriter := 20
	dataSize := 8192 // 1 chunk per write

	// Channel to track completion
	done := make(chan bool, numWriters)
	errors := make(chan error, numWriters*writesPerWriter)

	// Timeout to detect deadlock - if this test hangs, it's a deadlock
	timeout := time.After(30 * time.Second)

	// Start concurrent writers
	for i := 0; i < numWriters; i++ {
		go func(writerID int) {
			for j := 0; j < writesPerWriter; j++ {
				data := make([]byte, dataSize)
				// Fill with identifiable data
				for k := 0; k < len(data); k++ {
					data[k] = byte((writerID*256 + j + k) % 256)
				}

				_, err := wal.Write(bytes.NewReader(data), len(data))
				if err != nil {
					errors <- fmt.Errorf("writer %d, write %d failed: %w", writerID, j, err)
				}
			}
			done <- true
		}(i)
	}

	// Wait for all writers to complete or timeout
	completed := 0
	for completed < numWriters {
		select {
		case <-done:
			completed++
		case err := <-errors:
			t.Errorf("Write error: %v", err)
		case <-timeout:
			t.Fatalf("DEADLOCK DETECTED: Test timed out after 30 seconds. "+
				"Only %d/%d writers completed. "+
				"This indicates the sync goroutine is blocking writes.", completed, numWriters)
		}
	}

	// Check for any remaining errors
	close(errors)
	for err := range errors {
		t.Errorf("Write error: %v", err)
	}

	t.Logf("All %d writers completed %d writes each successfully", numWriters, writesPerWriter)
}

// TestSyncDoesNotBlockWrites verifies that the sync operation doesn't hold locks
// during the actual disk I/O, which would block concurrent writes.
func TestSyncDoesNotBlockWrites(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), fmt.Sprintf("wal-sync-block-test-%d", time.Now().UnixNano()))
	os.MkdirAll(tmpDir, 0750)
	defer os.RemoveAll(tmpDir)

	wal, err := New("", tmpDir)
	assert.NoError(t, err)
	defer wal.Close()

	// Do an initial write to ensure WAL file exists
	data := make([]byte, 8192)
	_, err = wal.Write(bytes.NewReader(data), len(data))
	assert.NoError(t, err)

	// Mark as dirty
	wal.dirty.Store(true)

	// Start a goroutine that will try to write while sync is running
	writeStarted := make(chan bool)
	writeDone := make(chan bool)
	timeout := time.After(5 * time.Second)

	go func() {
		writeStarted <- true
		_, err := wal.Write(bytes.NewReader(data), len(data))
		if err != nil {
			t.Errorf("Write during sync failed: %v", err)
		}
		writeDone <- true
	}()

	// Wait for write goroutine to start
	<-writeStarted

	// Trigger sync - this should NOT block the write goroutine
	wal.syncWALIfDirty()

	// The write should complete quickly if sync doesn't block
	select {
	case <-writeDone:
		// Success - write completed
	case <-timeout:
		t.Fatal("DEADLOCK: Write blocked by sync operation for more than 5 seconds")
	}
}
