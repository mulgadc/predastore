package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// 32MB Shard size per WAL file (prod)
// 4MB for dev (testing)
var ShardSize uint32 = 1024 * 1024 * 4

// 8kb chunk sizes
const ChunkSize uint32 = 1024 * 8

type Flags uint32

const (
	FlagNone         Flags = 0
	FlagEndOfShard   Flags = 1 << 0 // this fragment is the last for this shard
	FlagShardHeader  Flags = 1 << 1 // special header/meta entry for this shard
	FlagCompressed   Flags = 1 << 2 // Data is compressed
	FlagDeleted      Flags = 1 << 3 // logical tombstone
	FlagPartialWrite Flags = 1 << 4 // might indicate recovery / truncated write
	FlagReserved1    Flags = 1 << 5
	FlagReserved2    Flags = 1 << 6
	FlagReserved3    Flags = 1 << 7
)

type WAL struct {
	WalNum   atomic.Uint64
	SeqNum   atomic.Uint64
	ShardNum atomic.Uint64
	Epoch    time.Time

	WalDir    string
	StateFile string

	Shard Shard
	mu    sync.RWMutex `json:"-"`
}

type WALState struct {
	WalNum   uint64
	SeqNum   uint64
	ShardNum uint64
	Epoch    time.Time

	WalDir    string
	StateFile string
}

type Shard struct {
	Magic     [4]byte
	Version   uint16
	ShardSize uint32
	ChunkSize uint32

	DB []*os.File `json:"-"`
}

type Fragment struct {
	SeqNum        uint64
	ShardNum      uint64
	ShardFragment uint32
	Length        uint32
	Flags         Flags // Feature flags
	Checksum      uint32
	Data          [ChunkSize]byte
}

// WALFileInfo represents information about a WAL file used to store an object
type WALFileInfo struct {
	WALNum uint64 // WAL file number (e.g., 1, 2, 3)
	Offset int64  // Starting offset in the WAL file (after WAL header)
	Size   int64  // Size of data written to this WAL file
}

// WriteResult contains information about where an object was written
type WriteResult struct {
	ShardNum  uint64        // ShardNum for this object
	WALFiles  []WALFileInfo // List of WAL files used (in order)
	TotalSize int           // Total size of the object written
}

func New(stateFile, walDir string) (wal *WAL, err error) {

	// Add header, and each Chunk header
	// FragmentHeaderSize() is 32 bytes
	// WALHeaderSize() 14 bytes
	var shardHeaders = ((ShardSize / ChunkSize) * 32) + 14

	// Append shardHeaders
	ShardSize -= shardHeaders

	// State
	wal = &WAL{
		WalDir:    walDir,
		StateFile: stateFile,

		Shard: Shard{
			// S3 Shard File (S3SF)
			Magic:     [4]byte{'S', '3', 'S', 'F'},
			Version:   1,
			ShardSize: ShardSize,
			ChunkSize: ChunkSize,
		},
	}

	_, err = os.Stat(stateFile)

	fmt.Println("stateFile", stateFile, "err", err)

	// If no file exists, start from 0
	if err != nil {
		fmt.Println("Incrementing WalNum and SeqNum")
		wal.SeqNum.Add(1)
		wal.WalNum.Add(1)
	} else {

		wal.LoadState(stateFile)

	}

	// Next, check if a WAL already exists
	walNum := wal.WalNum.Load()

	filename := filepath.Join(walDir, FormatWalFile(walNum))
	fmt.Println("walNum", walNum, "filename", filename)

	_, err = os.Stat(filename)

	if err != nil {

		err = wal.createWALUnlocked(filename)
		//wal.CreateWAL(filename)
		if err != nil {
			return wal, err
		}

	}

	return wal, err
}

// SaveState saves the WAL state to disk
func (wal *WAL) SaveState() error {
	state := WALState{
		WalNum:    wal.WalNum.Load(),
		SeqNum:    wal.SeqNum.Load(),
		ShardNum:  wal.ShardNum.Load(),
		Epoch:     wal.Epoch,
		WalDir:    wal.WalDir,
		StateFile: wal.StateFile,
	}

	stateData, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal state data: %v", err)
	}

	filename := filepath.Join(wal.WalDir, "state.json")
	if wal.StateFile != "" {
		filename = wal.StateFile
	}

	return os.WriteFile(filename, stateData, 0640)
}

// LoadState loads the WAL state from disk
func (wal *WAL) LoadState(stateFile string) error {
	stateData, err := os.ReadFile(stateFile)
	if err != nil {
		return err
	}

	var state WALState
	if err := json.Unmarshal(stateData, &state); err != nil {
		return err
	}

	wal.WalNum.Store(state.WalNum)
	wal.SeqNum.Store(state.SeqNum)
	wal.ShardNum.Store(state.ShardNum)
	wal.Epoch = state.Epoch
	wal.WalDir = state.WalDir
	wal.StateFile = state.StateFile

	return nil
}

/*
func (wal *WAL) CreateWAL(filename string) (err error) {

	// Lock operations on the WAL
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// Create the directory if it doesn't exist
	os.MkdirAll(filepath.Dir(filename), 0750)

	// Create the file if it doesn't exist, make sure writes and committed immediately
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR|syscall.O_SYNC, 0640)
	if err != nil {
		return err
	}

	// Check if file is empty (new file) or has existing data
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return err
	}

	// If file is empty, write WAL header
	if stat.Size() == 0 {
		var headers []byte
		headers = wal.WALHeader()

		_, err = file.Write(headers)
		if err != nil {
			file.Close()
			slog.Error("Could not write headers")
			return err
		}
	}

	// Append the latest "hot" WAL file to the DB
	wal.Shard.DB = append(wal.Shard.DB, file)

	return nil
}
*/

// getCurrentWALFileSize returns the current size of the active WAL file (excluding header)
func (wal *WAL) getCurrentWALFileSize() (int64, error) {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	if len(wal.Shard.DB) == 0 {
		return 0, errors.New("no WAL files open")
	}

	activeWal := wal.Shard.DB[len(wal.Shard.DB)-1]
	stat, err := activeWal.Stat()
	if err != nil {
		return 0, err
	}

	// Return size minus WAL header
	walHeaderSize := int64(wal.WALHeaderSize())
	fileSize := stat.Size()
	if fileSize < walHeaderSize {
		return 0, nil
	}

	return fileSize - walHeaderSize, nil
}

// ensureWALFile ensures we have a WAL file open and returns its index
func (wal *WAL) ensureWALFile() (int, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// If we have files open, check if current one has space
	if len(wal.Shard.DB) > 0 {
		activeWal := wal.Shard.DB[len(wal.Shard.DB)-1]
		stat, err := activeWal.Stat()
		if err != nil {
			return 0, err
		}

		// Check if current file has space
		// ShardSize is the max total file size on disk (including WAL header and fragment headers)
		// We need to leave room for at least one fragment (32 bytes header + up to ChunkSize data)
		// Use strict < to ensure we always have room
		maxFragmentSize := int64(32 + ChunkSize)
		if stat.Size() < int64(wal.Shard.ShardSize)-maxFragmentSize {
			return len(wal.Shard.DB) - 1, nil
		}
	}

	// Need to create a new WAL file
	// Keep incrementing until we find an empty file or one with space
	maxAttempts := 100
	for attempt := 0; attempt < maxAttempts; attempt++ {
		filename := filepath.Join(wal.WalDir, FormatWalFile(wal.WalNum.Load()))

		err := wal.createWALUnlocked(filename)
		if err != nil {
			// If file is full, try next number
			if attempt < maxAttempts-1 {
				fmt.Println("Incrementing WalNum")
				fmt.Println("wal.WalNum.Load()", wal.WalNum.Load())
				wal.WalNum.Add(1)
				fmt.Println("wal.WalNum.Load()", wal.WalNum.Load())

				continue
			}
			return 0, err
		}

		return len(wal.Shard.DB) - 1, nil
	}

	return 0, fmt.Errorf("could not find available WAL file after %d attempts", maxAttempts)
}

// createWALUnlocked creates a new WAL file (must be called with lock held)
func (wal *WAL) createWALUnlocked(filename string) error {

	// Create the directory if it doesn't exist
	//os.MkdirAll(filepath.Dir(filename), 0750)

	// Check if file already exists and has space for at least one full fragment
	// We need room for: 32 bytes header + up to ChunkSize (8192) bytes data = 8224 bytes
	maxFragmentSize := int64(32 + ChunkSize)
	if stat, err := os.Stat(filename); err == nil {
		// File exists, check if it has space for at least one fragment
		if stat.Size()+maxFragmentSize > int64(wal.Shard.ShardSize) {
			// File doesn't have enough space, we need a new file number
			return fmt.Errorf("WAL file %s doesn't have enough space (%d + %d > %d)",
				filename, stat.Size(), maxFragmentSize, wal.Shard.ShardSize)
		}
	}

	// Open or create the file
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR|syscall.O_SYNC, 0640)
	if err != nil {
		return err
	}

	// Check if file is empty (new file) or has existing data
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return err
	}

	// If file is empty, write WAL header
	if stat.Size() == 0 {
		var headers []byte
		headers = wal.WALHeader()

		_, err = file.Write(headers)
		if err != nil {
			file.Close()
			slog.Error("Could not write headers")
			return err
		}
	} else {
		// File exists, verify it has space for at least one fragment
		maxFragmentSize := int64(32 + ChunkSize)
		if stat.Size()+maxFragmentSize > int64(wal.Shard.ShardSize) {
			// File doesn't have enough space, close it and return error
			file.Close()
			return fmt.Errorf("WAL file %s doesn't have enough space (%d + %d > %d)",
				filename, stat.Size(), maxFragmentSize, wal.Shard.ShardSize)
		}
	}

	// Append the latest "hot" WAL file to the DB
	wal.Shard.DB = append(wal.Shard.DB, file)

	// Write the state file to disk, current WAL, Shard Num, etc
	err = wal.SaveState()

	if err != nil {
		return fmt.Errorf("failed to save state: %v", err)
	}

	return nil
}

func (wal *WAL) Write(r io.Reader, totalSize int) (*WriteResult, error) {
	result := &WriteResult{
		WALFiles:  make([]WALFileInfo, 0),
		TotalSize: totalSize,
	}

	// Increment shard number once per object
	wal.ShardNum.Add(1)
	result.ShardNum = wal.ShardNum.Load()

	remaining := totalSize
	var shardFragment uint32
	var currentWALIndex int = -1
	var currentWALFileSize int64 = 0

	for remaining > 0 {
		// Ensure we have a WAL file with space
		walIndex, err := wal.ensureWALFile()
		if err != nil {
			return nil, fmt.Errorf("failed to ensure WAL file: %v", err)
		}

		// If we switched to a new WAL file, update tracking
		if walIndex != currentWALIndex {
			// Finalize previous WAL file info
			if currentWALIndex >= 0 && len(result.WALFiles) > 0 {
				result.WALFiles[len(result.WALFiles)-1].Size = currentWALFileSize
			}

			// Get new WAL file number and determine offset
			wal.mu.RLock()
			newWALNum := wal.WalNum.Load()
			var newOffset int64 = 0
			if walIndex < len(wal.Shard.DB) {
				activeWal := wal.Shard.DB[walIndex]
				stat, err := activeWal.Stat()
				if err == nil {
					// Offset is where we start writing this object's data
					// For a brand new file, this is 0 (right after WAL header)
					// For an existing file, it's the current position
					currentFileSize := stat.Size()
					walHeaderSize := int64(wal.WALHeaderSize())
					if currentFileSize <= walHeaderSize {
						// Brand new file, offset is 0
						newOffset = 0
					} else {
						// Existing file, offset is current position (excluding WAL header)
						newOffset = currentFileSize - walHeaderSize
					}
				}
			}
			wal.mu.RUnlock()

			// Start tracking new WAL file
			result.WALFiles = append(result.WALFiles, WALFileInfo{
				WALNum: newWALNum,
				Offset: newOffset,
				Size:   0,
			})

			currentWALIndex = walIndex
			// Reset currentWALFileSize to 0 (we track size written for THIS object from the offset)
			currentWALFileSize = 0
		}

		// Read in 8kb chunks (or less for last chunk)
		chunk, err := ReadChunk(r)
		if len(chunk) == 0 {
			break
		}
		if err != nil {
			slog.Error("ReadChunk failed", "err", err)
			return nil, fmt.Errorf("ReadChunk failed: %v", err)
		}

		// Clamp chunk size to remaining bytes
		actualChunkSize := len(chunk)
		if actualChunkSize > remaining {
			actualChunkSize = remaining
			chunk = chunk[:actualChunkSize]
		}

		// Check available space in current WAL file (re-check after ensuring file)
		// Get total file size (including WAL header)
		wal.mu.RLock()
		if currentWALIndex >= len(wal.Shard.DB) {
			wal.mu.RUnlock()
			return nil, errors.New("invalid WAL file index")
		}
		activeWal := wal.Shard.DB[currentWALIndex]
		stat, err := activeWal.Stat()
		wal.mu.RUnlock()
		if err != nil {
			return nil, fmt.Errorf("failed to get WAL file stat: %v", err)
		}

		// Check if this chunk fits in current WAL file
		// ShardSize is the max total file size on disk (including WAL header and all fragment headers)
		chunkSizeWithHeader := int64(32 + actualChunkSize)
		currentTotalSize := stat.Size()
		// Use strict check: if adding this chunk would exceed ShardSize, create a new file
		if currentTotalSize+chunkSizeWithHeader > int64(wal.Shard.ShardSize) {
			// This chunk won't fit in current file, create a new file
			// First, finalize the current WAL file
			if currentWALIndex >= 0 && len(result.WALFiles) > 0 {
				result.WALFiles[len(result.WALFiles)-1].Size = currentWALFileSize
			}

			// Create a new WAL file (ensureWALFile will create one if current is full)
			walIndex, err = wal.ensureWALFile()
			if err != nil {
				return nil, fmt.Errorf("failed to ensure new WAL file: %v", err)
			}

			// Update tracking for new file
			wal.mu.RLock()
			newWALNum := wal.SeqNum.Load()
			var newOffset int64 = 0
			if walIndex < len(wal.Shard.DB) {
				activeWal := wal.Shard.DB[walIndex]
				stat, err := activeWal.Stat()
				if err == nil {
					currentFileSize := stat.Size()
					walHeaderSize := int64(wal.WALHeaderSize())
					if currentFileSize <= walHeaderSize {
						// Brand new file, offset is 0
						newOffset = 0
					} else {
						// Existing file, offset is current position (excluding WAL header)
						newOffset = currentFileSize - walHeaderSize
					}
				}
			}
			wal.mu.RUnlock()

			result.WALFiles = append(result.WALFiles, WALFileInfo{
				WALNum: newWALNum,
				Offset: newOffset,
				Size:   0,
			})
			currentWALIndex = walIndex
			currentWALFileSize = 0

			// Re-verify the new file has space
			wal.mu.RLock()
			if currentWALIndex < len(wal.Shard.DB) {
				activeWal := wal.Shard.DB[currentWALIndex]
				stat, err := activeWal.Stat()
				wal.mu.RUnlock()
				if err == nil {
					currentTotalSize = stat.Size()
					if currentTotalSize+chunkSizeWithHeader > int64(wal.Shard.ShardSize) {
						return nil, fmt.Errorf("new WAL file %d already too full: %d + %d > %d",
							currentWALIndex, currentTotalSize, chunkSizeWithHeader, wal.Shard.ShardSize)
					}
				}
			} else {
				wal.mu.RUnlock()
			}
		}

		// Write the fragment
		isLastFragment := remaining-actualChunkSize <= 0
		err = wal.writeFragment(currentWALIndex, result.ShardNum, shardFragment, chunk[:actualChunkSize], isLastFragment)
		if err != nil {
			return nil, fmt.Errorf("failed to write fragment: %v", err)
		}

		// Track size written for this object in current WAL file (from the offset)
		fragmentSize := int64(32 + actualChunkSize) // header + data
		currentWALFileSize += fragmentSize
		remaining -= actualChunkSize
		shardFragment++

		// Verify we haven't exceeded ShardSize (safety check)
		wal.mu.RLock()
		if currentWALIndex < len(wal.Shard.DB) {
			activeWal := wal.Shard.DB[currentWALIndex]
			stat, err := activeWal.Stat()
			wal.mu.RUnlock()
			if err == nil && stat.Size() > int64(wal.Shard.ShardSize) {
				return nil, fmt.Errorf("WAL file %d exceeded ShardSize: %d > %d",
					currentWALIndex, stat.Size(), wal.Shard.ShardSize)
			}
		} else {
			wal.mu.RUnlock()
		}
	}

	// Finalize last WAL file info
	if len(result.WALFiles) > 0 {
		result.WALFiles[len(result.WALFiles)-1].Size = currentWALFileSize
	}

	return result, nil
}

// writeFragment writes a single fragment to the specified WAL file
func (wal *WAL) writeFragment(walIndex int, shardNum uint64, shardFragment uint32, chunk []byte, isLast bool) error {
	wal.mu.RLock()
	if walIndex >= len(wal.Shard.DB) {
		wal.mu.RUnlock()
		return errors.New("invalid WAL file index")
	}
	activeWal := wal.Shard.DB[walIndex]
	wal.mu.RUnlock()

	// Increment SeqNum for each fragment
	seqNum := wal.SeqNum.Add(1) - 1 // Add returns new value, subtract 1 to get what we used

	fragment := Fragment{}
	fragment.SeqNum = seqNum
	fragment.ShardNum = shardNum
	fragment.ShardFragment = shardFragment
	fragment.Length = uint32(len(chunk))

	if isLast {
		fragment.Flags |= FlagEndOfShard
	}

	// Calculate a CRC32 checksum of the block data and headers
	payload := make([]byte, fragment.FragmentHeaderSize()+len(chunk))

	headers := fragment.FragmentHeader()
	copy(payload[0:len(headers)], headers)

	binary.BigEndian.PutUint64(payload[0:8], fragment.SeqNum)
	binary.BigEndian.PutUint64(payload[8:16], fragment.ShardNum)
	binary.BigEndian.PutUint32(payload[16:20], fragment.ShardFragment)
	binary.BigEndian.PutUint32(payload[20:24], uint32(fragment.Length))
	binary.BigEndian.PutUint32(payload[24:28], uint32(fragment.Flags))

	checksum_validated := crc32.ChecksumIEEE(payload[0:len(headers)])
	checksum_validated = crc32.Update(checksum_validated, crc32.IEEETable, chunk)

	fragment.Checksum = checksum_validated
	payload = fragment.AppendChecksum(payload)

	// Add the data to the chunk
	copy(payload[32:32+len(chunk)], chunk)

	// Write to the WAL file
	_, err := activeWal.Write(payload)
	if err != nil {
		return fmt.Errorf("failed to write to WAL file: %v", err)
	}

	return nil
}

func (wal *WAL) Close() (err error) {

	// Loop through each file

	for _, v := range wal.Shard.DB {

		v.Close()

	}

	// Write the state file to disk, current WAL, Shard Num, etc
	err = wal.SaveState()

	if err != nil {
		return fmt.Errorf("failed to save state: %v", err)
	}

	return

}

// ReadFromWriteResult reads an object using the WriteResult from a previous Write() call
func (wal *WAL) ReadFromWriteResult(result *WriteResult) (data []byte, err error) {
	if len(result.WALFiles) == 0 {
		return nil, errors.New("no WAL files in WriteResult")
	}

	data = make([]byte, result.TotalSize)
	var bytesRead int

	for _, walFile := range result.WALFiles {
		f, err := os.Open(filepath.Join(wal.WalDir, FormatWalFile(walFile.WALNum)))
		if err != nil {
			return nil, fmt.Errorf("failed to open WAL file %d: %v", walFile.WALNum, err)
		}
		defer f.Close()

		// Skip WAL header
		walHeaderSize := wal.WALHeaderSize()
		_, err = f.Seek(int64(walHeaderSize)+walFile.Offset, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to seek to offset in WAL file %d: %v", walFile.WALNum, err)
		}

		// Read fragments from this WAL file until we've read walFile.Size bytes
		var fileBytesRead int64 = 0
		for fileBytesRead < walFile.Size {
			fragment := Fragment{}
			headerSize := int(fragment.FragmentHeaderSize())

			// Read the fragment header
			headerBuf := make([]byte, headerSize)
			if _, err = io.ReadFull(f, headerBuf); err != nil {
				return nil, fmt.Errorf("could not read chunk header from WAL %d: %v", walFile.WALNum, err)
			}

			// Parse all header fields
			fragment.SeqNum = binary.BigEndian.Uint64(headerBuf[0:8])
			fragment.ShardNum = binary.BigEndian.Uint64(headerBuf[8:16])
			fragment.ShardFragment = binary.BigEndian.Uint32(headerBuf[16:20])
			fragment.Length = binary.BigEndian.Uint32(headerBuf[20:24])
			fragment.Flags = Flags(binary.BigEndian.Uint32(headerBuf[24:28]))
			fragment.Checksum = binary.BigEndian.Uint32(headerBuf[28:32])

			// Sanity checks
			if fragment.ShardNum != result.ShardNum {
				return nil, fmt.Errorf("shard num mismatch in WAL %d: expected %d, got %d", walFile.WALNum, result.ShardNum, fragment.ShardNum)
			}
			if fragment.Length > wal.Shard.ChunkSize {
				return nil, fmt.Errorf("chunk length %d exceeds max %d in WAL %d", fragment.Length, wal.Shard.ChunkSize, walFile.WALNum)
			}

			payloadSize := int(fragment.Length)

			// Read the payload
			chunkBuffer := make([]byte, payloadSize)
			n, err := io.ReadFull(f, chunkBuffer)
			if err != nil {
				return nil, fmt.Errorf("could not read chunk from WAL %d: %v", walFile.WALNum, err)
			}

			// Validate checksum
			headerForChecksum := make([]byte, 32)
			copy(headerForChecksum, headerBuf)
			headerForChecksum[28] = 0
			headerForChecksum[29] = 0
			headerForChecksum[30] = 0
			headerForChecksum[31] = 0

			calculatedChecksum := crc32.ChecksumIEEE(headerForChecksum)
			calculatedChecksum = crc32.Update(calculatedChecksum, crc32.IEEETable, chunkBuffer)

			if calculatedChecksum != fragment.Checksum {
				return nil, fmt.Errorf("checksum mismatch for fragment %d in WAL %d: expected %d, got %d", fragment.ShardFragment, walFile.WALNum, fragment.Checksum, calculatedChecksum)
			}

			// Copy to output buffer
			remaining := result.TotalSize - bytesRead
			copySize := payloadSize
			if copySize > remaining {
				copySize = remaining
			}
			copy(data[bytesRead:bytesRead+copySize], chunkBuffer[:copySize])
			bytesRead += copySize
			fileBytesRead += int64(32 + n) // header + data

			// Validate end-of-shard flag
			if fragment.Flags&FlagEndOfShard != 0 {
				// This is the last fragment for this shard
				// Verify that we've read all expected bytes
				remainingAfterRead := result.TotalSize - bytesRead
				if remainingAfterRead != 0 {
					return nil, fmt.Errorf("end-of-shard flag set but %d bytes remaining (expected 0)", remainingAfterRead)
				}
				// Verify that the fragment length matches what we expected to read
				if copySize != remaining {
					return nil, fmt.Errorf("end-of-shard flag set but fragment length %d doesn't match remaining bytes %d", copySize, remaining)
				}
				// No more fragments should be read after this
				break
			}
		}
	}

	if bytesRead != result.TotalSize {
		return nil, fmt.Errorf("read %d bytes but expected %d", bytesRead, result.TotalSize)
	}

	return data, nil
}

// Read reads an object from a single WAL file (backward compatibility)
func (wal *WAL) Read(walNum uint64, shardNum uint64, filesize uint32) (data []byte, err error) {
	var bytesRead uint32
	var remaining uint32

	f, err := os.Open(filepath.Join(wal.WalDir, FormatWalFile(walNum)))

	if err != nil {
		return nil, err
	}

	// Confirm WAL is as expected, check magic
	walHeader := make([]byte, wal.WALHeaderSize())

	n, err := io.ReadFull(f, walHeader)

	if err != nil {
		return nil, fmt.Errorf("could not read WAL header: %v", err)
	}

	if n != len(walHeader) {
		return nil, errors.New("file header len incorrect")
	}

	// Confirm
	if !bytes.Equal(walHeader[:4], wal.Shard.Magic[:]) {
		return nil, errors.New("file not recognized as WAL")
	}

	// Read each chunk, confirm fragments are as expected
	remaining = filesize
	data = make([]byte, filesize)

	for remaining > 0 {

		fragment := Fragment{}
		headerSize := int(fragment.FragmentHeaderSize())

		// Read the fragment header first so we know exactly how much payload to read.
		headerBuf := make([]byte, headerSize)
		if _, err = io.ReadFull(f, headerBuf); err != nil {
			return nil, fmt.Errorf("could not read chunk header: %v", err)
		}

		// Parse all header fields
		fragment.SeqNum = binary.BigEndian.Uint64(headerBuf[0:8])
		fragment.ShardNum = binary.BigEndian.Uint64(headerBuf[8:16])
		fragment.ShardFragment = binary.BigEndian.Uint32(headerBuf[16:20])
		fragment.Length = binary.BigEndian.Uint32(headerBuf[20:24])
		fragment.Flags = Flags(binary.BigEndian.Uint32(headerBuf[24:28]))
		fragment.Checksum = binary.BigEndian.Uint32(headerBuf[28:32])

		// Sanity checks
		if fragment.ShardNum != shardNum {
			return nil, fmt.Errorf("shard num mismatch %d vs %d", fragment.ShardNum, shardNum)
		}
		if fragment.Length > wal.Shard.ChunkSize {
			return nil, fmt.Errorf("chunk length %d exceeds max %d", fragment.Length, wal.Shard.ChunkSize)
		}

		payloadSize := int(fragment.Length)

		// Read exactly the payload for this fragment (no over-read into the next chunk).
		chunkBuffer := make([]byte, payloadSize)

		//slog.Debug("chunkBuffer", "len", len(chunkBuffer), "fragment.Length", fragment.Length, "remaining", remaining, "bytesRead", bytesRead)

		n, err = io.ReadFull(f, chunkBuffer) // use io.ReadFull so we get exactly len(chunkBuffer) or an error

		// Confirm, check EOF
		if err != nil {
			return nil, fmt.Errorf("could not read chunk: %v", err)
		}

		// Validate checksum: calculate checksum the same way as Write()
		// Checksum is calculated over: full 32-byte header (with checksum field set to 0) + data
		headerForChecksum := make([]byte, 32)
		copy(headerForChecksum, headerBuf)
		// Set checksum field to 0 for calculation (bytes 28-32)
		headerForChecksum[28] = 0
		headerForChecksum[29] = 0
		headerForChecksum[30] = 0
		headerForChecksum[31] = 0

		calculatedChecksum := crc32.ChecksumIEEE(headerForChecksum)
		calculatedChecksum = crc32.Update(calculatedChecksum, crc32.IEEETable, chunkBuffer)

		if calculatedChecksum != fragment.Checksum {
			return nil, fmt.Errorf("checksum mismatch for fragment %d: expected %d, got %d", fragment.ShardFragment, fragment.Checksum, calculatedChecksum)
		}

		// Clamp payloadSize to what's logically remaining in the object
		if uint32(payloadSize) > remaining {
			payloadSize = int(remaining)
		}

		// Validate end-of-shard flag before reading
		if fragment.Flags&FlagEndOfShard != 0 {
			// This is the last fragment for this shard
			// Verify that the fragment length matches remaining bytes
			if uint32(payloadSize) != remaining {
				return nil, fmt.Errorf("end-of-shard flag set but fragment length %d doesn't match remaining bytes %d", payloadSize, remaining)
			}
		}

		// Bounds-safe copy
		copy(
			data[bytesRead:bytesRead+uint32(payloadSize)],
			chunkBuffer[:payloadSize],
		)

		remaining -= uint32(payloadSize)
		bytesRead += uint32(payloadSize)

		// If end-of-shard flag was set, verify we've read all expected bytes and break
		if fragment.Flags&FlagEndOfShard != 0 {
			if remaining != 0 {
				return nil, fmt.Errorf("end-of-shard flag set but %d bytes remaining (expected 0)", remaining)
			}
			// No more fragments should be read after this
			break
		}
	}

	// Verify we read all expected bytes (if end-of-shard was not set, this catches missing data)
	if remaining != 0 {
		return nil, fmt.Errorf("read incomplete: %d bytes remaining but end-of-shard flag not set", remaining)
	}

	return data, nil

}

func numFragments(size int) int {
	if size == 0 {
		return 0
	}
	return (size + int(ChunkSize) - 1) / int(ChunkSize)
}

func ReadChunk(r io.Reader) (chunk []byte, err error) {
	buf := make([]byte, ChunkSize)

	for {
		n, err := r.Read(buf)

		if err == io.EOF {
			if n > 0 {
				// Return the actual bytes read, not the full buffer
				return buf[:n], nil
			}
			return nil, nil // finished cleanly
		}

		if err != nil {
			return nil, err // read error
		}

		if n > 0 {
			// Return only the bytes actually read, not the full 8KB buffer
			return buf[:n], nil
		}
	}

}

func (fragment *Fragment) FragmentHeader() []byte {
	header := make([]byte, fragment.FragmentHeaderSize())

	binary.BigEndian.PutUint64(header[0:8], fragment.SeqNum)
	binary.BigEndian.PutUint64(header[8:16], fragment.ShardNum)
	binary.BigEndian.PutUint32(header[16:20], fragment.ShardFragment)
	binary.BigEndian.PutUint32(header[20:24], fragment.Length)
	binary.BigEndian.PutUint32(header[24:28], uint32(fragment.Flags))

	return header
}

func (fragment *Fragment) AppendChecksum(payload []byte) []byte {

	// Update the checksum since we have calculated it
	//slog.Debug("Checksum", "v", fragment.Checksum)
	binary.BigEndian.PutUint32(payload[28:32], fragment.Checksum)

	return payload

}

// WALHeaderSize returns the size of the WAL header in bytes
func (fragment *Fragment) FragmentHeaderSize() int {

	/*
		SeqNum 8
		ShardNum 8
		ShardFragment 4
		Len 4
		Flags 4
		Checksum 4

		32 bytes total
	*/

	return binary.Size(fragment.SeqNum) +
		binary.Size(fragment.ShardNum) +
		binary.Size(fragment.ShardFragment) +
		binary.Size(fragment.Length) +
		binary.Size(fragment.Flags) +
		binary.Size(fragment.Checksum)
}

func (wal *WAL) WALHeader() []byte {
	header := make([]byte, wal.WALHeaderSize())
	copy(header[0:4], wal.Shard.Magic[:])
	binary.BigEndian.PutUint16(header[4:6], wal.Shard.Version)
	binary.BigEndian.PutUint32(header[6:10], wal.Shard.ShardSize)
	binary.BigEndian.PutUint32(header[10:14], wal.Shard.ChunkSize)
	return header
}

// WALHeaderSize returns the size of the WAL header in bytes
func (wal *WAL) WALHeaderSize() int {
	// Magic bytes (4) + Version (2) + ShardSize (4) + ChunkSize (4)
	return len(wal.Shard.Magic) + binary.Size(wal.Shard.Version) + binary.Size(wal.Shard.ShardSize) + binary.Size(wal.Shard.ChunkSize)
}

func FormatWalFile(seqNum uint64) string {
	return fmt.Sprintf("%016d.wal", seqNum)
}
