package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
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
	"time"

	"github.com/mulgadc/predastore/s3db"
)

// ShardSize is the max DATA size per WAL file (not including headers).
// The actual file size will be larger due to WAL header + fragment headers.
// Use MaxFileSize() to get the actual max file size.
// 32MB for prod, 4MB for dev/testing
var ShardSize uint32 = 1024 * 1024 * 4

// 8kb chunk sizes
const ChunkSize uint32 = 1024 * 8

// DefaultWALSyncInterval controls periodic fsync of WAL to disk (default 200ms)
// Inspired by PostgreSQL's wal_writer_delay, BadgerDB's SyncWrites, MongoDB's journalCommitInterval
const DefaultWALSyncInterval = 200 * time.Millisecond

const (
	// FragmentHeaderBytes is the on-disk header size per fragment.
	// See Fragment.FragmentHeaderSize() which is expected to be 32.
	FragmentHeaderBytes = 32
)

// fragmentBufferPool reuses fragment write buffers to reduce GC pressure.
// Buffer size: FragmentHeaderBytes (32) + ChunkSize (8192) = 8224 bytes
// This pool is critical for performance - writeFragment is called thousands of times
// per shard write, and allocating 8KB+ per call causes significant GC overhead.
var fragmentBufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, FragmentHeaderBytes+ChunkSize)
	},
}

// zeroPadBuffer is a pre-allocated zero buffer for efficient padding.
// Using copy() from this is faster than a byte-by-byte loop.
var zeroPadBuffer = make([]byte, ChunkSize)

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

	// Badger DB for local object to WAL/shard/offset
	DB *s3db.S3DB

	// WALSyncInterval controls periodic fsync of WAL to disk (default 200ms)
	// Inspired by PostgreSQL's wal_writer_delay, BadgerDB's SyncWrites, MongoDB's journalCommitInterval
	WALSyncInterval time.Duration `json:"-"`

	// dirty tracks whether there are unflushed writes since last sync
	// Uses atomic for lock-free access from write path and sync goroutine
	dirty atomic.Bool

	// WAL syncer control (background goroutine for periodic fsync)
	walSyncTicker *time.Ticker
	walSyncStop   chan struct{}
	walSyncDone   chan struct{}
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

	DB []*bufferedWALFile `json:"-"`
}

// bufferedWALFile wraps an os.File with a bufio.Writer for efficient batched writes.
// This reduces syscalls by ~10x for typical shard writes (512 fragments per 4MB shard).
type bufferedWALFile struct {
	file   *os.File
	writer *bufio.Writer
}

// Write writes to the buffered writer
func (b *bufferedWALFile) Write(p []byte) (int, error) {
	return b.writer.Write(p)
}

// Flush flushes buffered data to the underlying file
func (b *bufferedWALFile) Flush() error {
	return b.writer.Flush()
}

// Sync flushes the buffer and syncs the underlying file to disk
func (b *bufferedWALFile) Sync() error {
	if err := b.writer.Flush(); err != nil {
		return err
	}
	return b.file.Sync()
}

// Stat returns file info (flushes buffer first to get accurate size)
func (b *bufferedWALFile) Stat() (os.FileInfo, error) {
	if err := b.writer.Flush(); err != nil {
		return nil, err
	}
	return b.file.Stat()
}

// Close flushes and closes the file
func (b *bufferedWALFile) Close() error {
	if err := b.writer.Flush(); err != nil {
		return err
	}
	return b.file.Close()
}

// newBufferedWALFile creates a new buffered WAL file with 64KB buffer
func newBufferedWALFile(f *os.File) *bufferedWALFile {
	return &bufferedWALFile{
		file:   f,
		writer: bufio.NewWriterSize(f, 64*1024), // 64KB buffer batches ~8 fragments
	}
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

// Object to Shard / WALFileInfo tracker (stored in Badger DB)
type ObjectWriteResult struct {
	Object      [32]byte    // Sha256 of bucket/object
	WriteResult WriteResult // List of WAL files used (in order)
}

func New(stateFile, walDir string) (wal *WAL, err error) {

	if stateFile == "" {
		stateFile = filepath.Join(walDir, "state.json")
	}

	// State
	wal = &WAL{
		WalDir:    walDir,
		StateFile: stateFile,

		Shard: Shard{
			// S3 Shard File (S3SF)
			Magic:   [4]byte{'S', '3', 'S', 'F'},
			Version: 1,
			// ShardSize is the max on-disk WAL file size (including WAL header + fragments).
			ShardSize: ShardSize,
			ChunkSize: ChunkSize,
		},

		// Default WAL sync interval (200ms like PostgreSQL, BadgerDB, RocksDB)
		WALSyncInterval: DefaultWALSyncInterval,
	}

	_, err = os.Stat(stateFile)

	// If no file exists, start from 0
	if err != nil {
		wal.SeqNum.Add(1)
		wal.WalNum.Add(1)
	} else {

		wal.LoadState(stateFile)

	}

	// Create Badger DB for local object to WAL/shard/offset
	wal.DB, err = s3db.New(filepath.Join(wal.WalDir, "db"))
	if err != nil {
		return nil, err
	}

	// Next, check if a WAL already exists
	walNum := wal.WalNum.Load()

	filename := filepath.Join(walDir, FormatWalFile(walNum))

	_, err = os.Stat(filename)

	if err != nil {

		err = wal.createWALUnlocked(filename)
		//wal.CreateWAL(filename)
		if err != nil {
			return wal, err
		}

	}

	// Start background WAL syncer for periodic fsync (200ms default)
	wal.StartWALSyncer()

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

// StartWALSyncer starts a background goroutine that periodically fsyncs the WAL to disk.
// This implements the "group commit" pattern used by PostgreSQL (wal_writer_delay),
// BadgerDB (SyncWrites with ticker), and MongoDB (journalCommitInterval).
//
// The syncer only performs fsync when there are dirty (unflushed) writes,
// avoiding unnecessary disk I/O when the system is idle.
func (wal *WAL) StartWALSyncer() {
	if wal.WALSyncInterval <= 0 {
		slog.Debug("WAL syncer disabled (interval <= 0)")
		return
	}

	wal.walSyncStop = make(chan struct{})
	wal.walSyncDone = make(chan struct{})
	wal.walSyncTicker = time.NewTicker(wal.WALSyncInterval)

	go func() {
		defer close(wal.walSyncDone)
		defer wal.walSyncTicker.Stop()

		for {
			select {
			case <-wal.walSyncTicker.C:
				wal.syncWALIfDirty()
			case <-wal.walSyncStop:
				// Final sync before shutdown
				wal.syncWALIfDirty()
				return
			}
		}
	}()

	slog.Debug("WAL syncer started", "interval", wal.WALSyncInterval)
}

// StopWALSyncer gracefully stops the background WAL sync goroutine.
// It signals the goroutine to stop and waits for it to complete its final sync.
func (wal *WAL) StopWALSyncer() {
	if wal.walSyncStop == nil {
		return
	}

	close(wal.walSyncStop)
	<-wal.walSyncDone

	wal.walSyncStop = nil
	wal.walSyncDone = nil
	wal.walSyncTicker = nil

	slog.Debug("WAL syncer stopped")
}

// syncWALIfDirty performs fsync on the active WAL file if there are pending writes.
// This is the core of the periodic sync mechanism - it checks the dirty flag
// and only syncs when necessary to avoid unnecessary I/O.
//
// IMPORTANT: We must NOT hold the lock during Sync() because:
// 1. Sync() can be slow (disk I/O)
// 2. Holding RLock during Sync() would block all Write() calls waiting for Lock()
// 3. This would cause a deadlock-like condition under heavy write load
//
// Note: Only the last file in wal.Shard.DB is the active WAL being written to.
// Previous files are closed after they're full.
func (wal *WAL) syncWALIfDirty() {
	// Fast path: check dirty flag without lock
	if !wal.dirty.Load() {
		return
	}

	// Clear dirty flag before sync (writes during sync will re-set it)
	wal.dirty.Store(false)

	// Get file handle under lock, but release BEFORE calling Sync()
	// This prevents blocking Write() operations during slow disk I/O
	var fileToSync *bufferedWALFile
	wal.mu.RLock()
	if len(wal.Shard.DB) > 0 {
		fileToSync = wal.Shard.DB[len(wal.Shard.DB)-1]
	}
	wal.mu.RUnlock()

	// Sync OUTSIDE of lock to prevent blocking writes (Sync() flushes buffer first)
	if fileToSync != nil {
		if err := fileToSync.Sync(); err != nil {
			slog.Error("WAL sync failed", "error", err)
			// Re-mark as dirty so next tick retries
			wal.dirty.Store(true)
		}
	}
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
	if stat.Size() < walHeaderSize {
		return 0, nil
	}

	return stat.Size() - walHeaderSize, nil
}

// ensureWALFile ensures we have a WAL file open and returns its index
// Caller must hold wal.mu lock
func (wal *WAL) ensureWALFile() (int, error) {
	// Note: Caller (Write) already holds the lock

	// If we have files open, check if current one has space
	if len(wal.Shard.DB) > 0 {
		activeWal := wal.Shard.DB[len(wal.Shard.DB)-1]
		stat, err := activeWal.Stat()
		if err != nil {
			return 0, err
		}

		// Check if current file has space
		// MaxFileSize() = ShardSize (data) + header overhead
		// We need to leave room for at least one fragment (32 bytes header + up to ChunkSize data)
		maxFragmentSize := int64(FragmentHeaderBytes + ChunkSize)
		if stat.Size()+maxFragmentSize <= wal.MaxFileSize() {
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
				//fmt.Println("Incrementing WalNum")
				//fmt.Println("wal.WalNum.Load()", wal.WalNum.Load())
				wal.WalNum.Add(1)
				//fmt.Println("wal.WalNum.Load()", wal.WalNum.Load())

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
	maxFragmentSize := int64(FragmentHeaderBytes + ChunkSize)
	maxFileSize := wal.MaxFileSize()
	if stat, err := os.Stat(filename); err == nil {
		// File exists, check if it has space for at least one fragment
		if stat.Size()+maxFragmentSize > maxFileSize {
			// File doesn't have enough space, we need a new file number
			return fmt.Errorf("WAL file %s doesn't have enough space (%d + %d > %d)",
				filename, stat.Size(), maxFragmentSize, maxFileSize)
		}
	}

	// Open or create the file
	// Removed syscall.O_SYNC - using periodic fsync (200ms) for better performance
	// This follows PostgreSQL, BadgerDB, RocksDB best practices for WAL writes
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0640)
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
		maxFragmentSize := int64(FragmentHeaderBytes + ChunkSize)
		if stat.Size()+maxFragmentSize > wal.MaxFileSize() {
			// File doesn't have enough space, close it and return error
			file.Close()
			return fmt.Errorf("WAL file %s doesn't have enough space (%d + %d > %d)",
				filename, stat.Size(), maxFragmentSize, wal.MaxFileSize())
		}
	}

	// Append the latest "hot" WAL file to the DB, wrapped with buffered writer
	wal.Shard.DB = append(wal.Shard.DB, newBufferedWALFile(file))

	// Write the state file to disk, current WAL, Shard Num, etc
	err = wal.SaveState()

	if err != nil {
		return fmt.Errorf("failed to save state: %v", err)
	}

	return nil
}

func (wal *WAL) Write(r io.Reader, totalSize int) (*WriteResult, error) {
	// Hold exclusive lock for entire write to prevent offset interleaving
	// This serializes writes but ensures correct offset tracking for reads
	wal.mu.Lock()
	defer wal.mu.Unlock()

	result := &WriteResult{
		WALFiles:  make([]WALFileInfo, 0),
		TotalSize: totalSize,
	}

	// Increment shard number once per object
	wal.ShardNum.Add(1)
	result.ShardNum = wal.ShardNum.Load()

	remaining := totalSize
	var actualBytesWritten int
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
			// (no lock needed - already holding exclusive lock from Write())
			newWALNum := wal.WalNum.Load()
			var newOffset int64 = 0
			if walIndex < len(wal.Shard.DB) {
				activeWal := wal.Shard.DB[walIndex]
				stat, err := activeWal.Stat()
				if err != nil {
					return nil, fmt.Errorf("failed to stat WAL file: %v", err)
				}
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
		// (no lock needed - already holding exclusive lock from Write())
		if currentWALIndex >= len(wal.Shard.DB) {
			return nil, errors.New("invalid WAL file index")
		}
		activeWal := wal.Shard.DB[currentWALIndex]
		stat, err := activeWal.Stat()
		if err != nil {
			return nil, fmt.Errorf("failed to stat WAL file: %v", err)
		}
		currentTotalSize := stat.Size()

		// Check if this chunk fits in current WAL file
		// MaxFileSize() = ShardSize (data) + header overhead
		// NOTE: On-disk payload is ALWAYS ChunkSize bytes (padded with zeros when actualChunkSize < ChunkSize),
		// so every fragment consumes a fixed size on disk.
		chunkSizeWithHeader := int64(FragmentHeaderBytes + ChunkSize)
		maxFileSize := wal.MaxFileSize()
		// Use strict check: if adding this chunk would exceed max file size, create a new file
		if currentTotalSize+chunkSizeWithHeader > maxFileSize {
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
			// (no lock needed - already holding exclusive lock from Write())
			newWALNum := wal.WalNum.Load()
			var newOffset int64 = 0
			if walIndex < len(wal.Shard.DB) {
				activeWal := wal.Shard.DB[walIndex]
				stat, err := activeWal.Stat()
				if err != nil {
					return nil, fmt.Errorf("failed to stat WAL file: %v", err)
				}
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

			result.WALFiles = append(result.WALFiles, WALFileInfo{
				WALNum: newWALNum,
				Offset: newOffset,
				Size:   0,
			})
			currentWALIndex = walIndex
			currentWALFileSize = 0

			// Re-verify the new file has space
			if currentWALIndex < len(wal.Shard.DB) {
				activeWal := wal.Shard.DB[currentWALIndex]
				stat, err := activeWal.Stat()
				if err != nil {
					return nil, fmt.Errorf("failed to stat WAL file: %v", err)
				}
				currentTotalSize = stat.Size()
				if currentTotalSize+chunkSizeWithHeader > maxFileSize {
					return nil, fmt.Errorf("new WAL file %d already too full: %d + %d > %d",
						currentWALIndex, currentTotalSize, chunkSizeWithHeader, maxFileSize)
				}
			}
		}

		// Write the fragment
		isLastFragment := remaining-actualChunkSize <= 0
		err = wal.writeFragment(currentWALIndex, result.ShardNum, shardFragment, chunk[:actualChunkSize], isLastFragment)
		if err != nil {
			return nil, fmt.Errorf("failed to write fragment: %v", err)
		}

		// Track size written for this object in current WAL file (from the offset)
		// header + fixed on-disk payload
		fragmentSize := int64(FragmentHeaderBytes + ChunkSize)
		currentWALFileSize += fragmentSize
		actualBytesWritten += actualChunkSize
		remaining -= actualChunkSize
		shardFragment++

		// Note: We don't check file size after writing because concurrent writes
		// can legitimately exceed the max file size slightly. The pre-write check
		// ensures we don't start writing if there's no room, but concurrent writes
		// may each pass the check and write, causing a slight overage. This is
		// acceptable as the data is still valid.
	}

	// Finalize last WAL file info
	if len(result.WALFiles) > 0 {
		result.WALFiles[len(result.WALFiles)-1].Size = currentWALFileSize
	}

	// Update TotalSize to reflect actual bytes written (may differ from expected if reader closed early)
	result.TotalSize = actualBytesWritten

	// Flush buffered writes to OS buffer so they're visible to readers.
	// This still benefits from batching (many fragments per flush) while ensuring
	// data visibility. Actual disk sync happens periodically via syncWALIfDirty.
	for _, db := range wal.Shard.DB {
		if err := db.Flush(); err != nil {
			return nil, fmt.Errorf("failed to flush WAL buffer: %w", err)
		}
	}

	return result, nil
}

// Update the ObjectToWAL record in the Badger DB
func (wal *WAL) UpdateObjectToWAL(objectHash [32]byte, result *WriteResult) error {

	objectWriteResult := ObjectWriteResult{
		Object:      objectHash,
		WriteResult: *result,
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(objectWriteResult); err != nil {
		return err
	}

	err := wal.DB.Set(objectHash[:], buf.Bytes())
	if err != nil {
		return fmt.Errorf("failed to update object to WAL: %v", err)
	}

	return nil
}

// UpdateShardToWAL stores a shard's metadata in the Badger DB using the provided key.
// This allows storing multiple shards of the same object under different keys.
func (wal *WAL) UpdateShardToWAL(shardKey []byte, objectHash [32]byte, result *WriteResult) error {
	objectWriteResult := ObjectWriteResult{
		Object:      objectHash,
		WriteResult: *result,
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(objectWriteResult); err != nil {
		return err
	}

	err := wal.DB.Set(shardKey, buf.Bytes())
	if err != nil {
		return fmt.Errorf("failed to update shard to WAL: %v", err)
	}

	return nil
}

// writeFragment writes a single fragment to the specified WAL file
// Caller must hold wal.mu lock (Write() holds it for the entire write operation)
func (wal *WAL) writeFragment(walIndex int, shardNum uint64, shardFragment uint32, chunk []byte, isLast bool) error {
	if walIndex >= len(wal.Shard.DB) {
		return errors.New("invalid WAL file index")
	}
	activeWal := wal.Shard.DB[walIndex]

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

	// Get a buffer from the pool to avoid allocation per fragment.
	// On disk, every fragment payload is exactly ChunkSize bytes (padded with zeros).
	// The header Length field stores the logical size.
	payload := fragmentBufferPool.Get().([]byte)

	// Write header fields directly to payload
	binary.BigEndian.PutUint64(payload[0:8], fragment.SeqNum)
	binary.BigEndian.PutUint64(payload[8:16], fragment.ShardNum)
	binary.BigEndian.PutUint32(payload[16:20], fragment.ShardFragment)
	binary.BigEndian.PutUint32(payload[20:24], uint32(fragment.Length))
	binary.BigEndian.PutUint32(payload[24:28], uint32(fragment.Flags))
	// Checksum field (28:32) MUST be zero for CRC calculation
	binary.BigEndian.PutUint32(payload[28:32], 0)

	// Add the data to the chunk
	copy(payload[FragmentHeaderBytes:FragmentHeaderBytes+len(chunk)], chunk)

	// Clear padding area after chunk data (important: previous data may remain).
	// Zero-padding is part of the CRC calculation.
	// Using copy() from zeroPadBuffer is faster than a byte-by-byte loop.
	paddingStart := FragmentHeaderBytes + len(chunk)
	copy(payload[paddingStart:], zeroPadBuffer[:len(payload)-paddingStart])

	// Checksum is calculated over: full 32-byte header (with checksum field set to 0) + full (padded) ChunkSize payload.
	checksum := crc32.ChecksumIEEE(payload[0:FragmentHeaderBytes])
	checksum = crc32.Update(checksum, crc32.IEEETable, payload[FragmentHeaderBytes:FragmentHeaderBytes+int(ChunkSize)])

	// Write checksum to header
	binary.BigEndian.PutUint32(payload[28:32], checksum)

	// Write to the WAL file
	_, err := activeWal.Write(payload)

	// Return buffer to pool immediately after write
	fragmentBufferPool.Put(payload)

	if err != nil {
		return fmt.Errorf("failed to write to WAL file: %v", err)
	}

	// Mark WAL as dirty for periodic sync goroutine
	wal.dirty.Store(true)

	return nil
}

func (wal *WAL) Close() (err error) {
	// Stop the WAL syncer goroutine (performs final sync before stopping)
	wal.StopWALSyncer()

	// Close the Badger DB
	defer wal.DB.Close()

	// Loop through each file
	for _, v := range wal.Shard.DB {
		// Final sync before close to ensure durability
		v.Sync()
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

			// Read the full on-disk chunk payload (fixed ChunkSize), then use fragment.Length as logical size.
			fullChunkBuffer := make([]byte, wal.Shard.ChunkSize)
			n, err := io.ReadFull(f, fullChunkBuffer)
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
			calculatedChecksum = crc32.Update(calculatedChecksum, crc32.IEEETable, fullChunkBuffer)

			if calculatedChecksum != fragment.Checksum {
				return nil, fmt.Errorf("checksum mismatch for fragment %d in WAL %d: expected %d, got %d", fragment.ShardFragment, walFile.WALNum, fragment.Checksum, calculatedChecksum)
			}

			// Copy to output buffer
			remaining := result.TotalSize - bytesRead
			copySize := payloadSize
			if copySize > remaining {
				copySize = remaining
			}
			copy(data[bytesRead:bytesRead+copySize], fullChunkBuffer[:copySize])
			bytesRead += copySize
			fileBytesRead += int64(FragmentHeaderBytes + n) // header + fixed payload

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

		// Read the full on-disk chunk payload (fixed ChunkSize), then use fragment.Length as logical size.
		fullChunkBuffer := make([]byte, wal.Shard.ChunkSize)

		n, err = io.ReadFull(f, fullChunkBuffer) // exactly ChunkSize bytes per fragment on disk

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
		calculatedChecksum = crc32.Update(calculatedChecksum, crc32.IEEETable, fullChunkBuffer)

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
			fullChunkBuffer[:payloadSize],
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

// The returned reader will yield result.TotalSize bytes (unless it errors early).
func (wal *WAL) ReadFromWriteResultStream(result *WriteResult) (io.Reader, error) {
	if result == nil {
		return nil, errors.New("nil WriteResult")
	}
	if len(result.WALFiles) == 0 {
		return nil, errors.New("no WAL files in WriteResult")
	}
	if result.TotalSize < 0 {
		return nil, fmt.Errorf("invalid TotalSize %d", result.TotalSize)
	}

	pr, pw := io.Pipe()

	go func() {
		// IMPORTANT: don't `defer pw.Close()` unconditionally.
		// If we hit an error and call CloseWithError, a later Close() may mask the error
		// and make the reader look like a clean EOF (0 bytes, nil error) depending on
		// implementation details. We explicitly close exactly once.
		var pipeErr error
		defer func() {
			if pipeErr != nil {
				_ = pw.CloseWithError(pipeErr)
				return
			}
			_ = pw.Close()
		}()

		var bytesWritten int

		// Reuse buffers to avoid per-fragment allocations.
		headerBuf := make([]byte, FragmentHeaderBytes)
		fullChunkBuffer := make([]byte, int(wal.Shard.ChunkSize))
		headerForChecksum := make([]byte, FragmentHeaderBytes)

		for _, walFile := range result.WALFiles {
			if walFile.Size < 0 {
				pipeErr = fmt.Errorf("invalid WALFileInfo.Size %d for WAL %d", walFile.Size, walFile.WALNum)
				return
			}

			f, err := os.Open(filepath.Join(wal.WalDir, FormatWalFile(walFile.WALNum)))
			if err != nil {
				pipeErr = fmt.Errorf("failed to open WAL file %d: %w", walFile.WALNum, err)
				return
			}

			// Ensure file is closed before moving to the next WAL segment.
			func() {
				defer f.Close()

				// Skip WAL header and seek to this object's start offset (Offset excludes WAL header).
				walHeaderSize := int64(wal.WALHeaderSize())
				if _, err := f.Seek(walHeaderSize+walFile.Offset, io.SeekStart); err != nil {
					pipeErr = fmt.Errorf("failed to seek in WAL %d: %w", walFile.WALNum, err)
					return
				}

				var fileBytesRead int64
				for fileBytesRead < walFile.Size {
					// Read fixed-size fragment header (32 bytes).
					if _, err := io.ReadFull(f, headerBuf); err != nil {
						pipeErr = fmt.Errorf("could not read fragment header from WAL %d: %w", walFile.WALNum, err)
						return
					}

					seqNum := binary.BigEndian.Uint64(headerBuf[0:8])
					shardNum := binary.BigEndian.Uint64(headerBuf[8:16])
					shardFragment := binary.BigEndian.Uint32(headerBuf[16:20])
					length := binary.BigEndian.Uint32(headerBuf[20:24])
					flags := Flags(binary.BigEndian.Uint32(headerBuf[24:28]))
					checksum := binary.BigEndian.Uint32(headerBuf[28:32])

					_ = seqNum // currently unused, but kept for symmetry/debuggability

					// Sanity checks (same intent as ReadFromWriteResult).
					if shardNum != result.ShardNum {
						pipeErr = fmt.Errorf(
							"shard num mismatch in WAL %d: expected %d, got %d",
							walFile.WALNum, result.ShardNum, shardNum,
						)
						return
					}
					if length > wal.Shard.ChunkSize {
						pipeErr = fmt.Errorf(
							"chunk length %d exceeds max %d in WAL %d",
							length, wal.Shard.ChunkSize, walFile.WALNum,
						)
						return
					}

					// Read full on-disk payload (fixed ChunkSize).
					if _, err := io.ReadFull(f, fullChunkBuffer); err != nil {
						pipeErr = fmt.Errorf("could not read chunk from WAL %d: %w", walFile.WALNum, err)
						return
					}

					// Validate checksum (same method as ReadFromWriteResult):
					// CRC(header-with-checksum-zeroed + full padded payload).
					copy(headerForChecksum, headerBuf)
					headerForChecksum[28], headerForChecksum[29], headerForChecksum[30], headerForChecksum[31] = 0, 0, 0, 0

					calculated := crc32.ChecksumIEEE(headerForChecksum)
					calculated = crc32.Update(calculated, crc32.IEEETable, fullChunkBuffer)
					if calculated != checksum {
						pipeErr = fmt.Errorf(
							"checksum mismatch for fragment %d in WAL %d: expected %d, got %d",
							shardFragment, walFile.WALNum, checksum, calculated,
						)
						return
					}

					// Stream logical bytes into the pipe.
					remaining := result.TotalSize - bytesWritten
					if remaining < 0 {
						pipeErr = fmt.Errorf("wrote past TotalSize (TotalSize=%d)", result.TotalSize)
						return
					}

					toWrite := int(length)
					if toWrite > remaining {
						toWrite = remaining
					}

					if toWrite > 0 {
						if _, err := pw.Write(fullChunkBuffer[:toWrite]); err != nil {
							// Reader side likely closed early.
							pipeErr = err
							return
						}
						bytesWritten += toWrite
					}

					// Track on-disk consumption for this segment.
					fileBytesRead += int64(FragmentHeaderBytes) + int64(wal.Shard.ChunkSize)

					// End-of-shard validation (same intent as ReadFromWriteResult).
					if flags&FlagEndOfShard != 0 {
						if bytesWritten != result.TotalSize {
							pipeErr = fmt.Errorf(
								"end-of-shard set in WAL %d but wrote %d/%d bytes",
								walFile.WALNum, bytesWritten, result.TotalSize,
							)
							return
						}
						return
					}
				}
			}()
		}

		// If we exhausted WAL segments, we must have produced TotalSize.
		if bytesWritten != result.TotalSize {
			pipeErr = fmt.Errorf("wrote %d bytes but expected %d", bytesWritten, result.TotalSize)
			return
		}
	}()

	return pr, nil
}

func numFragments(size int) int {
	if size == 0 {
		return 0
	}
	return (size + int(ChunkSize) - 1) / int(ChunkSize)
}

func ReadChunk(r io.Reader) (chunk []byte, err error) {
	buf := make([]byte, ChunkSize)
	totalRead := 0

	// Keep reading until we have a full chunk or hit EOF
	for totalRead < int(ChunkSize) {
		n, err := r.Read(buf[totalRead:])
		totalRead += n

		if err == io.EOF {
			if totalRead > 0 {
				return buf[:totalRead], nil
			}
			return nil, nil // finished cleanly
		}

		if err != nil {
			return nil, err // read error
		}
	}

	return buf[:totalRead], nil
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

// MaxFileSize returns the maximum WAL file size including all headers.
// ShardSize is the max DATA size; the actual file is larger due to:
// - WAL header (14 bytes)
// - Fragment headers (32 bytes per 8KB chunk)
func (wal *WAL) MaxFileSize() int64 {
	dataSize := int64(wal.Shard.ShardSize)
	numFragments := (dataSize + int64(wal.Shard.ChunkSize) - 1) / int64(wal.Shard.ChunkSize)
	headerOverhead := int64(wal.WALHeaderSize()) + numFragments*int64(FragmentHeaderBytes)
	return dataSize + headerOverhead
}

func FormatWalFile(seqNum uint64) string {
	return fmt.Sprintf("%016d.wal", seqNum)
}
