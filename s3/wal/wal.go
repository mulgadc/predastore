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
const ShardSize uint32 = 1024 * 1024 * 4

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
	SeqNum   atomic.Uint64
	ShardNum atomic.Uint64
	Epoch    time.Time

	WalDir string

	Shard Shard
	mu    sync.RWMutex `json:"-"`
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

func New(stateFile, walDir string) (wal *WAL, err error) {

	_, err = os.Stat(stateFile)

	// State
	wal = &WAL{
		WalDir: walDir,

		Shard: Shard{
			// S3 Shard File (S3SF)
			Magic:     [4]byte{'S', '3', 'S', 'F'},
			Version:   1,
			ShardSize: ShardSize,
			ChunkSize: ChunkSize,
		},
	}

	// If no file exists, start from 0
	if err != nil {
		wal.SeqNum.Add(1)
	} else {

		// Read the state file
		stateData, err := os.ReadFile(stateFile)

		if err != nil {
			slog.Error("Cannot read stateFile", "stateFile", stateFile)
			return wal, err
		}

		err = json.Unmarshal(stateData, &wal)

		if err != nil {
			slog.Error("Cannot unmarshal stateFile", "stateFile", stateFile)

			return wal, err
		}

	}

	// Next, check if a WAL already exists
	seqNum := wal.SeqNum.Load()
	filename := filepath.Join(walDir, FormatWalFile(seqNum))
	_, err = os.Stat(filename)

	if err != nil {

		err = wal.CreateWAL(filename)
		if err != nil {
			return wal, err
		}

	}

	return wal, err
}

func (wal *WAL) CreateWAL(filename string) (err error) {

	// Lock operations on the WAL
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// Create the directory if it doesn't exist
	os.MkdirAll(filepath.Dir(filename), 0750)

	// Create the file if it doesn't exist, make sure writes and committed immediately
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR|syscall.O_SYNC, 0640)

	// Append the WAL header, format
	// Check our type
	var headers []byte
	headers = wal.WALHeader()

	_, err = file.Write(headers)

	if err != nil {
		slog.Error("Could not write headers")
		return err
	}

	// Append the latest "hot" WAL file to the DB
	wal.Shard.DB = append(wal.Shard.DB, file)

	//slog.Debug("OpenWAL complete, new WAL", "file", *file)

	return

}

func (wal *WAL) Write(r io.Reader, totalSize int) (n int, err error) {

	//wal.mu.Lock()
	//defer wal.mu.Lock()

	//numFragments = numFragments(size)

	remaining := totalSize
	var shardFragment uint32

	// Increment shard number
	wal.ShardNum.Add(1)

	for remaining > 0 {

		// Read in 8kb chunks (or less for last chunk)
		chunk, err := ReadChunk(r)

		//slog.Debug("chunk len", "len", len(chunk), "remaining", remaining)

		if len(chunk) == 0 {
			break
		}

		if err != nil {
			slog.Error("ReadChunk failed", "err", err)
			return n, err
		}

		// Clamp chunk size to remaining bytes
		actualChunkSize := len(chunk)
		if actualChunkSize > remaining {
			actualChunkSize = remaining
			chunk = chunk[:actualChunkSize]
		}

		fragment := Fragment{}

		fragment.SeqNum = wal.SeqNum.Load()
		fragment.ShardNum = wal.ShardNum.Load()

		// TODO: Read buffer, calculate segment
		fragment.ShardFragment = shardFragment

		fragment.Length = uint32(actualChunkSize)

		// Calculate a CRC32 checksum of the block data and headers

		payload := make([]byte, fragment.FragmentHeaderSize()+actualChunkSize)

		headers := fragment.FragmentHeader()

		copy(payload[0:len(headers)], headers)

		binary.BigEndian.PutUint64(payload[0:8], fragment.SeqNum)
		binary.BigEndian.PutUint64(payload[8:16], fragment.ShardNum)
		binary.BigEndian.PutUint32(payload[16:20], fragment.ShardFragment)
		binary.BigEndian.PutUint32(payload[20:24], uint32(fragment.Length))
		binary.BigEndian.PutUint32(payload[24:28], uint32(fragment.Flags))

		checksum_validated := crc32.ChecksumIEEE(payload[0:len(headers)])
		// Add the chunk, to calculate the checksum
		checksum_validated = crc32.Update(checksum_validated, crc32.IEEETable, chunk)

		fragment.Checksum = checksum_validated

		payload = fragment.AppendChecksum(payload)

		expectedSize := actualChunkSize + 32
		if expectedSize != len(payload) {
			slog.Error("Chunk size mismatch", "expectedSize", expectedSize, "len", len(payload))
			return 0, errors.New("Chunk size mismatch")
		}

		// Add the data to the chunk
		copy(payload[32:32+actualChunkSize], chunk)

		// Write to the active WAL
		activeWal := len(wal.Shard.DB) - 1
		wal.Shard.DB[activeWal].Write(payload)

		// Increment bytes read/write
		n += actualChunkSize
		remaining -= actualChunkSize

		// Increment shardFragment
		shardFragment++

	}

	return n, err

}

func (wal *WAL) Close() {

	// Loop through each file

	for _, v := range wal.Shard.DB {

		v.Close()

	}
}

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

		// Bounds-safe copy
		copy(
			data[bytesRead:bytesRead+uint32(payloadSize)],
			chunkBuffer[:payloadSize],
		)

		remaining -= uint32(payloadSize)
		bytesRead += uint32(payloadSize)
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
	// Magic bytes (4) + Version (2) + BlockSize (4) + Timestamp (8)
	return len(wal.Shard.Magic) + binary.Size(wal.Shard.Version) + binary.Size(wal.Shard.ShardSize) + binary.Size(wal.Shard.ChunkSize)
}

func FormatWalFile(seqNum uint64) string {
	return fmt.Sprintf("%016d.wal", seqNum)
}
