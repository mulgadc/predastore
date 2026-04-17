package store

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
)

const extension = ".seg"

// Magic bytes used to identify a file as a segment.
var magic = [4]byte{'S', '3', 'S', 'F'}

const (
	_ uint16 = iota
	v1
)

const (
	_          = iota
	KiB uint64 = 1 << (10 * iota)
	MiB
	GiB
)

const (
	// segmentHeaderSize is the on-disk segment file header length.
	segmentHeaderSize uint64 = 14

	// slotHeaderSize is the on-disk slot header length.
	slotHeaderSize uint64 = 32

	// slotPayloadSize is the payload length of one slot on-disk.
	slotPayloadSize uint64 = 8 * KiB

	// totalSlotSize is the on-disk size of each slot in a segment file.
	totalSlotSize uint64 = slotHeaderSize + slotPayloadSize
)

// slotFlags is the per-slot feature flag bitmask.
type slotFlags uint32

const (
	flagNone         slotFlags = 0
	flagEndOfShard   slotFlags = 1 << 0
	flagShardHeader  slotFlags = 1 << 1
	flagCompressed   slotFlags = 1 << 2
	flagDeleted      slotFlags = 1 << 3
	flagPartialWrite slotFlags = 1 << 4
	flagReserved1    slotFlags = 1 << 5
	flagReserved2    slotFlags = 1 << 6
	flagReserved3    slotFlags = 1 << 7
)

// segment is a pointer to single file used by the Store to persist
// appended data to disk. It is broken into "slots". Each beginning with
// 32 bytes of header information.
type segment struct {
	version uint16

	file     *os.File
	refCount atomic.Int32

	slotsTotal uint64
	slotsUsed  uint64
}

// allocation is a contiguous range of slots within a single segment reserved
// for the contents of a corresponding call to Store.append().
type allocation struct {
	seg    *segment
	offset uint64
	len    uint64
}

func openSegment(dir string, num uint64) (seg *segment, err error) {
	// Build segment file path.
	path := filepath.Join(dir, fmt.Sprintf("%016d%s", num, extension))

	// Open or create the file.
	// Removed syscall.O_SYNC - calling fsync explicitly for better performance.
	seg.file, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		slog.Error("failed to open segment file", "error", err)
		return nil, err
	}

	// Close the file if an error is encountered.
	defer func() {
		if err != nil {
			if closeErr := seg.file.Close(); closeErr != nil {
				slog.Debug("failed to close segment file", "error", closeErr)
			}
		}
	}()

	stat, err := seg.file.Stat()
	if err != nil {
		slog.Error("failed to stat segment file", "error", err)
		return nil, err
	}

	switch {
	// File doesn't has no free slots, return.
	case stat.Size()+int64(totalSlotSize) > seg.maxFileSize():
		return nil, fmt.Errorf("segment %s doesn't have enough space (%d + %d > %d)",
			path, stat.Size(), totalSlotSize, seg.maxFileSize())

	// File is empty, write segment header to file.
	case stat.Size() == 0:
		header := make([]byte, segmentHeaderSize)
		copy(header[0:4], magic[:])
		binary.BigEndian.PutUint16(header[4:6], v1)
		binary.BigEndian.PutUint32(header[6:10], uint32(seg.maxFileSize()))
		binary.BigEndian.PutUint32(header[10:14], uint32(slotPayloadSize))

		_, err = seg.file.Write(header)
		if err != nil {
			slog.Error("failed to write segment file header", "error", err)
			return nil, err
		}
	}

	return seg, nil
}

// Calculate the maxiumum file size in bytes of this segment including all headers.
func (s *segment) maxFileSize() int64 {
	return int64(segmentHeaderSize + totalSlotSize*s.slotsTotal)
}

// Get the byte-aligned offset of a slot.
func (s *segment) byteOffset(slotOffset uint64) uint64 {
	return segmentHeaderSize + slotOffset*totalSlotSize
}

// Close the underlying file if the segment is full and no writers or
// readers hold references to it.
func (s *segment) tryClose() {
	panic("store: segment.tryClose not implemented")
}
