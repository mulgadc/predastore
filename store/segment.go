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

// magic identifies a file as a store segment.
var magic = [4]byte{'S', '3', 'S', 'F'}

const (
	_ uint16 = iota
	v1
)

const (
	KiB = 1024
	MiB = 1024 * KiB
	GiB = 1024 * MiB
)

const (
	segmentHeaderSize = 14
	slotHeaderSize    = 32
	slotPayloadSize   = 8 * KiB
	totalSlotSize     = slotHeaderSize + slotPayloadSize
	slotsPerSegment   = 1 * GiB / slotPayloadSize
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

// segment represents a single segment file on disk, divided into fixed-size slots.
type segment struct {
	num     uint64
	version uint16

	file *os.File
	refs atomic.Int32

	slotsTotal int
	slotsUsed  int
}

// openSegment opens or creates the segment file for the given number and adds one reference for the Store.
func openSegment(dir string, num uint64) (seg *segment, err error) {
	seg = &segment{
		num:        num,
		slotsTotal: slotsPerSegment,
	}

	path := filepath.Join(dir, fmt.Sprintf("%016d%s", num, extension))

	seg.file, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		slog.Error("store: openSegment: open file", "error", err)
		return nil, err
	}

	// Clean up the file handle on error.
	defer func() {
		if err != nil {
			if closeErr := seg.file.Close(); closeErr != nil {
				slog.Debug("store: openSegment: close file on error", "error", closeErr)
			}
		}
	}()

	stat, err := seg.file.Stat()
	if err != nil {
		slog.Error("store: openSegment: stat file", "error", err)
		return nil, err
	}

	switch {
	// Segment full.
	case stat.Size()+totalSlotSize > seg.maxFileSize():
		return nil, fmt.Errorf("store: openSegment: segment full %s (%d + %d > %d)",
			path, stat.Size(), totalSlotSize, seg.maxFileSize())

	// New file: write the 14-byte segment header.
	case stat.Size() == 0:
		header := make([]byte, segmentHeaderSize)
		copy(header[0:4], magic[:])                                         // [0:4]   magic
		binary.BigEndian.PutUint16(header[4:6], v1)                         // [4:6]   version
		binary.BigEndian.PutUint32(header[6:10], uint32(seg.maxFileSize())) //nolint:gosec // G115: maxFileSize ~1 GiB, fits uint32 // [6:10]  max file size
		binary.BigEndian.PutUint32(header[10:14], uint32(slotPayloadSize))  // [10:14] slot payload size

		_, err = seg.file.Write(header)
		if err != nil {
			slog.Error("store: openSegment: write header", "error", err)
			return nil, err
		}
		seg.version = v1

	// Existing file: validate header and derive slot usage.
	default:
		header := make([]byte, segmentHeaderSize)
		if _, err = seg.file.ReadAt(header, 0); err != nil {
			return nil, fmt.Errorf("store: openSegment: read header: %w", err)
		}
		var fileMagic [4]byte
		copy(fileMagic[:], header[0:4])
		if fileMagic != magic {
			return nil, fmt.Errorf("store: openSegment: invalid magic %x in %s", fileMagic, path)
		}
		seg.version = binary.BigEndian.Uint16(header[4:6])
		// Derive slot count from file size minus the fixed header.
		seg.slotsUsed = int((stat.Size() - segmentHeaderSize) / totalSlotSize)
	}

	// Add the Store's own reference.
	seg.refs.Add(1)

	return seg, nil
}

// maxFileSize returns the maximum file size in bytes, including all headers.
func (seg *segment) maxFileSize() int64 {
	return int64(segmentHeaderSize + totalSlotSize*seg.slotsTotal)
}

// freeSlots returns the number of unallocated slots remaining in the segment.
func (seg *segment) freeSlots() int {
	return seg.slotsTotal - seg.slotsUsed
}

// byteOffset returns the absolute file offset for the given slot index.
func (seg *segment) byteOffset(slotOffset int) int64 {
	return int64(segmentHeaderSize + slotOffset*totalSlotSize)
}

// Close closes the underlying file. It returns an error if the segment
// still has references or free slots.
func (seg *segment) Close() error {
	if seg.refs.Load() != 0 {
		return fmt.Errorf("store: segment: in use")
	} else if seg.freeSlots() > 0 {
		return fmt.Errorf("store: segment: has free slots")
	} else if err := seg.file.Close(); err != nil {
		return err
	}

	return nil
}
