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

// segment is a pointer to single file used by the Store to persist
// appended data to disk. It is broken into "slots". Each beginning with
// 32 bytes of header information.
type segment struct {
	num     uint64
	version uint16

	file *os.File
	refs atomic.Int32

	slotsTotal int
	slotsUsed  int
}

func openSegment(dir string, num uint64) (seg *segment, err error) {
	seg = &segment{
		num:        num,
		slotsTotal: slotsPerSegment,
	}

	// Build segment file path.
	path := filepath.Join(dir, fmt.Sprintf("%016d%s", num, extension))

	// Open or create the file.
	seg.file, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		slog.Error("store: openSegment: open file", "error", err)
		return nil, err
	}

	// Close the file if an error is encountered.
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
	// File has no free slots, return.
	case stat.Size()+totalSlotSize > seg.maxFileSize():
		return nil, fmt.Errorf("store: openSegment: segment full %s (%d + %d > %d)",
			path, stat.Size(), totalSlotSize, seg.maxFileSize())

	// File is empty, write segment header to file.
	case stat.Size() == 0:
		header := make([]byte, segmentHeaderSize)
		copy(header[0:4], magic[:])
		binary.BigEndian.PutUint16(header[4:6], v1)
		binary.BigEndian.PutUint32(header[6:10], uint32(seg.maxFileSize())) //nolint:gosec // G115: maxFileSize ~1 GiB, fits uint32
		binary.BigEndian.PutUint32(header[10:14], uint32(slotPayloadSize))

		_, err = seg.file.Write(header)
		if err != nil {
			slog.Error("store: openSegment: write header", "error", err)
			return nil, err
		}
		seg.version = v1

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
		seg.slotsUsed = int((stat.Size() - segmentHeaderSize) / totalSlotSize)
	}

	seg.refs.Add(1)

	return seg, nil
}

// Calculate the maxiumum file size in bytes of this segment including all headers.
func (seg *segment) maxFileSize() int64 {
	return int64(segmentHeaderSize + totalSlotSize*seg.slotsTotal)
}

func (seg *segment) freeSlots() int {
	return seg.slotsTotal - seg.slotsUsed
}

func (seg *segment) byteOffset(slotOffset int) int64 {
	return int64(segmentHeaderSize + slotOffset*totalSlotSize)
}

// Close the underlying file if the segment is full and no writers or
// readers hold references to it.
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
