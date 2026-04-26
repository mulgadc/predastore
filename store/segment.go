package store

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
)

const segmentFilename = "%016d.seg"

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
	segHeaderSize  = 14
	fragHeaderSize = 32
	fragBodySize   = 8 * KiB
	totalFragSize  = fragHeaderSize + fragBodySize
	maxSegSize     = 4 * GiB
)

type segmentFlags uint32

const (
	flagFull segmentFlags = 1 << iota
)

type shardFlags uint32

const (
	flagDeleted shardFlags = 1 << iota
)

type fragFlags uint32

const (
	flagEndOfShard fragFlags = 1 << iota
)

type segment struct {
	file *os.File
	refs atomic.Int32

	closed bool
}

func (store *Store) getSegment(num uint64) (seg *segment, err error) {
	if seg, ok := store.segCache[num]; ok {
		return seg, nil
	}

	seg, err = openSegment(store.dir, num)
	if err != nil {
		return nil, err
	}

	store.segCache[num] = seg

	return seg, nil
}

func openSegment(dir string, num uint64) (seg *segment, err error) {
	path := filepath.Join(dir, fmt.Sprintf(segmentFilename, num))

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			if closeErr := file.Close(); closeErr != nil {
				slog.Warn("failed to close segment",
					"segNum", num,
					"error", closeErr,
				)
			}
		}
	}()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	switch {
	// New file: write the segment header.
	case info.Size() == 0:
		header := make([]byte, segHeaderSize)
		copy(header[0:4], magic[:])                             // [0:4]   Magic bytes
		binary.BigEndian.PutUint16(header[4:6], v1)             // [4:6]   File version
		binary.BigEndian.PutUint32(header[6:10], uint32(0))     // [6:10]  Flags
		binary.BigEndian.PutUint32(header[10:segHeaderSize], 0) // [6:14]  Empty

		if _, err = file.WriteAt(header, 0); err != nil {
			return nil, fmt.Errorf("write header: %w", err)
		}

	// Existing file: validate segment header.
	default:
		header := make([]byte, segHeaderSize)
		if _, err = file.ReadAt(header, 0); err != nil {
			return nil, fmt.Errorf("read header: %w", err)
		}

		var fileMagic [4]byte
		copy(fileMagic[:], header[0:4])
		if fileMagic != magic {
			return nil, fmt.Errorf("invalid magic %x", fileMagic)
		}
	}

	seg = &segment{
		file:   file,
		closed: false,
	}

	return seg, nil
}

func (seg *segment) isFull() (bool, error) {
	if seg.closed {
		return false, fmt.Errorf("segment closed")
	}

	buf := make([]byte, 4)
	if _, err := seg.file.ReadAt(buf, 6); err != nil {
		return false, err
	}

	flags := segmentFlags(binary.BigEndian.Uint32(buf[:]))

	return flags&flagFull != 0, nil
}

func (seg *segment) markFull() error {
	if seg.closed {
		return fmt.Errorf("segment closed")
	}

	buf := make([]byte, 4)
	if _, err := seg.file.ReadAt(buf, 6); err != nil {
		return err
	}

	flags := segmentFlags(binary.BigEndian.Uint32(buf[:]))
	binary.BigEndian.PutUint32(buf[:], uint32(flags|flagFull))

	if _, err := seg.file.WriteAt(buf, 6); err != nil {
		return err
	}

	return nil
}

func (seg *segment) Size() (int64, error) {
	if seg.closed {
		return 0, fmt.Errorf("segment closed")
	}

	info, err := seg.file.Stat()
	if err != nil {
		return 0, err
	}

	return info.Size(), nil
}
