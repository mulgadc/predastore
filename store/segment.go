package store

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
)

const extension = ".seg"

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
	slotHeaderSize = 32
	fragSize       = 8 * KiB
	slotSize       = slotHeaderSize + fragSize
	maxSegSize     = 4 * GiB
)

type segmentFlags uint32

const (
	flagSegFull slotFlags = 1 << iota
)

type objectFlags uint32

const (
	flagObjDeleted objectFlags = 1 << iota
)

type slotFlags uint32

const (
	flagSlotLast slotFlags = 1 << iota
)

type segment struct {
	fd   *os.File
	refs atomic.Int32

	closed bool
}

func (st *Store) getSegment(num segNum) (seg *segment, err error) {
	if seg, ok := st.segs[num]; ok {
		return seg, nil
	}

	seg, err = openSegment(st.dir, num)
	if err != nil {
		return nil, err
	}

	st.segs[num] = seg

	return seg, nil
}

func openSegment(dir string, num segNum) (seg *segment, err error) {
	path := filepath.Join(dir, fmt.Sprintf("%016d%s", num, extension))

	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

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

		if _, err = file.Write(header); err != nil {
			return nil, fmt.Errorf("write seg %d header: %w", num, err)
		}

	// Existing file: validate segment header.
	default:
		header := make([]byte, segHeaderSize)
		if _, err = file.ReadAt(header, 0); err != nil {
			return nil, fmt.Errorf("read seg %d header: %w", num, err)
		}

		var fileMagic [4]byte
		copy(fileMagic[:], header[0:4])
		if fileMagic != magic {
			return nil, fmt.Errorf("read seg %d header: invalid magic %x", num, fileMagic)
		}
	}

	seg = &segment{
		fd:     file,
		closed: false,
	}

	return seg, nil
}

func (seg *segment) isFull() (bool, error) {
	buf := make([]byte, 4)
	if _, err := seg.fd.ReadAt(buf, 6); err != nil {
		return false, err
	}

	flags := binary.BigEndian.Uint32(buf[:])

	return flags&uint32(flagSegFull) != 0, nil
}

func (seg *segment) markFull() error {
	buf := make([]byte, 4)
	if _, err := seg.fd.ReadAt(buf, 6); err != nil {
		return err
	}

	flags := binary.BigEndian.Uint32(buf[:])
	binary.BigEndian.PutUint32(buf[:], flags|uint32(flagSegFull)) // [6:10]  Flags

	if _, err := seg.fd.WriteAt(buf, 6); err != nil {
		return err
	}

	return nil
}

func (seg *segment) Size() (int64, error) {
	if seg.closed {
		return 0, fmt.Errorf("segment is closed")
	}

	info, err := seg.fd.Stat()
	if err != nil {
		return 0, err
	}

	return info.Size(), nil
}
