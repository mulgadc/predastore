package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
)

const segFilename = "%016d.seg"

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

// Segment layout:
//
//	[0:14]  segment header (magic, version, flags, reserved)
//	[14:…]  sequence of fixed-size fragments (fragHeaderSize + fragBodySize each)
//
// Fragment header layout (32 bytes):
//
//	[0:8]   fragNum   — global fragment counter (monotonic across segments)
//	[8:16]  shardNum  — shard identifier
//	[16:20] reserved
//	[20:24] payloadLen — actual data bytes in this fragment's body (≤ fragBodySize)
//	[24:28] flags     — fragFlags (flagEndOfShard marks the last fragment of a shard)
//	[28:32] crc32     — IEEE CRC over the entire fragment with this field zeroed
const (
	segHeaderSize     = 14
	fragHeaderSize    = 32
	fragBodySize      = 8 * KiB
	totalFragSize     = fragHeaderSize + fragBodySize
	DefaultMaxSegSize = 4 * GiB
)

type segmentFlags uint32

const (
	flagFull segmentFlags = 1 << iota
)

type shardFlags uint32 //nolint:unused // reserved for tombstone support.

const (
	flagDeleted shardFlags = 1 << iota //nolint:unused // reserved for tombstone support.
)

type fragFlags uint32

const (
	flagEndOfShard fragFlags = 1 << iota
)

// file is the subset of *os.File that segments depend on. Tests swap
// openFile to substitute a fault-injecting wrapper.
type file interface {
	io.ReaderAt
	io.WriterAt

	Truncate(size int64) error
	Sync() error
	Stat() (os.FileInfo, error)
	Close() error
}

// openFile is the package-level opener used by openSegment. Production code
// uses os.OpenFile; tests override via export_test.go.
var openFile = func(path string) (file, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
}

// segment is an open segment file handle with a reference count. The refs
// counter tracks active readers and writers; Store.Close waits for all refs
// to drain before closing the file descriptor.
type segment struct {
	file

	refs atomic.Int32
}

// getSegment returns a cached segment or opens it from disk. Callers must hold
// store.mutex.
func (store *Store) getSegment(num uint64) (*segment, error) {
	if seg, ok := store.segCache[num]; ok {
		return seg, nil
	}

	seg, err := openSegment(store.dir, num)
	if err != nil {
		return nil, err
	}

	store.segCache[num] = seg

	return seg, nil
}

func (store *Store) getNextSegment(attempts int, guard func(*segment) error) (seg *segment, err error) {
	for attempt := range attempts {
		seg, err = func() (*segment, error) {
			seg, err := store.getSegment(store.segNum)
			if err != nil {
				return nil, err
			}

			full, err := seg.isFull()
			if err != nil {
				return nil, err
			} else if full {
				return nil, errors.New("segment full")
			}

			err = guard(seg)
			if err != nil {
				return nil, err
			}

			return seg, nil
		}()

		if err == nil {
			return seg, nil
		}

		if attempt < attempts-1 {
			slog.Debug("rotating segment",
				"segNum", store.segNum,
				"attempt", attempt,
				"error", err,
			)

			store.segNum += 1
			continue
		}
	}

	return nil, fmt.Errorf("get segment after %d attempts: %w", attempts, err)
}

func openSegment(dir string, num uint64) (*segment, error) {
	path := filepath.Join(dir, fmt.Sprintf(segFilename, num))

	f, err := openFile(path)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			if closeErr := f.Close(); closeErr != nil {
				slog.Warn("failed to close segment",
					"segNum", num,
					"error", closeErr,
				)
			}
		}
	}()

	info, err := f.Stat()
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

		if _, err = f.WriteAt(header, 0); err != nil {
			return nil, fmt.Errorf("write header: %w", err)
		}

	// Existing file: validate segment header.
	default:
		header := make([]byte, segHeaderSize)
		if _, err = f.ReadAt(header, 0); err != nil {
			return nil, fmt.Errorf("read header: %w", err)
		}

		var fileMagic [4]byte
		copy(fileMagic[:], header[0:4])
		if fileMagic != magic {
			return nil, fmt.Errorf("invalid magic %x", fileMagic)
		}
	}

	return &segment{file: f}, nil
}

// isFull reads the segment header flags and returns whether flagFull is set.
func (seg *segment) isFull() (bool, error) {
	buf := make([]byte, 4)
	if _, err := seg.ReadAt(buf, 6); err != nil {
		return false, err
	}

	flags := segmentFlags(binary.BigEndian.Uint32(buf[:]))

	return flags&flagFull != 0, nil
}

// markFull sets flagFull in the segment header. Once set, subsequent calls to
// isFull return true and the store will roll to the next segment.
func (seg *segment) markFull() error {
	buf := make([]byte, 4)
	if _, err := seg.ReadAt(buf, 6); err != nil {
		return err
	}

	flags := segmentFlags(binary.BigEndian.Uint32(buf[:]))
	binary.BigEndian.PutUint32(buf[:], uint32(flags|flagFull))

	if _, err := seg.WriteAt(buf, 6); err != nil {
		return err
	}

	return nil
}
