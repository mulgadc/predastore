package store

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

const segFilename = "%016d.seg"

// magic identifies the encryption-at-rest segment format. There is no in-place
// migration from older formats — openSegment rejects them so the operator is
// forced to start with a fresh data dir.
var magic = [4]byte{'S', '3', 'S', 'E'}

const (
	_ uint16 = iota
	v1
)

// Segment layout:
//
//	[0:4]   magic
//	[4:6]   version
//	[6:10]  flags (segmentFlags)
//	[10:14] reserved
//	[14:…]  sequence of fixed-size fragments (see fragment.go for layout)
const (
	segHeaderSize     = 14
	segFlagsOffset    = 6
	segFlagsSize      = 4
	DefaultMaxSegSize = 4 * GiB

	// maxSegmentScanAttempts caps the linear walk for a non-full segment used
	// during Open recovery and Append rolls. A handful is normal; 100 is a
	// blow-the-whistle bound that catches a runaway full-flag bug rather than
	// silently scanning thousands of files.
	maxSegmentScanAttempts = 100
)

type segmentFlags uint32

const (
	flagFull segmentFlags = 1 << iota
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

// segment is an open segment file handle with a reference count. refs tracks
// active readers and writers; Store.Close calls refs.Wait to drain them
// before closing the file descriptor. The WaitGroup is safe because addRef is
// only called from Lookup/Append (which hold store.mutex), and Close flips
// store.closed under the same mutex before waiting — so no new addRef can
// race with Wait. full caches the on-disk full flag so the hot Append path
// avoids a ReadAt per call.
type segment struct {
	file

	refs sync.WaitGroup
	full atomic.Bool
}

func (seg *segment) addRef()      { seg.refs.Add(1) }
func (seg *segment) releaseRef()  { seg.refs.Done() }
func (seg *segment) waitForRefs() { seg.refs.Wait() }

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

// nextAvailableSegment returns the first non-full segment at or after
// store.segNum, advancing store.segNum past any full segments it walks
// over. Callers must hold store.mutex.
func (store *Store) nextAvailableSegment() (*segment, error) {
	for range maxSegmentScanAttempts {
		seg, err := store.getSegment(store.segNum)
		if err != nil {
			return nil, fmt.Errorf("get segment %d: %w", store.segNum, err)
		}

		if !seg.full.Load() {
			return seg, nil
		}

		slog.Debug("segment full, rolling", "segNum", store.segNum)
		store.segNum++
	}

	return nil, fmt.Errorf("no non-full segment in %d attempts starting at %d", maxSegmentScanAttempts, store.segNum-maxSegmentScanAttempts)
}

// rollSegment advances past the current segment unconditionally and opens
// the next one. Used when the current segment is non-full but can't fit the
// incoming shard. Callers must hold store.mutex.
func (store *Store) rollSegment() (*segment, error) {
	store.segNum++
	return store.getSegment(store.segNum)
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

	seg := &segment{file: f}

	switch {
	// New file: write the segment header.
	case info.Size() == 0:
		header := make([]byte, segHeaderSize)
		copy(header[0:4], magic[:])                  // [0:4]  magic
		binary.BigEndian.PutUint16(header[4:6], v1)  // [4:6]  version
		binary.BigEndian.PutUint32(header[6:10], 0)  // [6:10] flags
		binary.BigEndian.PutUint32(header[10:14], 0) // [10:14] reserved

		if _, err = f.WriteAt(header, 0); err != nil {
			return nil, fmt.Errorf("write header: %w", err)
		}

	// Existing file: validate magic and seed the full-flag cache.
	default:
		header := make([]byte, segHeaderSize)
		if _, err = f.ReadAt(header, 0); err != nil {
			return nil, fmt.Errorf("read header: %w", err)
		}

		var fileMagic [4]byte
		copy(fileMagic[:], header[0:4])
		if fileMagic != magic {
			return nil, fmt.Errorf("segment %s: invalid magic %x: encryption-at-rest format requires a fresh data dir", path, fileMagic)
		}

		flags := segmentFlags(binary.BigEndian.Uint32(header[segFlagsOffset : segFlagsOffset+segFlagsSize]))
		if flags&flagFull != 0 {
			seg.full.Store(true)
		}
	}

	return seg, nil
}

// readFlags returns the segment-header flags word.
func (seg *segment) readFlags() (segmentFlags, error) {
	buf := make([]byte, segFlagsSize)
	if _, err := seg.ReadAt(buf, segFlagsOffset); err != nil {
		return 0, err
	}
	return segmentFlags(binary.BigEndian.Uint32(buf)), nil
}

// writeFlags overwrites the segment-header flags word.
func (seg *segment) writeFlags(flags segmentFlags) error {
	var buf [segFlagsSize]byte
	binary.BigEndian.PutUint32(buf[:], uint32(flags))
	_, err := seg.WriteAt(buf[:], segFlagsOffset)
	return err
}

// markFull sets flagFull in the segment header and the in-memory cache.
// Once set, the store skips this segment when looking for a write target.
func (seg *segment) markFull() error {
	flags, err := seg.readFlags()
	if err != nil {
		return err
	}
	if err := seg.writeFlags(flags | flagFull); err != nil {
		return err
	}
	seg.full.Store(true)
	return nil
}
