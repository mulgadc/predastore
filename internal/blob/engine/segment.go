package engine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

const (
	segFilename    = "%016d.seg"
	idxSegFilename = "%016d.idx"
)

// errSegmentBusy reports a drop declined because the segment still has live
// readers or writers. Not a failure: the caller retries on its next cycle.
var errSegmentBusy = errors.New("segment still referenced")

const (
	// segRefPollInterval is how often the close path rechecks a segment's
	// reference count.
	segRefPollInterval = 20 * time.Millisecond

	// closeRefDrainBudget bounds how long Close waits for one segment's readers
	// and writers to finish before closing the fd out from under them.
	closeRefDrainBudget = 10 * time.Second
)

// idxEntry is one row of a segment's append-only reverse sidecar (.idx), written
// per allocation so compaction can enumerate a segment's extents without
// scanning the whole index.
const idxEntrySize = 52 // Off(8) ‖ Key(36) ‖ PSize(8)

type idxEntry struct {
	Off   int64
	Key   [36]byte
	PSize int64
}

func (e idxEntry) encode() []byte {
	buf := make([]byte, idxEntrySize)
	binary.BigEndian.PutUint64(buf[0:8], uint64(e.Off)) //nolint:gosec // round-trips bit-for-bit via int64 cast on decode.
	copy(buf[8:44], e.Key[:])
	binary.BigEndian.PutUint64(buf[44:52], uint64(e.PSize)) //nolint:gosec // round-trips bit-for-bit via int64 cast on decode.
	return buf
}

func decodeIdxEntry(b []byte) (e idxEntry, err error) {
	if len(b) != idxEntrySize {
		return e, fmt.Errorf("idxEntry: invalid length %d, want %d", len(b), idxEntrySize)
	}
	e.Off = int64(binary.BigEndian.Uint64(b[0:8])) //nolint:gosec // round-trips bit-for-bit from encode.
	copy(e.Key[:], b[8:44])
	e.PSize = int64(binary.BigEndian.Uint64(b[44:52])) //nolint:gosec // round-trips bit-for-bit from encode.
	return e, nil
}

// scanIdx reads a segment's whole .idx into memory, listing every extent ever
// allocated there. The rows are hints: callers must back-check each against the
// index, which is the authority on what is live.
func scanIdx(dir string, segNum uint64) ([]idxEntry, error) {
	// Read-only: a segment scanned by compaction is expected to already exist,
	// so a missing .idx must be reported rather than fabricated.
	f, err := openFile(filepath.Join(dir, fmt.Sprintf(idxSegFilename, segNum)), false)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			slog.Warn("failed to close idx", "segNum", segNum, "error", closeErr)
		}
	}()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	// Whole records only. A torn trailing row is a crash mid-append, and since
	// .idx is fsynced before the index commit, that extent never went live.
	count := info.Size() / idxEntrySize
	entries := make([]idxEntry, 0, count)
	buf := make([]byte, idxEntrySize)
	for i := range count {
		if _, err := f.ReadAt(buf, i*idxEntrySize); err != nil {
			return nil, err
		}
		e, err := decodeIdxEntry(buf)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

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

// openFile is the package-level opener used by openSegment and scanIdx.
// create distinguishes the append path, which may fabricate a fresh segment,
// from read paths (lookup, compaction), which must report a missing segment
// as an error rather than silently creating a header-only stand-in.
// Production code uses os.OpenFile; tests override via export_test.go.
var openFile = func(path string, create bool) (file, error) {
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE
	}
	return os.OpenFile(path, flags, 0600)
}

// segment is an open segment file handle with a reference count.
type segment struct {
	file // .seg

	idx     file   // .idx sidecar
	idxSize int64  // .idx append cursor, guarded by store.mutex
	num     uint64 // for unlink paths on drop

	// Active readers and writers, drained before the fd closes. A counter
	// rather than a WaitGroup so it can be tested without blocking: addRef only
	// runs from Lookup/Append under store.mutex, so a zero read under that same
	// lock is exact — no new reference can appear while it is held.
	refs atomic.Int64

	// Caches the on-disk full flag, sparing the hot Append path a ReadAt.
	full atomic.Bool
}

func (seg *segment) addRef()     { seg.refs.Add(1) }
func (seg *segment) releaseRef() { seg.refs.Add(-1) }
func (seg *segment) busy() bool  { return seg.refs.Load() > 0 }

// waitForRefs drains active references, giving up after budget and reporting
// whether it drained. Only the close path waits: a reader wedged against a peer
// that stopped reading must not hold shutdown open indefinitely, and a segment
// closed under one fails that read rather than stalling the process.
func (seg *segment) waitForRefs(budget time.Duration) bool {
	deadline := time.Now().Add(budget)
	for seg.busy() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(segRefPollInterval)
	}
	return true
}

// appendIdx writes one row at the .idx append cursor and advances it. Caller
// holds store.mutex.
func (seg *segment) appendIdx(e idxEntry) error {
	if _, err := seg.idx.WriteAt(e.encode(), seg.idxSize); err != nil {
		return err
	}
	seg.idxSize += idxEntrySize
	return nil
}

func (seg *segment) syncIdx() error { return seg.idx.Sync() }

// dropSegment unlinks a drained segment and its sidecar. Caller holds
// store.mutex. Safe only once every live extent it held has been committed
// elsewhere.
//
// A segment with live references is left in place and reported busy. Waiting
// for them here would hold store.mutex across a wait no reader bounds, so a
// single wedged reader would stall every other read and write on the node.
// Deferring costs nothing but the dead space until the next cycle.
func (store *Store) dropSegment(num uint64) error {
	seg, ok := store.segCache[num]
	if !ok {
		return nil
	}
	if seg.busy() {
		return fmt.Errorf("%w: segment %d", errSegmentBusy, num)
	}
	delete(store.segCache, num)
	store.stats.liveSegments--

	var errs []error
	if err := seg.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close segment %d: %w", num, err))
	}
	if err := seg.idx.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close idx %d: %w", num, err))
	}
	if err := os.Remove(filepath.Join(store.dir, fmt.Sprintf(segFilename, num))); err != nil {
		errs = append(errs, fmt.Errorf("unlink segment %d: %w", num, err))
	}
	if err := os.Remove(filepath.Join(store.dir, fmt.Sprintf(idxSegFilename, num))); err != nil {
		errs = append(errs, fmt.Errorf("unlink idx %d: %w", num, err))
	}
	return errors.Join(errs...)
}

// countSegments counts the .seg files in dir. Called once at Open to seed the
// live-segment counter, never on a reporting path.
func countSegments(dir string) (int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	var n int64
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".seg" {
			continue
		}
		// An empty file is one a crash left between create and header write.
		// openSegment headers it and reports it as created, so counting it here
		// as well would count it twice.
		info, err := e.Info()
		if err != nil || info.Size() == 0 {
			continue
		}
		n++
	}
	return n, nil
}

// getSegment returns a cached segment or opens it from disk. create selects
// the append path (fabricate a fresh segment if none exists) versus a read
// path (report a missing segment as an error). A cache hit ignores create,
// since the segment already exists either way. Callers must hold
// store.mutex.
func (store *Store) getSegment(num uint64, create bool) (*segment, error) {
	if seg, ok := store.segCache[num]; ok {
		return seg, nil
	}

	seg, created, err := openSegment(store.dir, num, create)
	if err != nil {
		return nil, err
	}
	if created {
		store.stats.liveSegments++
	}

	store.segCache[num] = seg

	return seg, nil
}

// nextAvailableSegment returns the first non-full segment at or after
// store.segNum, advancing store.segNum past any full segments it walks
// over. Callers must hold store.mutex.
func (store *Store) nextAvailableSegment() (*segment, error) {
	for range maxSegmentScanAttempts {
		// Append path: fabricate the segment if it doesn't exist yet.
		seg, err := store.getSegment(store.segNum, true)
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
// incoming value. Callers must hold store.mutex.
func (store *Store) rollSegment() (*segment, error) {
	store.segNum++
	// Append path: fabricate the segment if it doesn't exist yet.
	return store.getSegment(store.segNum, true)
}

// openSegment opens or creates segment num. created reports whether the file
// was brought into existence by this call, which is what the live-segment
// counter is maintained from.
func openSegment(dir string, num uint64, create bool) (seg *segment, created bool, err error) {
	path := filepath.Join(dir, fmt.Sprintf(segFilename, num))

	f, err := openFile(path, create)
	if err != nil {
		return nil, false, err
	}

	idxFile, err := openFile(filepath.Join(dir, fmt.Sprintf(idxSegFilename, num)), create)
	if err != nil {
		if closeErr := f.Close(); closeErr != nil {
			slog.Warn("failed to close segment", "segNum", num, "error", closeErr)
		}
		return nil, false, err
	}

	defer func() {
		if err != nil {
			for name, c := range map[string]file{"segment": f, "idx": idxFile} {
				if closeErr := c.Close(); closeErr != nil {
					slog.Warn("failed to close "+name, "segNum", num, "error", closeErr)
				}
			}
		}
	}()

	idxInfo, err := idxFile.Stat()
	if err != nil {
		return nil, false, fmt.Errorf("stat idx %d: %w", num, err)
	}

	info, err := f.Stat()
	if err != nil {
		return nil, false, err
	}

	seg = &segment{file: f, idx: idxFile, idxSize: idxInfo.Size(), num: num}

	switch {
	// New file: write the segment header.
	case info.Size() == 0:
		created = true

		header := make([]byte, segHeaderSize)
		copy(header[0:4], magic[:])                  // [0:4]  magic
		binary.BigEndian.PutUint16(header[4:6], v1)  // [4:6]  version
		binary.BigEndian.PutUint32(header[6:10], 0)  // [6:10] flags
		binary.BigEndian.PutUint32(header[10:14], 0) // [10:14] reserved

		if _, err = f.WriteAt(header, 0); err != nil {
			return nil, false, fmt.Errorf("write header: %w", err)
		}

	// Existing file: validate magic and seed the full-flag cache.
	default:
		header := make([]byte, segHeaderSize)
		if _, err = f.ReadAt(header, 0); err != nil {
			return nil, false, fmt.Errorf("read header: %w", err)
		}

		var fileMagic [4]byte
		copy(fileMagic[:], header[0:4])
		if fileMagic != magic {
			return nil, false, fmt.Errorf("segment %s: invalid magic %x: encryption-at-rest format requires a fresh data dir", path, fileMagic)
		}

		flags := segmentFlags(binary.BigEndian.Uint32(header[segFlagsOffset : segFlagsOffset+segFlagsSize]))
		if flags&flagFull != 0 {
			seg.full.Store(true)
		}
	}

	return seg, created, nil
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
