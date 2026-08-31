package engine

import (
	"encoding/binary"
	"fmt"
)

// extent locates a value's data on disk. Encoded as 32 bytes in the index:
// SegNum, Off, PSize, LSize, each 8-byte big-endian.
//   - PSize: physical (on-disk) size including fragment headers and GCM tags = fragCount * totalFragSize
//   - LSize: logical (data-only) size as seen by callers of Read/Write
type extent struct {
	SegNum uint64
	Off    int64
	PSize  int64
	LSize  int64
}

// An index value is an extent followed by the write epoch that produced it. A
// prepared value carries the time it was prepared as well, so a row abandoned
// by a caller that died between prepare and commit can be aged out rather than
// pinning its segment forever.
//
// The extent occupies the same leading bytes in both, which is what lets
// compaction relocate either kind by rewriting the head and keeping the tail.
const (
	extentEncodedSize = 32
	indexValueSize    = extentEncodedSize + 8
	preparedValueSize = indexValueSize + 8
)

func (ext extent) encode() []byte {
	buf := make([]byte, extentEncodedSize)
	binary.BigEndian.PutUint64(buf[0:8], ext.SegNum)
	binary.BigEndian.PutUint64(buf[8:16], uint64(ext.Off))    //nolint:gosec // round-trips bit-for-bit via int64 cast on decode.
	binary.BigEndian.PutUint64(buf[16:24], uint64(ext.PSize)) //nolint:gosec // round-trips bit-for-bit via int64 cast on decode.
	binary.BigEndian.PutUint64(buf[24:32], uint64(ext.LSize)) //nolint:gosec // round-trips bit-for-bit via int64 cast on decode.
	return buf
}

func decodeExtent(buf []byte) (ext extent, err error) {
	if len(buf) != extentEncodedSize {
		return ext, fmt.Errorf("invalid length %d, want %d", len(buf), extentEncodedSize)
	}

	ext.SegNum = binary.BigEndian.Uint64(buf[0:8])
	ext.Off = int64(binary.BigEndian.Uint64(buf[8:16]))    //nolint:gosec // round-trips bit-for-bit from encode.
	ext.PSize = int64(binary.BigEndian.Uint64(buf[16:24])) //nolint:gosec // round-trips bit-for-bit from encode.
	ext.LSize = int64(binary.BigEndian.Uint64(buf[24:32])) //nolint:gosec // round-trips bit-for-bit from encode.
	return ext, nil
}

// encodeIndexValue renders the live row for a committed extent.
func encodeIndexValue(ext extent, epoch uint64) []byte {
	buf := make([]byte, indexValueSize)
	copy(buf, ext.encode())
	binary.BigEndian.PutUint64(buf[extentEncodedSize:], epoch)
	return buf
}

// encodePreparedValue renders a prepared row. at is a wall-clock nanosecond
// stamp used only to age the row out; nothing reads it for ordering.
func encodePreparedValue(ext extent, epoch uint64, at int64) []byte {
	buf := make([]byte, preparedValueSize)
	copy(buf, encodeIndexValue(ext, epoch))
	binary.BigEndian.PutUint64(buf[indexValueSize:], uint64(at)) //nolint:gosec // round-trips bit-for-bit via int64 cast on decode.
	return buf
}

// decodeIndexValue parses a live or a prepared row. Both are accepted here
// because compaction and the reaper walk rows without knowing which kind they
// hold; a caller that cares checks the width itself.
func decodeIndexValue(buf []byte) (ext extent, epoch uint64, err error) {
	if len(buf) != indexValueSize && len(buf) != preparedValueSize {
		return ext, 0, fmt.Errorf("invalid index value length %d, want %d or %d",
			len(buf), indexValueSize, preparedValueSize)
	}
	ext, err = decodeExtent(buf[:extentEncodedSize])
	if err != nil {
		return ext, 0, err
	}
	return ext, binary.BigEndian.Uint64(buf[extentEncodedSize:indexValueSize]), nil
}

// decodePreparedAt reads the stamp from a prepared row.
func decodePreparedAt(buf []byte) (int64, error) {
	if len(buf) != preparedValueSize {
		return 0, fmt.Errorf("invalid prepared value length %d, want %d", len(buf), preparedValueSize)
	}
	return int64(binary.BigEndian.Uint64(buf[indexValueSize:])), nil //nolint:gosec // round-trips bit-for-bit from encode.
}

// repointValue rewrites a row's extent while keeping everything after it, so a
// relocation preserves the epoch and, on a prepared row, its stamp.
func repointValue(old []byte, newExt extent) []byte {
	out := make([]byte, len(old))
	copy(out, newExt.encode())
	copy(out[extentEncodedSize:], old[extentEncodedSize:])
	return out
}
