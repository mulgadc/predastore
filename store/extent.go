package store

import (
	"encoding/binary"
	"fmt"
)

// extent locates a shard's data on disk. Encoded as 32 bytes in the index:
// SegNum, Off, PSize, LSize, each 8-byte big-endian.
//   - PSize: physical (on-disk) size including fragment headers and GCM tags = fragCount * totalFragSize
//   - LSize: logical (data-only) size as seen by callers of Read/Write
type extent struct {
	SegNum uint64
	Off    int64
	PSize  int64
	LSize  int64
}

const extentEncodedSize = 32

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
		return ext, fmt.Errorf("extent: invalid length %d, want %d", len(buf), extentEncodedSize)
	}

	ext.SegNum = binary.BigEndian.Uint64(buf[0:8])
	ext.Off = int64(binary.BigEndian.Uint64(buf[8:16]))    //nolint:gosec // round-trips bit-for-bit from encode.
	ext.PSize = int64(binary.BigEndian.Uint64(buf[16:24])) //nolint:gosec // round-trips bit-for-bit from encode.
	ext.LSize = int64(binary.BigEndian.Uint64(buf[24:32])) //nolint:gosec // round-trips bit-for-bit from encode.
	return ext, nil
}
