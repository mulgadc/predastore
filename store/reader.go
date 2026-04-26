package store

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

const readBufLen = 1

// shardReader provides random and sequential access to a single shard's data.
// It reads one fragment at a time from disk, validates CRC, and copies the
// payload into the caller's buffer. bufPos tracks the logical read position
// for sequential Read() calls; ReadAt is stateless.
type shardReader struct {
	seg *segment
	ext extent

	buf    []byte
	bufPos int64

	onClose func() error
	closed  bool
}

// Read implements io.Reader by delegating to ReadAt at the current position.
func (r *shardReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, fmt.Errorf("shard reader closed")
	}

	if r.bufPos >= r.ext.LSize {
		return 0, io.EOF
	}

	n, err := r.ReadAt(p, r.bufPos)
	r.bufPos += int64(n)
	return n, err
}

// ReadAt reads len(p) bytes from the shard starting at logical byte offset.
// Translates logical offsets to on-disk fragment positions:
//
//	fragIndex = logicalPos / fragBodySize
//	diskOff   = ext.Off + fragIndex * totalFragSize
//
// Each fragment is CRC-validated before payload extraction.
func (r *shardReader) ReadAt(p []byte, off int64) (int, error) {
	if off >= r.ext.LSize {
		return 0, io.EOF
	}

	if off+int64(len(p)) > r.ext.LSize {
		p = p[:r.ext.LSize-off]
	}

	totalCopied := 0
	for totalCopied < len(p) {
		logicalPos := off + int64(totalCopied)
		fragIndex := logicalPos / fragBodySize
		bodyOffset := int(logicalPos % fragBodySize)

		diskOff := r.ext.Off + fragIndex*totalFragSize
		if _, err := r.seg.file.ReadAt(r.buf[:totalFragSize], diskOff); err != nil {
			return totalCopied, fmt.Errorf("read segment %d at offset %d: %w", r.ext.SegNum, diskOff, err)
		}

		// Validate CRC
		storedCRC := binary.BigEndian.Uint32(r.buf[28:32])
		binary.BigEndian.PutUint32(r.buf[28:32], 0)
		if computed := crc32.ChecksumIEEE(r.buf[:totalFragSize]); computed != storedCRC {
			return totalCopied, fmt.Errorf("crc mismatch in segment %d at offset %d: stored=%08x computed=%08x", r.ext.SegNum, diskOff, storedCRC, computed)
		}

		payloadLen := int(binary.BigEndian.Uint32(r.buf[20:24]))
		if payloadLen > fragBodySize {
			return totalCopied, fmt.Errorf("invalid payload length %d in segment %d at offset %d", payloadLen, r.ext.SegNum, diskOff)
		}

		n := copy(p[totalCopied:], r.buf[fragHeaderSize+bodyOffset:fragHeaderSize+payloadLen])
		totalCopied += n
	}

	if off+int64(totalCopied) >= r.ext.LSize {
		return totalCopied, io.EOF
	}

	return totalCopied, nil
}

// WriteTo streams the full shard to w via io.SectionReader over ReadAt.
func (r *shardReader) WriteTo(w io.Writer) (int64, error) {
	return io.Copy(w, io.NewSectionReader(r, 0, r.ext.LSize))
}

// Size returns the logical (data-only) size of the shard, excluding fragment headers.
func (r *shardReader) Size() int64 {
	return r.ext.LSize
}

// Close releases the segment reference. Must be called exactly once.
func (r *shardReader) Close() error {
	if r.closed {
		return fmt.Errorf("shard reader closed")
	}

	r.closed = true

	return r.onClose()
}
