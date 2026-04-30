package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

const writeBufLen = 32

// shardWriter writes a single shard as a contiguous sequence of fragments.
// The internal buffer is sized writeBufLen*totalFragSize and mirrors the on-disk
// layout exactly (headers interleaved with body data). This allows flush to issue
// a single WriteAt for the entire buffer without re-packing.
//
// Position tracking:
//   - bufPos: bytes written into the buffer (0 → ext.PSize), includes headers
//   - extPos: bytes already flushed to disk; buf[0:bufPos-extPos] is unflushed
//
// Fragment headers are written inline by writeHeader() whenever bufPos lands on
// a fragment boundary. Payload length, flags, and CRC are deferred to flush().
type shardWriter struct {
	seg *segment
	ext extent

	shardNum uint64
	fragNum  uint64

	buf    []byte
	bufPos int64
	extPos int64

	onClose func() error
	closed  bool
}

var ErrClosedWriter = errors.New("closed writer")

// dataWritten derives logical bytes written from bufPos. Each complete fragment
// contributes fragBodySize logical bytes; a partial fragment contributes
// (bufPos % totalFragSize - fragHeaderSize) bytes once past the header.
func (w *shardWriter) dataWritten() int64 {
	return w.bufPos/totalFragSize*fragBodySize + max(0, w.bufPos%totalFragSize-fragHeaderSize)
}

// Write copies p into the shard via ReadFrom over a bytes.Reader, sharing the
// header/flush loop. Returns "shard full" if p doesn't fit in the remaining
// logical capacity; the trailing io.EOF from a fully-consumed reader is
// swallowed.
func (w *shardWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, ErrClosedWriter
	}

	r := bytes.NewReader(p)
	n, err := w.ReadFrom(r)
	if errors.Is(err, io.EOF) {
		err = nil
	}

	if err == nil && r.Len() > 0 {
		err = fmt.Errorf("shard full")
	}

	return int(n), err
}

// ReadFrom streams from r into the shard until dataSize is reached or r returns
// an error. Unlike Write, it returns io.EOF from r without treating it as a
// fault — partial reads are flushed before the error propagates.
func (w *shardWriter) ReadFrom(r io.Reader) (total int64, err error) {
	if w.closed {
		return 0, ErrClosedWriter
	}

	for w.dataWritten() < w.ext.LSize {
		if w.bufPos%totalFragSize == 0 {
			w.writeHeader()
		}

		bufPos := int(w.bufPos - w.extPos)
		bodyLeft := int(totalFragSize - w.bufPos%totalFragSize)
		dataLeft := int(w.ext.LSize - w.dataWritten())

		n, readErr := r.Read(w.buf[bufPos : bufPos+min(bodyLeft, dataLeft)])
		w.bufPos += int64(n)
		total += int64(n)

		if int(w.bufPos-w.extPos) >= len(w.buf) || w.dataWritten() >= w.ext.LSize {
			if err := w.flush(w.dataWritten() >= w.ext.LSize); err != nil {
				return total, err
			}
		}

		if readErr != nil {
			return total, readErr
		}
	}

	return total, nil
}

// Close flushes any remaining buffered data, syncs the segment file, then
// commits the extent to the index via onClose. Must be called exactly once.
// The segment ref is always decremented on exit; the index commit only runs
// when the data is fully durable, preserving rollback on flush/Sync failure.
func (w *shardWriter) Close() (err error) {
	if w.closed {
		return ErrClosedWriter
	}

	w.closed = true
	defer w.seg.refs.Add(-1)

	if w.bufPos > w.extPos {
		if err = w.flush(true); err != nil {
			return err
		}
	}

	if err = w.seg.Sync(); err != nil {
		return fmt.Errorf("sync segment %d: %w", w.ext.SegNum, err)
	}

	return w.onClose()
}

// writeHeader writes fragNum and shardNum into the buffer at the current
// position, zeroes the remaining header fields (payloadLen, flags, crc are
// filled in by flush), and advances bufPos past the header.
func (w *shardWriter) writeHeader() {
	bufPos := int(w.bufPos - w.extPos)
	binary.BigEndian.PutUint64(w.buf[bufPos:], w.fragNum)
	binary.BigEndian.PutUint64(w.buf[bufPos+8:], w.shardNum)
	clear(w.buf[bufPos+16 : bufPos+fragHeaderSize])
	w.bufPos += fragHeaderSize
	w.fragNum++
}

// flush writes the buffered fragments to disk in a single WriteAt. Before the
// write it fills in each fragment's payloadLen, flags, and CRC. If final is
// true the last fragment gets flagEndOfShard, its body is zero-padded, and
// payloadLen reflects the actual data (which may be < fragBodySize).
func (w *shardWriter) flush(final bool) error {
	bufUsed := int(w.bufPos - w.extPos)
	if bufUsed <= 0 {
		return nil
	}

	fragCount := (bufUsed + totalFragSize - 1) / totalFragSize
	writeLen := fragCount * totalFragSize

	for i := range fragCount {
		pos := i * totalFragSize

		// Determine actual payload length; pad the final fragment's tail with zeros.
		bodySize := fragBodySize
		if final && i == fragCount-1 {
			bodySize = bufUsed - pos - fragHeaderSize
			clear(w.buf[pos+fragHeaderSize+bodySize : pos+totalFragSize])
		}
		binary.BigEndian.PutUint32(w.buf[pos+20:pos+24], uint32(bodySize)) //nolint:gosec // bodySize bounded by fragBodySize (8 KiB).

		var flags fragFlags
		if final && i == fragCount-1 {
			flags = flagEndOfShard
		}
		binary.BigEndian.PutUint32(w.buf[pos+24:pos+28], uint32(flags))

		// CRC covers the full fragment with the CRC field itself zeroed.
		binary.BigEndian.PutUint32(w.buf[pos+28:pos+32], 0)
		binary.BigEndian.PutUint32(w.buf[pos+28:pos+32], crc32.ChecksumIEEE(w.buf[pos:pos+totalFragSize]))
	}

	if _, err := w.seg.WriteAt(w.buf[:writeLen], w.ext.Off+w.extPos); err != nil {
		return fmt.Errorf("write to segment %d at offset %d: %w", w.ext.SegNum, w.ext.Off+w.extPos, err)
	}

	w.extPos = w.bufPos
	return nil
}
