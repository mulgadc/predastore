package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sync"
)

const writeBufferFrags = 1

var (
	ErrWriterClosed = errors.New("writer closed")
	ErrObjectFull   = errors.New("object full")
)

// slotBufferPool reuses slot-sized buffers (slotHeaderSize + slotPayloadSize = 8224 bytes).
var slotBufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, slotSize)
		return &b
	},
}

// zeroPadBuffer is a pre-allocated zero slice for clearing the padding region of a slot payload.
var zeroPadBuffer = make([]byte, int(slotBodySize))

type objectWriter struct {
	seg     *segment
	extent  extent
	objNum  uint64
	slotNum uint64
	buf     []byte
	off     int

	onClose func() error
	closed  bool
}

func (writer *objectWriter) Write(p []byte) (total int, err error) {
	if writer.closed {
		return 0, ErrWriterClosed
	}

	for len(p) > 0 {
		if int64(writer.off) >= writer.extent.size {
			return total, ErrObjectFull
		}

		n := copy(writer.buf[writer.off%len(writer.buf):], p)
		writer.off += n
		total += n
		p = p[n:]

		if n > 0 {
			switch {
			case int64(writer.off) >= writer.extent.size:
				if err := writer.flush(true); err != nil {
					return total, err
				}

				return total, ErrObjectFull

			case writer.off%len(writer.buf) == 0:
				if err := writer.flush(false); err != nil {
					return total, err
				}
			}
		}
	}

	return total, nil
}

func (writer *objectWriter) ReadFrom(r io.Reader) (total int64, err error) {
	if writer.closed {
		return 0, ErrWriterClosed
	}

	for {
		n, err := r.Read(writer.buf[writer.off%len(writer.buf):])
		writer.off += n
		total += int64(n)

		if n > 0 {
			switch {
			case int64(writer.off) >= writer.extent.size:
				if err := writer.flush(true); err != nil {
					return total, err
				}

				return total, ErrObjectFull

			case writer.off%len(writer.buf) == 0:
				if err := writer.flush(false); err != nil {
					return total, err
				}
			}
		}

		if err != nil {
			return total, err
		}
	}
}

func (writer *objectWriter) Close() error {
	if writer.closed {
		return ErrWriterClosed
	}

	writer.closed = true

	if err := writer.seg.file.Sync(); err != nil {
		return fmt.Errorf("sync segment %d: %w", writer.extent.segNum, err)
	}

	return writer.onClose()
}

func (writer *objectWriter) flush(final bool) error {
	pos := 0
	for pos < len(writer.buf) {
		bufPtr := slotBufferPool.Get().(*[]byte)
		slot := *bufPtr

		fragSize := min(slotBodySize, writer.off-pos)

		// Copy fragment into slot
		copy(slot[slotHeaderSize:slotHeaderSize+fragSize], writer.buf[pos:pos+fragSize])

		// Pad fragment if short
		if fragSize < slotBodySize {
			copy(slot[slotHeaderSize+fragSize:], zeroPadBuffer[:slotBodySize-fragSize])
		}

		// Write slot header
		binary.BigEndian.PutUint64(slot[0:8], writer.slotNum)
		binary.BigEndian.PutUint64(slot[8:16], writer.objNum)
		binary.BigEndian.PutUint32(slot[16:20], 0)
		binary.BigEndian.PutUint32(slot[20:24], uint32(fragSize))

		var flags slotFlags
		if writer.slotNum == (uint64(writer.extent.size)+slotBodySize-1)/slotBodySize-1 {
			flags = flagSlotFinal
		}
		binary.BigEndian.PutUint32(slot[24:28], uint32(flags))
		binary.BigEndian.PutUint32(slot[28:32], 0)

		// Calculate CRC
		binary.BigEndian.PutUint32(slot[28:32], crc32.ChecksumIEEE(slot[:]))

		off := writer.extent.off + int64(writer.slotNum*slotSize)
		if _, err := writer.seg.file.WriteAt(slot[:slotSize], off); err != nil {
			slotBufferPool.Put(bufPtr)
			return fmt.Errorf("objectWriter: WriteAt offset %d: %w", off, err)
		}

		slotBufferPool.Put(bufPtr)
		pos += fragSize
		writer.slotNum += 1
	}

	return nil
}
