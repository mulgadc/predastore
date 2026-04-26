package store

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

const writeBufLen = 1

type shardWriter struct {
	seg      *segment
	extent   extent
	shardNum uint64
	fragNum  uint64
	dataSize int64
	buf      []byte
	off      int64
	flushed  int64

	onClose func() error
	closed  bool
}

// dataWritten returns the number of logical (non-header) bytes written so far.
func (w *shardWriter) dataWritten() int64 {
	return w.off/totalFragSize*fragBodySize + max(0, w.off%totalFragSize-fragHeaderSize)
}

func (w *shardWriter) Write(p []byte) (total int, err error) {
	if w.closed {
		return 0, fmt.Errorf("shard writer closed")
	}

	for len(p) > 0 {
		if w.dataWritten() >= w.dataSize {
			return total, fmt.Errorf("shard full")
		}

		if w.off%totalFragSize == 0 {
			w.writeHeader()
		}

		bufPos := int(w.off - w.flushed)
		bodyLeft := int(totalFragSize - w.off%totalFragSize)
		dataLeft := int(w.dataSize - w.dataWritten())
		n := copy(w.buf[bufPos:bufPos+min(bodyLeft, dataLeft)], p)
		w.off += int64(n)
		total += n
		p = p[n:]

		if int(w.off-w.flushed) >= len(w.buf) || w.dataWritten() >= w.dataSize {
			if err := w.flush(w.dataWritten() >= w.dataSize); err != nil {
				return total, err
			}
		}
	}

	return total, nil
}

func (w *shardWriter) ReadFrom(r io.Reader) (total int64, err error) {
	if w.closed {
		return 0, fmt.Errorf("shard writer closed")
	}

	for w.dataWritten() < w.dataSize {
		if w.off%totalFragSize == 0 {
			w.writeHeader()
		}

		bufPos := int(w.off - w.flushed)
		bodyLeft := int(totalFragSize - w.off%totalFragSize)
		dataLeft := int(w.dataSize - w.dataWritten())

		n, readErr := r.Read(w.buf[bufPos : bufPos+min(bodyLeft, dataLeft)])
		w.off += int64(n)
		total += int64(n)

		if int(w.off-w.flushed) >= len(w.buf) || w.dataWritten() >= w.dataSize {
			if err := w.flush(w.dataWritten() >= w.dataSize); err != nil {
				return total, err
			}
		}

		if readErr != nil {
			return total, readErr
		}
	}

	return total, nil
}

func (w *shardWriter) Close() error {
	if w.closed {
		return fmt.Errorf("shard writer closed")
	}

	w.closed = true

	if w.off > w.flushed {
		if err := w.flush(true); err != nil {
			return err
		}
	}

	if err := w.seg.file.Sync(); err != nil {
		return fmt.Errorf("sync segment %d: %w", w.extent.SegNum, err)
	}

	return w.onClose()
}

func (w *shardWriter) writeHeader() {
	bufPos := int(w.off - w.flushed)
	binary.BigEndian.PutUint64(w.buf[bufPos:], w.fragNum)
	binary.BigEndian.PutUint64(w.buf[bufPos+8:], w.shardNum)
	clear(w.buf[bufPos+16 : bufPos+fragHeaderSize])
	w.off += fragHeaderSize
	w.fragNum++
}

func (w *shardWriter) flush(final bool) error {
	bufUsed := int(w.off - w.flushed)
	if bufUsed <= 0 {
		return nil
	}

	nFrags := (bufUsed + totalFragSize - 1) / totalFragSize
	writeLen := nFrags * totalFragSize

	for i := range nFrags {
		pos := i * totalFragSize

		bodySize := fragBodySize
		if final && i == nFrags-1 {
			bodySize = bufUsed - pos - fragHeaderSize
			clear(w.buf[pos+fragHeaderSize+bodySize : pos+totalFragSize])
		}
		binary.BigEndian.PutUint32(w.buf[pos+20:pos+24], uint32(bodySize))

		var flags fragFlags
		if final && i == nFrags-1 {
			flags = flagEndOfShard
		}
		binary.BigEndian.PutUint32(w.buf[pos+24:pos+28], uint32(flags))

		binary.BigEndian.PutUint32(w.buf[pos+28:pos+32], 0)
		binary.BigEndian.PutUint32(w.buf[pos+28:pos+32], crc32.ChecksumIEEE(w.buf[pos:pos+totalFragSize]))
	}

	if _, err := w.seg.file.WriteAt(w.buf[:writeLen], w.extent.Off+w.flushed); err != nil {
		return fmt.Errorf("WriteAt offset %d: %w", w.extent.Off+w.flushed, err)
	}

	w.flushed = w.off
	return nil
}
