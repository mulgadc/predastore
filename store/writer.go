package store

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

const writeBufLen = 1

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

// dataWritten returns the number of logical (non-header) bytes written so far.
func (w *shardWriter) dataWritten() int64 {
	return w.bufPos/totalFragSize*fragBodySize + max(0, w.bufPos%totalFragSize-fragHeaderSize)
}

func (w *shardWriter) Write(p []byte) (total int, err error) {
	if w.closed {
		return 0, fmt.Errorf("shard writer closed")
	}

	for len(p) > 0 {
		if w.dataWritten() >= w.ext.LSize {
			return total, fmt.Errorf("shard full")
		}

		if w.bufPos%totalFragSize == 0 {
			w.writeHeader()
		}

		bufPos := int(w.bufPos - w.extPos)
		bodyLeft := int(totalFragSize - w.bufPos%totalFragSize)
		dataLeft := int(w.ext.LSize - w.dataWritten())
		n := copy(w.buf[bufPos:bufPos+min(bodyLeft, dataLeft)], p)
		w.bufPos += int64(n)
		total += n
		p = p[n:]

		if int(w.bufPos-w.extPos) >= len(w.buf) || w.dataWritten() >= w.ext.LSize {
			if err := w.flush(w.dataWritten() >= w.ext.LSize); err != nil {
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

func (w *shardWriter) Close() error {
	if w.closed {
		return fmt.Errorf("shard writer closed")
	}

	w.closed = true

	if w.bufPos > w.extPos {
		if err := w.flush(true); err != nil {
			return err
		}
	}

	if err := w.seg.file.Sync(); err != nil {
		return fmt.Errorf("sync segment %d: %w", w.ext.SegNum, err)
	}

	return w.onClose()
}

func (w *shardWriter) writeHeader() {
	bufPos := int(w.bufPos - w.extPos)
	binary.BigEndian.PutUint64(w.buf[bufPos:], w.fragNum)
	binary.BigEndian.PutUint64(w.buf[bufPos+8:], w.shardNum)
	clear(w.buf[bufPos+16 : bufPos+fragHeaderSize])
	w.bufPos += fragHeaderSize
	w.fragNum++
}

func (w *shardWriter) flush(final bool) error {
	bufUsed := int(w.bufPos - w.extPos)
	if bufUsed <= 0 {
		return nil
	}

	fragCount := (bufUsed + totalFragSize - 1) / totalFragSize
	writeLen := fragCount * totalFragSize

	for i := range fragCount {
		pos := i * totalFragSize

		bodySize := fragBodySize
		if final && i == fragCount-1 {
			bodySize = bufUsed - pos - fragHeaderSize
			clear(w.buf[pos+fragHeaderSize+bodySize : pos+totalFragSize])
		}
		binary.BigEndian.PutUint32(w.buf[pos+20:pos+24], uint32(bodySize))

		var flags fragFlags
		if final && i == fragCount-1 {
			flags = flagEndOfShard
		}
		binary.BigEndian.PutUint32(w.buf[pos+24:pos+28], uint32(flags))

		binary.BigEndian.PutUint32(w.buf[pos+28:pos+32], 0)
		binary.BigEndian.PutUint32(w.buf[pos+28:pos+32], crc32.ChecksumIEEE(w.buf[pos:pos+totalFragSize]))
	}

	if _, err := w.seg.file.WriteAt(w.buf[:writeLen], w.ext.Off+w.extPos); err != nil {
		return fmt.Errorf("write to segment %d at offset %d: %w", w.ext.SegNum, w.ext.Off+w.extPos, err)
	}

	w.extPos = w.bufPos
	return nil
}
