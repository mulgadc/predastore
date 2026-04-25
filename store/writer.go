package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

const writeBufFrags = 1

var ErrWriterFinished = errors.New("object finished writing")

type objectWriter struct {
	key    ObjectKey
	seg    *segment
	ext    extent
	extPos int64
	buf    []byte
	bufPos int

	onClose func()
	closed  bool
}

func (ow *objectWriter) Write(dat []byte) (tot int, err error) {
	if ow.closed {
		return 0, nil
	}

	for len(dat) > 0 {
		n := copy(ow.buf[ow.bufPos:], dat)
		ow.extPos += int64(n)
		ow.bufPos += n
		tot += n
		dat = dat[n:]

		if n > 0 {
			if ow.extPos >= ow.ext.size {
				if err = ow.flush(true); err != nil {
					return tot, err
				}

				if len(dat) > 0 {
					return tot, ErrWriterFinished
				}

				return tot, nil
			}

			if ow.bufPos == writeBufFrags*fragSize {
				if err := ow.flush(false); err != nil {
					return tot, err
				}
			}
		}

		if err != nil {
			return tot, err
		}
	}

	return tot, nil
}

func (ow *objectWriter) ReadFrom(r io.Reader) (tot int64, err error) {
	if ow.closed {
		return 0, nil
	}

	for {
		n, err := r.Read(ow.buf[ow.bufPos:])
		ow.extPos += int64(n)
		ow.bufPos += n
		tot += int64(n)

		if n > 0 {
			if ow.extPos >= ow.ext.size {
				if err := ow.flush(true); err != nil {
					return tot, err
				}

				return tot, io.EOF
			}

			if ow.bufPos == writeBufFrags*fragSize {
				if err := ow.flush(false); err != nil {
					return tot, err
				}
			}
		}

		if err != nil {
			return tot, err
		}
	}
}

func (ow *objectWriter) Close() error {
	if ow.closed {
		return fmt.Errorf("objectWriter closed")
	}

	ow.closed = true

	if ow.bufPos > 0 {
		if err := ow.flushBuffer(true); err != nil {
			return err
		}
	}

	for _, ext := range ow.exts {
		if err := ext.seg.file.Sync(); err != nil {
			return fmt.Errorf("store: objectWriter: fsync segment %d: %w", ext.seg.num, err)
		}
	}

	remaining := ow.size
	byteExts := make([]byteExtent, len(ow.exts))
	for i, ext := range ow.exts {
		payloadBytes := min(remaining, int64(ext.size)*int64(fragSize))
		byteExts[i] = byteExtent{
			segmentNum: ext.seg.num,
			offset:     uint64(ext.seg.byteOffset(ext.offset)), //nolint:gosec // G115: byteOffset non-negative
			size:       uint64(payloadBytes),                   //nolint:gosec // G115: payloadBytes non-negative
		}
		remaining -= payloadBytes
	}

	obj := &objectReader{
		totalSize:   ow.size,
		byteExtents: byteExts,
		store:       ow.store,
	}

	encoded, err := encodeObject(obj)
	if err != nil {
		return fmt.Errorf("store: objectWriter: encode: %w", err)
	}
	if err := ow.store.index.Set([]byte(ow.key), encoded); err != nil {
		return fmt.Errorf("store: objectWriter: commit to index: %w", err)
	}

	return nil
}

func (ow *objectWriter) flush() error {
	offset := 0
	for offset < ow.bufUsed {
		payloadLen := min(fragSize, ow.bufUsed-offset)
		if !final && payloadLen < fragSize {
			break
		}

		ext, slotInExt := ow.locateSlot(ow.fragIndex)
		if ext == nil {
			return fmt.Errorf("store: objectWriter: fragment %d exceeds reserved slots", ow.fragIndex)
		}

		bufPtr := slotBufferPool.Get().(*[]byte) //nolint:forcetypeassert,errcheck // Pool.New always returns *[]byte
		slot := *bufPtr

		copy(slot[slotHeaderSize:slotHeaderSize+payloadLen], ow.buf[offset:offset+payloadLen])
		if payloadLen < fragSize {
			copy(slot[slotHeaderSize+payloadLen:slotHeaderSize+fragSize], zeroPadBuffer[:fragSize-payloadLen])
		}

		binary.BigEndian.PutUint64(slot[0:8], ow.seqNum+uint64(ow.fragIndex)) //nolint:gosec // G115: fragIndex non-negative, bounded by fragCount
		binary.BigEndian.PutUint64(slot[8:16], ow.shardNum)
		binary.BigEndian.PutUint32(slot[16:20], uint32(ow.fragIndex)) //nolint:gosec // G115: fragIndex non-negative, bounded by fragCount
		binary.BigEndian.PutUint32(slot[20:24], uint32(payloadLen))   //nolint:gosec // G115: payloadLen bounded by slotPayloadSize (8 KiB)
		var flags slotFlags
		if ow.fragIndex == ow.fragCount-1 {
			flags = flagEndOfShard
		}
		binary.BigEndian.PutUint32(slot[24:28], uint32(flags))
		binary.BigEndian.PutUint32(slot[28:32], 0)

		checksum := crc32.ChecksumIEEE(slot[0:slotHeaderSize])
		checksum = crc32.Update(checksum, crc32.IEEETable, slot[slotHeaderSize:slotHeaderSize+fragSize])
		binary.BigEndian.PutUint32(slot[28:32], checksum)

		diskOffset := ext.seg.byteOffset(ext.offset + slotInExt)
		if _, err := ext.seg.file.WriteAt(slot[:slotSize], diskOffset); err != nil {
			slotBufferPool.Put(bufPtr)
			return fmt.Errorf("store: objectWriter: WriteAt slot %d: %w", ow.fragIndex, err)
		}

		slotBufferPool.Put(bufPtr)
		offset += payloadLen
		ow.written += int64(payloadLen)
		ow.fragIndex++
	}

	if offset > 0 {
		remaining := copy(ow.buf, ow.buf[offset:ow.bufUsed])
		ow.bufUsed = remaining
	}

	return nil
}

func (ow *objectWriter) locateSlot(fragIndex int) (*slotExtent, int) {
	idx := fragIndex
	for _, ext := range ow.exts {
		if idx < ext.size {
			return ext, idx
		}
		idx -= ext.size
	}
	return nil, 0
}
