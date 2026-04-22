package store

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

const writeBufferFragments = 1

var _ ObjectWriter = (*objectWriter)(nil)

// Append reserves slots for an object of the given size and returns an
// ObjectWriter. The caller writes data via the ObjectWriter and calls
// Close to fsync and commit. The lock is held only during reservation.
func (store *Store) Append(key string, size int64) (ObjectWriter, error) {
	store.mu.Lock()

	fragCount := (int(size) + slotPayloadSize - 1) / slotPayloadSize
	if fragCount == 0 {
		fragCount = 1
	}

	seqNum := store.seqNum.Load()
	store.seqNum.Add(uint64(fragCount))
	shardNum := store.shardNum.Add(1)
	slotExts, err := store.reserve(fragCount)

	store.mu.Unlock()

	if err != nil {
		return nil, fmt.Errorf("store: Append: reserve: %w", err)
	}

	return &objectWriter{
		store:     store,
		key:       key,
		size:      size,
		exts:      slotExts,
		seqNum:    seqNum,
		shardNum:  shardNum,
		fragCount: fragCount,
		buf:       make([]byte, writeBufferFragments*slotPayloadSize),
	}, nil
}

type objectWriter struct {
	store     *Store
	key       string
	size      int64
	exts      []*slotExtent
	seqNum    uint64
	shardNum  uint64
	fragCount int

	buf       []byte
	bufUsed   int
	fragIndex int
	written   int64
	closed    bool
}

// Write buffers data and flushes full fragments to disk.
func (ow *objectWriter) Write(p []byte) (int, error) {
	if ow.closed {
		return 0, fmt.Errorf("store: objectWriter: write after close")
	}

	total := 0
	for len(p) > 0 {
		space := writeBufferFragments*slotPayloadSize - ow.bufUsed
		n := copy(ow.buf[ow.bufUsed:ow.bufUsed+space], p)
		ow.bufUsed += n
		total += n
		p = p[n:]

		if ow.bufUsed == writeBufferFragments*slotPayloadSize {
			if err := ow.flushBuffer(false); err != nil {
				return total, err
			}
		}
	}
	return total, nil
}

// ReadFrom drains r through the same buffering path as Write.
func (ow *objectWriter) ReadFrom(r io.Reader) (int64, error) {
	if ow.closed {
		return 0, fmt.Errorf("store: objectWriter: ReadFrom after close")
	}

	var total int64
	for {
		space := writeBufferFragments*slotPayloadSize - ow.bufUsed
		n, err := r.Read(ow.buf[ow.bufUsed : ow.bufUsed+space])
		ow.bufUsed += n
		total += int64(n)

		if ow.bufUsed == writeBufferFragments*slotPayloadSize {
			if flushErr := ow.flushBuffer(false); flushErr != nil {
				return total, flushErr
			}
		}

		if err == io.EOF {
			return total, nil
		}
		if err != nil {
			return total, err
		}
	}
}

// Close flushes any partial fragment, fsyncs all touched segments, commits
// the object to the index, and releases extent references.
func (ow *objectWriter) Close() error {
	if ow.closed {
		return nil
	}
	ow.closed = true

	defer func() {
		for _, ext := range ow.exts {
			ext.Close()
		}
	}()

	if ow.bufUsed > 0 {
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
		payloadBytes := min(remaining, int64(ext.size)*int64(slotPayloadSize))
		byteExts[i] = byteExtent{
			segmentNum: ext.seg.num,
			offset:     uint64(ext.seg.byteOffset(ext.offset)), //nolint:gosec // G115: byteOffset non-negative
			size:       uint64(payloadBytes),                   //nolint:gosec // G115: payloadBytes non-negative
		}
		remaining -= payloadBytes
	}

	obj := &Object{
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

// flushBuffer writes buffered data as complete slot fragments to disk.
// If final is true, handles the last (possibly short) fragment.
func (ow *objectWriter) flushBuffer(final bool) error {
	offset := 0
	for offset < ow.bufUsed {
		payloadLen := min(slotPayloadSize, ow.bufUsed-offset)
		if !final && payloadLen < slotPayloadSize {
			break
		}

		ext, slotInExt := ow.locateSlot(ow.fragIndex)
		if ext == nil {
			return fmt.Errorf("store: objectWriter: fragment %d exceeds reserved slots", ow.fragIndex)
		}

		bufPtr := slotBufferPool.Get().(*[]byte) //nolint:forcetypeassert,errcheck // Pool.New always returns *[]byte
		slot := *bufPtr

		copy(slot[slotHeaderSize:slotHeaderSize+payloadLen], ow.buf[offset:offset+payloadLen])
		if payloadLen < slotPayloadSize {
			copy(slot[slotHeaderSize+payloadLen:slotHeaderSize+slotPayloadSize], zeroPadBuffer[:slotPayloadSize-payloadLen])
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
		checksum = crc32.Update(checksum, crc32.IEEETable, slot[slotHeaderSize:slotHeaderSize+slotPayloadSize])
		binary.BigEndian.PutUint32(slot[28:32], checksum)

		diskOffset := ext.seg.byteOffset(ext.offset + slotInExt)
		if _, err := ext.seg.file.WriteAt(slot[:totalSlotSize], diskOffset); err != nil {
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

// locateSlot maps a fragment index to its extent and slot-within-extent.
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
