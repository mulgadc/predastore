package store

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const readBufferFragments = 1

// Lookup fetches the Object metadata for key from the store index.
// Returns (nil, false) if the key does not exist.
func (store *Store) Lookup(key string) (ObjectReader, bool) {
	data, err := store.index.Get([]byte(key))
	if err != nil {
		return nil, false
	}
	obj, err := decodeObject(data)
	if err != nil {
		return nil, false
	}
	obj.store = store
	return obj, true
}

// ReadAt reads len(p) bytes from the object starting at byte offset.
// Stateless and safe for concurrent use. Reads full fragments from disk
// even for partial requests.
func (obj *Object) ReadAt(p []byte, offset int64) (int, error) {
	if offset >= obj.totalSize {
		return 0, io.EOF
	}

	clamped := false
	if offset+int64(len(p)) > obj.totalSize {
		p = p[:obj.totalSize-offset]
		clamped = true
	}

	byteExtIndex, offsetInByteExt := locateByteOffset(obj.byteExtents, offset)

	slotIndex := int(offsetInByteExt / slotPayloadSize)
	offsetInSlot := int(offsetInByteExt % slotPayloadSize)

	fragBuf := make([]byte, readBufferFragments*totalSlotSize)
	totalCopied := 0

	for byteExtIndex < len(obj.byteExtents) && totalCopied < len(p) {
		ext := obj.byteExtents[byteExtIndex]
		slotsInExt := (int(ext.size) + slotPayloadSize - 1) / slotPayloadSize //nolint:gosec // G115: ext.size bounded by segment capacity (~1 GiB)

		f, err := os.Open(filepath.Join(obj.store.dir, fmt.Sprintf("%016d%s", ext.segmentNum, extension)))
		if err != nil {
			return totalCopied, fmt.Errorf("store: ReadAt: open segment %d: %w", ext.segmentNum, err)
		}

		for slotIndex < slotsInExt && totalCopied < len(p) {
			fragsToRead := min(readBufferFragments, slotsInExt-slotIndex)
			diskOffset := int64(ext.offset) + int64(slotIndex)*int64(totalSlotSize) //nolint:gosec // G115: ext.offset bounded by segment file size
			readSize := fragsToRead * totalSlotSize

			if _, err := f.ReadAt(fragBuf[:readSize], diskOffset); err != nil {
				f.Close()
				return totalCopied, fmt.Errorf("store: ReadAt: read segment %d offset %d: %w",
					ext.segmentNum, diskOffset, err)
			}

			for fi := range fragsToRead {
				if totalCopied >= len(p) {
					break
				}

				slot := fragBuf[fi*totalSlotSize : (fi+1)*totalSlotSize]

				storedCRC := binary.BigEndian.Uint32(slot[28:32])
				binary.BigEndian.PutUint32(slot[28:32], 0)
				computed := crc32.ChecksumIEEE(slot[0:slotHeaderSize])
				computed = crc32.Update(computed, crc32.IEEETable, slot[slotHeaderSize:slotHeaderSize+slotPayloadSize])

				if computed != storedCRC {
					f.Close()
					return totalCopied, fmt.Errorf("store: ReadAt: CRC mismatch segment %d offset %d: stored=%08x computed=%08x",
						ext.segmentNum, diskOffset+int64(fi)*int64(totalSlotSize), storedCRC, computed)
				}

				payloadLen := int(binary.BigEndian.Uint32(slot[20:24]))
				if payloadLen > slotPayloadSize {
					f.Close()
					return totalCopied, fmt.Errorf("store: ReadAt: invalid payload length %d in segment %d offset %d",
						payloadLen, ext.segmentNum, diskOffset+int64(fi)*int64(totalSlotSize))
				}

				payloadStart := slotHeaderSize + offsetInSlot
				payloadEnd := slotHeaderSize + payloadLen
				n := copy(p[totalCopied:], slot[payloadStart:payloadEnd])
				totalCopied += n
				offsetInSlot = 0
			}

			slotIndex += fragsToRead
		}

		f.Close()
		slotIndex = 0
		byteExtIndex++
	}

	if clamped {
		return totalCopied, io.EOF
	}
	return totalCopied, nil
}

// WriteTo streams the full object to w. Implements io.WriterTo.
func (obj *Object) WriteTo(w io.Writer) (int64, error) {
	sr := io.NewSectionReader(obj, 0, obj.totalSize)
	return io.Copy(w, sr)
}

// locateByteOffset walks byteExtents to find which extent and offset-within-extent
// a given global byte offset falls in.
func locateByteOffset(exts []byteExtent, off int64) (extIndex int, offsetInExt int64) {
	remaining := off
	for i, ext := range exts {
		if remaining < int64(ext.size) { //nolint:gosec // G115: ext.size bounded by segment capacity
			return i, remaining
		}
		remaining -= int64(ext.size) //nolint:gosec // G115: ext.size bounded by segment capacity
	}
	return len(exts), 0
}
