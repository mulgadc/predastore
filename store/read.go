package store

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// Lookup fetches the Shard metadata for (objectHash, shardIndex) from the store index.
func (store *Store) Lookup(objectHash [32]byte, shardIndex int) (*Shard, error) {
	data, err := store.index.Get(makeShardKey(objectHash, shardIndex))
	if err != nil {
		return nil, fmt.Errorf("store: Lookup: %w", err)
	}
	sh, err := decodeShard(data)
	if err != nil {
		return nil, fmt.Errorf("store: Lookup: decode: %w", err)
	}
	sh.store = store
	return sh, nil
}

// NewReader returns an *io.SectionReader over the full shard.
func (shard *Shard) NewReader() *io.SectionReader {
	return io.NewSectionReader(shard, 0, int64(shard.TotalSize))
}

// NewSectionReader returns an *io.SectionReader over [off, off+n) bytes of the shard.
func (shard *Shard) NewSectionReader(offset int64, n int64) *io.SectionReader {
	return io.NewSectionReader(shard, offset, n)
}

// ReadAt implements io.ReaderAt. Each call is stateless and thread-safe.
func (shard *Shard) ReadAt(p []byte, offset int64) (int, error) {
	totalSize := int64(shard.TotalSize)
	if offset >= totalSize {
		return 0, io.EOF
	}

	// Clamp to shard bounds.
	clamped := false
	if offset+int64(len(p)) > totalSize {
		p = p[:totalSize-offset]
		clamped = true
	}

	var locIdx int
	var byteInLoc int64
	remaining := offset
	for i, loc := range shard.Locations {
		if remaining < int64(loc.Size) {
			locIdx = i
			byteInLoc = remaining
			break
		}
		remaining -= int64(loc.Size)
		locIdx = len(shard.Locations)
	}
	slotIdx := int(byteInLoc / slotPayloadSize)
	offsetInSlot := int(byteInLoc % slotPayloadSize)

	bufPtr := slotBufferPool.Get().(*[]byte) //nolint:forcetypeassert,errcheck
	buf := *bufPtr
	defer slotBufferPool.Put(bufPtr)

	totalCopied := 0

	for locIdx < len(shard.Locations) && totalCopied < len(p) {
		loc := shard.Locations[locIdx]
		slotsInLoc := (int(loc.Size) + slotPayloadSize - 1) / slotPayloadSize

		f, err := os.Open(filepath.Join(shard.store.dir, fmt.Sprintf("%016d%s", loc.SegmentNum, extension)))
		if err != nil {
			return totalCopied, fmt.Errorf("store: ReadAt: open segment %d: %w", loc.SegmentNum, err)
		}

		for slotIdx < slotsInLoc && totalCopied < len(p) {
			diskOff := int64(loc.Offset) + int64(slotIdx)*int64(totalSlotSize)

			if _, err := f.ReadAt(buf[:totalSlotSize], diskOff); err != nil {
				f.Close()
				return totalCopied, fmt.Errorf("store: ReadAt: read segment %d offset %d: %w",
					loc.SegmentNum, diskOff, err)
			}

			storedCRC := binary.BigEndian.Uint32(buf[28:32])
			binary.BigEndian.PutUint32(buf[28:32], 0)
			computed := crc32.ChecksumIEEE(buf[0:slotHeaderSize])
			computed = crc32.Update(computed, crc32.IEEETable, buf[slotHeaderSize:slotHeaderSize+slotPayloadSize])

			if computed != storedCRC {
				f.Close()
				return totalCopied, fmt.Errorf("store: ReadAt: CRC mismatch segment %d offset %d: stored=%08x computed=%08x",
					loc.SegmentNum, diskOff, storedCRC, computed)
			}

			length := int(binary.BigEndian.Uint32(buf[20:24]))
			if length > slotPayloadSize {
				f.Close()
				return totalCopied, fmt.Errorf("store: ReadAt: invalid length %d in segment %d offset %d",
					length, loc.SegmentNum, diskOff)
			}

			payloadStart := slotHeaderSize + offsetInSlot
			payloadEnd := slotHeaderSize + length
			n := copy(p[totalCopied:], buf[payloadStart:payloadEnd])
			totalCopied += n
			offsetInSlot = 0
			slotIdx++
		}

		f.Close()
		slotIdx = 0
		locIdx++
	}

	if clamped {
		return totalCopied, io.EOF
	}
	return totalCopied, nil
}
