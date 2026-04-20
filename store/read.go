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
	// Attach the Store so the Shard can open segment files for reads.
	sh.store = store
	return sh, nil
}

// NewReader returns an io.SectionReader over the full shard.
func (shard *Shard) NewReader() *io.SectionReader {
	return io.NewSectionReader(shard, 0, int64(shard.TotalSize))
}

// NewSectionReader returns an io.SectionReader over [offset, offset+n) bytes of the shard.
func (shard *Shard) NewSectionReader(offset int64, n int64) *io.SectionReader {
	return io.NewSectionReader(shard, offset, n)
}

// ReadAt reads len(p) bytes from the shard starting at byte offset. Stateless and safe for concurrent use.
func (shard *Shard) ReadAt(p []byte, offset int64) (int, error) {
	totalSize := int64(shard.TotalSize)
	if offset >= totalSize {
		return 0, io.EOF
	}

	// Clamp read to shard bounds; signal EOF after filling.
	clamped := false
	if offset+int64(len(p)) > totalSize {
		p = p[:totalSize-offset]
		clamped = true
	}

	// Walk Locations to find the starting location and byte offset within it.
	var locIndex int
	var offsetInLoc int64
	remaining := offset
	for i, loc := range shard.Locations {
		if remaining < int64(loc.Size) {
			locIndex = i
			offsetInLoc = remaining
			break
		}
		remaining -= int64(loc.Size)
		locIndex = len(shard.Locations)
	}
	// Convert byte offset within the location to a slot index and offset within that slot.
	slotIndex := int(offsetInLoc / slotPayloadSize)
	offsetInSlot := int(offsetInLoc % slotPayloadSize)

	bufPtr := slotBufferPool.Get().(*[]byte) //nolint:forcetypeassert,errcheck
	buf := *bufPtr
	defer slotBufferPool.Put(bufPtr)

	totalCopied := 0

	for locIndex < len(shard.Locations) && totalCopied < len(p) {
		loc := shard.Locations[locIndex]
		// Derive slot count from the location's logical byte size.
		slotsInLoc := (int(loc.Size) + slotPayloadSize - 1) / slotPayloadSize

		f, err := os.Open(filepath.Join(shard.store.dir, fmt.Sprintf("%016d%s", loc.SegmentNum, extension)))
		if err != nil {
			return totalCopied, fmt.Errorf("store: ReadAt: open segment %d: %w", loc.SegmentNum, err)
		}

		for slotIndex < slotsInLoc && totalCopied < len(p) {
			// Compute absolute file offset for this slot.
			diskOffset := int64(loc.Offset) + int64(slotIndex)*int64(totalSlotSize)

			// Read the full slot (header + padded payload) into the buffer.
			if _, err := f.ReadAt(buf[:totalSlotSize], diskOffset); err != nil {
				f.Close()
				return totalCopied, fmt.Errorf("store: ReadAt: read segment %d offset %d: %w",
					loc.SegmentNum, diskOffset, err)
			}

			// Validate CRC: zero the stored field, recompute over header + payload.
			storedCRC := binary.BigEndian.Uint32(buf[28:32])
			binary.BigEndian.PutUint32(buf[28:32], 0)
			computed := crc32.ChecksumIEEE(buf[0:slotHeaderSize])
			computed = crc32.Update(computed, crc32.IEEETable, buf[slotHeaderSize:slotHeaderSize+slotPayloadSize])

			if computed != storedCRC {
				f.Close()
				return totalCopied, fmt.Errorf("store: ReadAt: CRC mismatch segment %d offset %d: stored=%08x computed=%08x",
					loc.SegmentNum, diskOffset, storedCRC, computed)
			}

			// Extract the logical payload length from the slot header [20:24].
			payloadLen := int(binary.BigEndian.Uint32(buf[20:24]))
			if payloadLen > slotPayloadSize {
				f.Close()
				return totalCopied, fmt.Errorf("store: ReadAt: invalid payload length %d in segment %d offset %d",
					payloadLen, loc.SegmentNum, diskOffset)
			}

			// Copy payload bytes, skipping offsetInSlot on the first slot only.
			payloadStart := slotHeaderSize + offsetInSlot
			payloadEnd := slotHeaderSize + payloadLen
			n := copy(p[totalCopied:], buf[payloadStart:payloadEnd])
			totalCopied += n
			offsetInSlot = 0
			slotIndex++
		}

		f.Close()
		slotIndex = 0
		locIndex++
	}

	if clamped {
		return totalCopied, io.EOF
	}
	return totalCopied, nil
}
