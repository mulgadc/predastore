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
func (sh *Shard) NewReader() *io.SectionReader {
	return io.NewSectionReader(sh, 0, int64(sh.TotalSize))
}

// NewSectionReader returns an *io.SectionReader over [off, off+n) bytes of the shard.
func (sh *Shard) NewSectionReader(off int64, n int64) *io.SectionReader {
	return io.NewSectionReader(sh, off, n)
}

// ReadAt implements io.ReaderAt. Each call is stateless and thread-safe.
func (sh *Shard) ReadAt(p []byte, off int64) (int, error) {
	totalSize := int64(sh.TotalSize)
	if off >= totalSize {
		return 0, io.EOF
	}

	// Clamp to shard bounds.
	clamped := false
	if off+int64(len(p)) > totalSize {
		p = p[:totalSize-off]
		clamped = true
	}

	globalSlot := int(off / slotPayloadSize)
	offsetInSlot := int(off % slotPayloadSize)
	locIdx, slotIdx := locateSlot(sh.Locations, globalSlot)

	bufPtr := slotBufferPool.Get().(*[]byte) //nolint:forcetypeassert,errcheck
	buf := *bufPtr
	defer slotBufferPool.Put(bufPtr)

	totalCopied := 0

	for locIdx < len(sh.Locations) && totalCopied < len(p) {
		loc := sh.Locations[locIdx]

		f, err := os.Open(segmentFilePath(sh.store.dir, loc.SegmentNum))
		if err != nil {
			return totalCopied, fmt.Errorf("store: ReadAt: open segment %d: %w", loc.SegmentNum, err)
		}

		for slotIdx < int(loc.Size) && totalCopied < len(p) {
			diskOff := int64(segmentHeaderSize) + int64(int(loc.Offset)+slotIdx)*int64(totalSlotSize)

			if _, err := f.ReadAt(buf[:totalSlotSize], diskOff); err != nil {
				f.Close()
				return totalCopied, fmt.Errorf("store: ReadAt: read segment %d slot %d: %w",
					loc.SegmentNum, int(loc.Offset)+slotIdx, err)
			}

			storedCRC := binary.BigEndian.Uint32(buf[28:32])
			binary.BigEndian.PutUint32(buf[28:32], 0)
			computed := crc32.ChecksumIEEE(buf[0:slotHeaderSize])
			computed = crc32.Update(computed, crc32.IEEETable, buf[slotHeaderSize:slotHeaderSize+slotPayloadSize])

			if computed != storedCRC {
				f.Close()
				return totalCopied, fmt.Errorf("store: ReadAt: CRC mismatch segment %d slot %d: stored=%08x computed=%08x",
					loc.SegmentNum, int(loc.Offset)+slotIdx, storedCRC, computed)
			}

			length := int(binary.BigEndian.Uint32(buf[20:24]))
			if length > slotPayloadSize {
				f.Close()
				return totalCopied, fmt.Errorf("store: ReadAt: invalid length %d in segment %d slot %d",
					length, loc.SegmentNum, int(loc.Offset)+slotIdx)
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

func segmentFilePath(dir string, num uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%016d%s", num, extension))
}

func locateSlot(locations []Location, globalSlot int) (locIdx, slotIdx int) {
	skipped := 0
	for i, loc := range locations {
		locSlots := int(loc.Size)
		if skipped+locSlots > globalSlot {
			return i, globalSlot - skipped
		}
		skipped += locSlots
	}
	return len(locations), 0
}
