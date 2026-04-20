package store

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

// Append writes an object shard to the store and commits its index entry.
// It reserves slot extents under a short lock, then drains r into those
// slots lock-free and fsyncs before returning.
func (store *Store) Append(objectHash [32]byte, shardIndex int, size int, r io.Reader) (*Shard, error) {
	// Phase 1: Reserve.
	store.mu.Lock()

	fragCount := (size + slotPayloadSize - 1) / slotPayloadSize

	seqNum := store.seqNum.Load()
	store.seqNum.Add(uint64(fragCount)) //nolint:gosec // G115: fragCount non-negative
	shardNum := store.shardNum.Add(1)
	exts, err := store.reserve(fragCount)

	store.mu.Unlock()

	if err != nil {
		return nil, fmt.Errorf("failed to reserve segment space: %w", err)
	}

	defer func() {
		for _, ext := range exts {
			ext.Close()
		}
	}()

	// Phase 2: Write.
	remaining := size
	fragIndex := 0

	for _, ext := range exts {
		for i := 0; i < ext.size; i++ {
			bufPtr := slotBufferPool.Get().(*[]byte) //nolint:forcetypeassert,errcheck // Pool.New always returns *[]byte
			buf := *bufPtr

			chunkLen, err := readFull(r, buf[slotHeaderSize:slotHeaderSize+slotPayloadSize], remaining)
			if err != nil {
				slotBufferPool.Put(bufPtr)
				return nil, fmt.Errorf("store: Append: read chunk %d: %w", fragIndex, err)
			}

			if chunkLen < slotPayloadSize {
				padStart := slotHeaderSize + chunkLen
				copy(buf[padStart:slotHeaderSize+slotPayloadSize], zeroPadBuffer[:slotPayloadSize-chunkLen])
			}

			binary.BigEndian.PutUint64(buf[0:8], seqNum+uint64(fragIndex))
			binary.BigEndian.PutUint64(buf[8:16], shardNum)
			binary.BigEndian.PutUint32(buf[16:20], uint32(fragIndex))
			binary.BigEndian.PutUint32(buf[20:24], uint32(chunkLen)) //nolint:gosec // G115: chunkLen bounded by slotPayloadSize (8 KiB)

			var flags slotFlags
			if fragIndex == fragCount-1 {
				flags = flagEndOfShard
			}
			binary.BigEndian.PutUint32(buf[24:28], uint32(flags))
			binary.BigEndian.PutUint32(buf[28:32], 0)

			checksum := crc32.ChecksumIEEE(buf[0:slotHeaderSize])
			checksum = crc32.Update(checksum, crc32.IEEETable, buf[slotHeaderSize:slotHeaderSize+slotPayloadSize])
			binary.BigEndian.PutUint32(buf[28:32], checksum)

			if _, err := ext.seg.file.WriteAt(buf[:slotHeaderSize+slotPayloadSize], ext.seg.byteOffset(ext.offset+i)); err != nil {
				slotBufferPool.Put(bufPtr)
				return nil, fmt.Errorf("store: Append: WriteAt slot %d: %w", fragIndex, err)
			}

			slotBufferPool.Put(bufPtr)
			remaining -= chunkLen
			fragIndex++
		}
	}

	// Phase 3: Commit.
	for _, ext := range exts {
		if err := ext.seg.file.Sync(); err != nil {
			return nil, fmt.Errorf("store: Append: fsync segment %d: %w", ext.seg.num, err)
		}
	}

	locations := make([]Location, len(exts))
	for i, ext := range exts {
		locations[i] = Location{
			SegmentNum: ext.seg.num,
			Offset:     uint64(ext.offset), //nolint:gosec // G115: offset non-negative
			Size:       uint64(ext.size),   //nolint:gosec // G115: size non-negative
		}
	}

	sh := &Shard{
		ObjectHash: objectHash,
		ShardIndex: shardIndex,
		TotalSize:  size,
		Locations:  locations,
		store:      store,
	}

	encoded, err := encodeShard(sh)
	if err != nil {
		return nil, fmt.Errorf("store: Append: encode shard: %w", err)
	}
	if err := store.index.Set(makeShardKey(objectHash, shardIndex), encoded); err != nil {
		return nil, fmt.Errorf("store: Append: commit to index: %w", err)
	}

	return sh, nil
}

func readFull(r io.Reader, buf []byte, remaining int) (int, error) {
	toRead := min(remaining, len(buf))
	if toRead == 0 {
		return 0, nil
	}
	return io.ReadFull(r, buf[:toRead])
}
