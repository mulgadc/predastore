package store

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

// Append writes an object shard to the store and commits its index entry. It
// reserves slot extents under a short lock, then drains r into those slots
// lock-free and fsyncs before returning.
func (store *Store) Append(objectHash [32]byte, shardIndex int, size int, r io.Reader) (*Shard, error) {
	// Phase 1: Reserve slots under the lock.
	store.mu.Lock()

	// Number of slots needed to hold size bytes of payload.
	fragCount := (size + slotPayloadSize - 1) / slotPayloadSize

	// Snapshot and advance monotonic counters while still under the lock.
	seqNum := store.seqNum.Load()
	store.seqNum.Add(uint64(fragCount)) //nolint:gosec // G115: fragCount non-negative
	shardNum := store.shardNum.Add(1)
	slotExts, err := store.reserve(fragCount)

	store.mu.Unlock()

	if err != nil {
		return nil, fmt.Errorf("store: Append: reserve: %w", err)
	}

	// Release extent refs when done, allowing retired segments to close.
	defer func() {
		for _, ext := range slotExts {
			ext.Close()
		}
	}()

	// Phase 2: Write slots lock-free.
	remaining := size
	fragIndex := 0

	for _, ext := range slotExts {
		for i := 0; i < ext.size; i++ {
			bufPtr := slotBufferPool.Get().(*[]byte) //nolint:forcetypeassert,errcheck // Pool.New always returns *[]byte
			buf := *bufPtr

			// Read up to one slot's worth of payload from the input.
			toRead := min(remaining, slotPayloadSize)
			payloadLen := 0
			if toRead > 0 {
				payloadLen, err = io.ReadFull(r, buf[slotHeaderSize:slotHeaderSize+toRead])
				if err != nil {
					slotBufferPool.Put(bufPtr)
					return nil, fmt.Errorf("store: Append: read chunk %d: %w", fragIndex, err)
				}
			}

			// Zero-pad the remainder so the CRC covers a full slot.
			if payloadLen < slotPayloadSize {
				padStart := slotHeaderSize + payloadLen
				copy(buf[padStart:slotHeaderSize+slotPayloadSize], zeroPadBuffer[:slotPayloadSize-payloadLen])
			}

			// Encode the 32-byte slot header.
			binary.BigEndian.PutUint64(buf[0:8], seqNum+uint64(fragIndex)) // [0:8]   sequence number
			binary.BigEndian.PutUint64(buf[8:16], shardNum)                // [8:16]  shard number
			binary.BigEndian.PutUint32(buf[16:20], uint32(fragIndex))      // [16:20] fragment index
			binary.BigEndian.PutUint32(buf[20:24], uint32(payloadLen))     //nolint:gosec // G115: payloadLen bounded by slotPayloadSize (8 KiB) // [20:24] payload length

			var flags slotFlags
			if fragIndex == fragCount-1 {
				flags = flagEndOfShard
			}
			binary.BigEndian.PutUint32(buf[24:28], uint32(flags)) // [24:28] flags
			binary.BigEndian.PutUint32(buf[28:32], 0)             // [28:32] CRC placeholder

			// CRC32-IEEE over header (with CRC field zeroed) + full padded payload.
			checksum := crc32.ChecksumIEEE(buf[0:slotHeaderSize])
			checksum = crc32.Update(checksum, crc32.IEEETable, buf[slotHeaderSize:slotHeaderSize+slotPayloadSize])
			binary.BigEndian.PutUint32(buf[28:32], checksum)

			if _, err := ext.seg.file.WriteAt(buf[:slotHeaderSize+slotPayloadSize], ext.seg.byteOffset(ext.offset+i)); err != nil {
				slotBufferPool.Put(bufPtr)
				return nil, fmt.Errorf("store: Append: WriteAt slot %d: %w", fragIndex, err)
			}

			slotBufferPool.Put(bufPtr)
			remaining -= payloadLen
			fragIndex++
		}
	}

	// Phase 3: Commit. Fsync all touched segments, then write shard metadata to the index.
	for _, ext := range slotExts {
		if err := ext.seg.file.Sync(); err != nil {
			return nil, fmt.Errorf("store: Append: fsync segment %d: %w", ext.seg.num, err)
		}
	}

	// Build byte extents from slot extents.
	remaining = size
	byteExts := make([]ByteExtent, len(slotExts))
	for i, slotExt := range slotExts {
		payloadBytes := min(remaining, slotExt.size*slotPayloadSize)
		byteExts[i] = ByteExtent{
			SegmentNum: slotExt.seg.num,
			Offset:     uint64(slotExt.seg.byteOffset(slotExt.offset)), //nolint:gosec // G115: byteOffset non-negative
			Size:       uint64(payloadBytes),                           //nolint:gosec // G115: payloadBytes non-negative
		}
		remaining -= payloadBytes
	}

	shard := &Shard{
		ObjectHash:  objectHash,
		ShardIndex:  shardIndex,
		TotalSize:   size,
		ByteExtents: byteExts,
		store:       store,
	}

	encoded, err := encodeShard(shard)
	if err != nil {
		return nil, fmt.Errorf("store: Append: encode shard: %w", err)
	}
	if err := store.index.Set(makeShardKey(objectHash, shardIndex), encoded); err != nil {
		return nil, fmt.Errorf("store: Append: commit to index: %w", err)
	}

	return shard, nil
}
