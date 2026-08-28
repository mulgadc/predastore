// Object bytes are spread over the data shards in blocks that alternate across
// them, so a write can encode one stripe at a time and never hold the object.
// Laying each shard out contiguously instead would mean parity for a given
// offset draws on bytes a whole shard apart, which cannot be done in one pass
// over the body without holding (k-1)/k of it.

package handlers

import (
	"fmt"
	"io"
)

// layout maps object offsets onto a shard and an offset within it. A block
// size equal to the shard size collapses to the contiguous layout the gate
// wrote before it streamed, which is how those objects still read back.
type layout struct {
	dataShards int
	shardSize  int64
	blockSize  int64
}

// newLayout resolves the layout of an object of this size. A block size of
// zero, which is what a placement record written before striping decodes to,
// means one block per shard.
func newLayout(dataShards int, size, blockSize int64) layout {
	shardSize := (size + int64(dataShards) - 1) / int64(dataShards)
	if blockSize <= 0 || blockSize > shardSize {
		blockSize = shardSize
	}

	return layout{dataShards: dataShards, shardSize: shardSize, blockSize: blockSize}
}

// writeLayout is the layout a new object is written with: blocks small enough
// that the working set is fixed, or a single block when the shard is smaller.
func writeLayout(dataShards int, size int64) layout {
	return newLayout(dataShards, size, streamBlockSize)
}

// locate reports which data shard holds an object offset, and where in it.
// The last stripe is short whenever the shard does not divide by the block, so
// offsets in it are spaced by that remainder rather than by the block size.
func (l layout) locate(offset int64) (shard int, at int64) {
	full := l.shardSize / l.blockSize
	head := full * int64(l.dataShards) * l.blockSize
	if offset < head {
		block := offset / l.blockSize

		return int(block % int64(l.dataShards)), block/int64(l.dataShards)*l.blockSize + offset%l.blockSize
	}

	tail := l.shardSize % l.blockSize
	rel := offset - head

	return int(rel / tail), full*l.blockSize + rel%tail
}

// contiguous reports whether an object range is one unbroken run inside a
// single shard, which is the only case a single ranged shard read can serve.
func (l layout) contiguous(start, end int64) bool {
	first, at := l.locate(start)
	last, to := l.locate(end)

	return first == last && to-at == end-start
}

// join reassembles the object from its data shards in the order the layout
// placed them, stopping at the object size so the padding that squares the
// last stripe is not served back to the client.
func (l layout) join(dst io.Writer, shards [][]byte, size int64) error {
	var written int64
	for offset := int64(0); offset < l.shardSize && written < size; offset += l.blockSize {
		n := min(l.blockSize, l.shardSize-offset)
		for i := range l.dataShards {
			if int64(len(shards[i])) < offset+n {
				return fmt.Errorf("data shard %d holds %d bytes, want at least %d",
					i, len(shards[i]), offset+n)
			}
			chunk := shards[i][offset : offset+n]
			if written+int64(len(chunk)) > size {
				chunk = chunk[:size-written]
			}
			if _, err := dst.Write(chunk); err != nil {
				return err
			}
			written += int64(len(chunk))
			if written >= size {
				break
			}
		}
	}
	if written != size {
		return fmt.Errorf("joined %d bytes, want %d", written, size)
	}

	return nil
}
