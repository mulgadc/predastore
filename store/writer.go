package store

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

var ErrClosedWriter = errors.New("closed writer")

// writer buffers one shard's fragments and seals each body in place before
// flushing.
type writer struct {
	store      *Store
	objectHash [32]byte
	shardIndex uint32
	storeID    uint32

	seg *segment
	ext extent

	shardNum uint64
	fragNum  uint64

	// The unflushed fragment window, mirroring the on-disk layout so flush can
	// issue one WriteAt for the whole thing.
	buf []byte

	// Offsets into the shard's extent: cursor is where the next byte goes,
	// flushedTo is fixed to the last successful WriteAt, and their difference is
	// the live buffer fill.
	cursor    int64
	flushedTo int64

	// Logical bytes written, tracked so it isn't recomputed from cursor per loop.
	dataLen int64

	closed bool
}

// Write copies p into the shard via ReadFrom over a bytes.Reader, sharing the
// header/flush loop. Returns ErrShardFull if p doesn't fit in the remaining
// logical capacity.
func (w *writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, ErrClosedWriter
	}

	r := bytes.NewReader(p)
	n, err := w.ReadFrom(r)
	if err == nil && r.Len() > 0 {
		err = ErrShardFull
	}

	return int(n), err
}

// ReadFrom streams from r into the shard, one fragment per iteration.
//
// If r runs dry before the shard is full, returns the bytes consumed and
// io.EOF. Close still flushes what was buffered, but the extent's LSize will
// overstate the data, and reading the unfilled tail surfaces ErrIntegrity.
func (w *writer) ReadFrom(r io.Reader) (total int64, err error) {
	if w.closed {
		return 0, ErrClosedWriter
	}

	for w.dataLen < w.ext.LSize {
		bufPos := int(w.cursor - w.flushedTo)
		frag := (*fragment)(w.buf[bufPos : bufPos+totalFragSize])
		frag.stampHeader(w.fragNum, w.shardNum)
		w.fragNum++
		w.cursor += fragHeaderSize

		// Cap the fill at the shard's remaining bytes so the final partial
		// fragment doesn't over-read.
		dataLeft := w.ext.LSize - w.dataLen
		want := int(min(int64(fragBodySize), dataLeft))
		n, readErr := io.ReadFull(r, frag.body()[:want])
		if errors.Is(readErr, io.ErrUnexpectedEOF) {
			readErr = io.EOF
		}
		w.cursor += int64(n)
		w.dataLen += int64(n)
		total += int64(n)

		// Skip the tag slot so the next iteration lands on a fragment boundary;
		// flush's Seal writes the tag bytes themselves.
		if n == fragBodySize {
			w.cursor += fragTagSize
		}

		if int(w.cursor-w.flushedTo) >= len(w.buf) || w.dataLen >= w.ext.LSize {
			if err := w.flush(w.dataLen >= w.ext.LSize); err != nil {
				return total, err
			}
		}

		if readErr != nil {
			return total, readErr
		}
	}

	return total, nil
}

// Close flushes, makes the data durable, then commits the extent to the index —
// so a failure before that last step leaves the shard's previous data intact.
// Must be called exactly once; it is what releases the segment reference Append
// took.
func (w *writer) Close() (err error) {
	if w.closed {
		return ErrClosedWriter
	}

	w.closed = true
	// Deferred, so the segment stays marked as holding an uncommitted write
	// until commitExtent below has either landed or failed for good.
	defer w.seg.releaseWriteRef()

	if w.cursor > w.flushedTo {
		if err = w.flush(true); err != nil {
			return err
		}
	}

	if err = w.seg.Sync(); err != nil {
		return fmt.Errorf("sync segment %d: %w", w.ext.SegNum, err)
	}

	// The .idx row must be durable before the index commit: it is compaction's
	// only enumeration source, so every index-committed extent must already be
	// findable in .idx or a drop could lose a live extent.
	if err = w.seg.syncIdx(); err != nil {
		return fmt.Errorf("sync idx %d: %w", w.ext.SegNum, err)
	}

	return w.store.commitExtent(w.objectHash, w.shardIndex, w.ext)
}

// flush seals each buffered fragment in place under aead, then writes the
// whole window in one WriteAt. If final, the last fragment is sealed with the
// flagEndOfShard flag and its size set to the actual data byte count.
func (w *writer) flush(final bool) error {
	bufUsed := int(w.cursor - w.flushedTo)
	if bufUsed <= 0 {
		return nil
	}

	fragCount := (bufUsed + totalFragSize - 1) / totalFragSize
	writeLen := fragCount * totalFragSize

	for i := range fragCount {
		pos := i * totalFragSize
		frag := (*fragment)(w.buf[pos : pos+totalFragSize])
		isLast := final && i == fragCount-1

		// The final fragment may be partial (size = data byte count) or
		// fully filled and already tag-skipped (bufUsed - pos - fragHeaderSize
		// would overshoot fragBodySize by fragTagSize, so cap with min).
		size := uint32(fragBodySize)
		var flags fragFlags
		if isLast {
			size = uint32(min(bufUsed-pos-fragHeaderSize, fragBodySize)) //nolint:gosec // bounded by fragBodySize (8 KiB).
			flags = flagEndOfShard
		}

		frag.seal(w.store.aead, w.objectHash, w.shardIndex, w.storeID, size, flags)
	}

	if _, err := w.seg.WriteAt(w.buf[:writeLen], w.ext.Off+w.flushedTo); err != nil {
		return fmt.Errorf("write to segment %d at offset %d: %w", w.ext.SegNum, w.ext.Off+w.flushedTo, err)
	}

	w.flushedTo = w.cursor
	return nil
}
