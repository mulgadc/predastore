package store

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

var ErrClosedWriter = errors.New("closed writer")

// writer buffers a single shard's fragments and seals each body in place
// before flushing. The internal buffer mirrors the on-disk layout (headers,
// body slots, and tag slots interleaved) so flush can issue one WriteAt for
// the whole window. cursor advances past each fragment's tag slot when the
// body fills, so the next iteration starts cleanly on a fragment boundary.
type writer struct {
	store      *Store
	objectHash [32]byte
	shardIndex uint32
	storeID    uint32

	seg *segment
	ext extent

	shardNum uint64
	fragNum  uint64

	// buf holds the unflushed fragment window. cursor is the offset into the
	// shard's extent where the next byte will go; flushedTo is the same kind
	// of offset, fixed to the last successful WriteAt. cursor - flushedTo is
	// the live buffer-fill in bytes. dataLen tracks logical bytes written so
	// we don't recompute it from cursor on every iteration.
	buf       []byte
	cursor    int64
	flushedTo int64
	dataLen   int64

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
// io.ReadFull fills the body, capped at the remaining shard bytes so the
// final partial fragment doesn't try to over-read. Flush triggers when the
// buffer window fills or the shard is done.
//
// If r is exhausted before the shard is filled (underfill), returns the
// bytes consumed and io.EOF — the writer's Close will still flush whatever
// partial bytes were buffered, but the index entry's LSize will exceed the
// actual data; ReadAt of the unfilled tail will surface ErrIntegrity.
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

		dataLeft := w.ext.LSize - w.dataLen
		want := int(min(int64(fragBodySize), dataLeft))
		n, readErr := io.ReadFull(r, frag.body()[:want])
		if errors.Is(readErr, io.ErrUnexpectedEOF) {
			readErr = io.EOF
		}
		w.cursor += int64(n)
		w.dataLen += int64(n)
		total += int64(n)

		// Skip the tag slot when the body filled, so the next iteration
		// starts at a fragment boundary. The tag bytes themselves are
		// written in place by flush()'s Seal.
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

// Close flushes any remaining buffered data, syncs the segment file, then
// commits the extent to the index. Must be called exactly once. The segment
// ref is always decremented on exit; the index commit only runs when the
// data is fully durable, preserving rollback on flush/Sync failure.
func (w *writer) Close() (err error) {
	if w.closed {
		return ErrClosedWriter
	}

	w.closed = true
	defer w.seg.releaseRef()

	if w.cursor > w.flushedTo {
		if err = w.flush(true); err != nil {
			return err
		}
	}

	if err = w.seg.Sync(); err != nil {
		return fmt.Errorf("sync segment %d: %w", w.ext.SegNum, err)
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
