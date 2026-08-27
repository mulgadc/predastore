package engine

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

var ErrClosedWriter = errors.New("closed writer")

// writer buffers one value's fragments and seals each body in place before
// flushing.
type writer struct {
	store   *Store
	key     [32]byte
	index   uint32
	epoch   uint64
	storeID uint32

	seg *segment
	ext extent

	valueNum uint64
	fragNum  uint64

	// The unflushed fragment window, mirroring the on-disk layout so flush can
	// issue one WriteAt for the whole thing.
	buf []byte

	// Offsets into the value's extent: cursor is where the next byte goes,
	// flushedTo is fixed to the last successful WriteAt, and their difference is
	// the live buffer fill.
	cursor    int64
	flushedTo int64

	// Logical bytes written, tracked so it isn't recomputed from cursor per loop.
	dataLen int64

	closed bool
}

// Write copies p into the value via ReadFrom over a bytes.Reader, sharing the
// header/flush loop. Returns ErrValueFull if p doesn't fit in the remaining
// logical capacity.
func (w *writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, ErrClosedWriter
	}

	r := bytes.NewReader(p)
	n, err := w.ReadFrom(r)
	if err == nil && r.Len() > 0 {
		err = ErrValueFull
	}

	return int(n), err
}

// ReadFrom streams from r into the value, one fragment per iteration.
//
// If r runs dry before the value is full, returns the bytes consumed and
// io.EOF. Close still flushes what was buffered, but the extent's LSize will
// overstate the data, and reading the unfilled tail surfaces ErrIntegrity.
func (w *writer) ReadFrom(r io.Reader) (total int64, err error) {
	if w.closed {
		return 0, ErrClosedWriter
	}

	for w.dataLen < w.ext.LSize {
		bufPos := int(w.cursor - w.flushedTo)
		frag := (*fragment)(w.buf[bufPos : bufPos+totalFragSize])
		frag.stampHeader(w.fragNum, w.valueNum)
		w.fragNum++
		w.cursor += fragHeaderSize

		// Cap the fill at the value's remaining bytes so the final partial
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

// Close flushes, makes the data durable, then records the extent as prepared.
// It does not publish it: the value keeps serving its previous data until
// Store.Commit runs, which is what makes an overwrite spanning several shards
// abortable rather than half-applied. Must be called exactly once; it is what
// releases the segment reference Append took.
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

	// The .idx row must be durable before the index commit: it is compaction's
	// only enumeration source, so every index-committed extent must already be
	// findable in .idx or a drop could lose a live extent.
	if err = w.seg.syncIdx(); err != nil {
		return fmt.Errorf("sync idx %d: %w", w.ext.SegNum, err)
	}

	return w.store.prepareExtent(w.key, w.index, w.ext, w.epoch)
}

// flush seals each buffered fragment in place under aead, then writes the
// whole window in one WriteAt. If final, the last fragment is sealed with the
// flagEndOfValue flag and its size set to the actual data byte count.
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
			flags = flagEndOfValue
		}

		frag.seal(w.store.aead, w.key, w.index, w.storeID, size, flags)
	}

	if _, err := w.seg.WriteAt(w.buf[:writeLen], w.ext.Off+w.flushedTo); err != nil {
		return fmt.Errorf("write to segment %d at offset %d: %w", w.ext.SegNum, w.ext.Off+w.flushedTo, err)
	}

	w.flushedTo = w.cursor
	return nil
}
