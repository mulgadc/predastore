package store

import (
	"bytes"
	"crypto/cipher"
	"errors"
	"fmt"
	"io"
)

const writeBufLen = 32

// shardWriter buffers a single shard's fragments and seals each body in place
// before flushing. The internal buffer mirrors the on-disk layout (headers,
// body slots, and tag slots interleaved) so flush can issue one WriteAt for
// the whole window. bufPos advances past each fragment's tag slot when the
// body fills, so the next iteration starts cleanly on a fragment boundary.
type shardWriter struct {
	objectHash [32]byte
	shardIndex uint32
	storeID    uint32
	aead       cipher.AEAD // shared cipher.AEAD built once at Store.Open; safe for concurrent use.

	seg *segment // nil for 0-byte shards (no segment reservation).
	ext extent

	shardNum uint64
	fragNum  uint64

	buf    []byte
	bufPos int64
	extPos int64

	onClose func() error
	closed  bool
}

var ErrClosedWriter = errors.New("closed writer")

// dataWritten derives logical bytes written from bufPos. Each complete fragment
// contributes fragBodySize logical bytes; a partial fragment contributes
// (bufPos % totalFragSize - fragHeaderSize) bytes once past the header.
func (w *shardWriter) dataWritten() int64 {
	return w.bufPos/totalFragSize*fragBodySize + max(0, w.bufPos%totalFragSize-fragHeaderSize)
}

// Write copies p into the shard via ReadFrom over a bytes.Reader, sharing the
// header/flush loop. Returns "shard full" if p doesn't fit in the remaining
// logical capacity; the trailing io.EOF from a fully-consumed reader is
// swallowed.
func (w *shardWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, ErrClosedWriter
	}

	r := bytes.NewReader(p)
	n, err := w.ReadFrom(r)
	if errors.Is(err, io.EOF) {
		err = nil
	}

	if err == nil && r.Len() > 0 {
		err = fmt.Errorf("shard full")
	}

	return int(n), err
}

// ReadFrom streams from r into the shard, one fragment per iteration.
// io.ReadFull fills the body, capped at the remaining shard bytes so the
// final partial fragment doesn't try to over-read. Flush triggers when the
// buffer window fills or the shard is done.
func (w *shardWriter) ReadFrom(r io.Reader) (total int64, err error) {
	if w.closed {
		return 0, ErrClosedWriter
	}

	for w.dataWritten() < w.ext.LSize {
		bufPos := int(w.bufPos - w.extPos)
		frag := (*fragment)(w.buf[bufPos : bufPos+totalFragSize])
		frag.stampHeader(w.fragNum, w.shardNum)
		w.fragNum++
		w.bufPos += fragHeaderSize

		dataLeft := w.ext.LSize - w.dataWritten()
		want := int(min(int64(fragBodySize), dataLeft))
		n, readErr := io.ReadFull(r, frag.body()[:want])
		if errors.Is(readErr, io.ErrUnexpectedEOF) {
			readErr = io.EOF
		}
		w.bufPos += int64(n)
		total += int64(n)

		// Skip the tag slot when the body filled, so the next iteration
		// starts at a fragment boundary. The tag bytes themselves are
		// written in place by flush()'s Seal.
		if n == fragBodySize {
			w.bufPos += fragTagSize
		}

		if int(w.bufPos-w.extPos) >= len(w.buf) || w.dataWritten() >= w.ext.LSize {
			if err := w.flush(w.dataWritten() >= w.ext.LSize); err != nil {
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
// commits the extent to the index via onClose. Must be called exactly once.
// The segment ref is always decremented on exit; the index commit only runs
// when the data is fully durable, preserving rollback on flush/Sync failure.
//
// 0-byte shards have no segment — Close skips flush/Sync and just runs the
// index commit.
func (w *shardWriter) Close() (err error) {
	if w.closed {
		return ErrClosedWriter
	}

	w.closed = true

	if w.seg != nil {
		defer w.seg.refs.Add(-1)

		if w.bufPos > w.extPos {
			if err = w.flush(true); err != nil {
				return err
			}
		}

		if err = w.seg.Sync(); err != nil {
			return fmt.Errorf("sync segment %d: %w", w.ext.SegNum, err)
		}
	}

	return w.onClose()
}

// flush seals each buffered fragment in place under aead, then writes the
// whole window in one WriteAt. If final, the last fragment is sealed with the
// flagEndOfShard flag and its size set to the actual data byte count.
func (w *shardWriter) flush(final bool) error {
	bufUsed := int(w.bufPos - w.extPos)
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

		frag.seal(w.aead, w.objectHash, w.shardIndex, w.storeID, size, flags)
	}

	if _, err := w.seg.WriteAt(w.buf[:writeLen], w.ext.Off+w.extPos); err != nil {
		return fmt.Errorf("write to segment %d at offset %d: %w", w.ext.SegNum, w.ext.Off+w.extPos, err)
	}

	w.extPos = w.bufPos
	return nil
}
