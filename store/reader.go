package store

import (
	"crypto/cipher"
	"errors"
	"fmt"
	"io"
)

var ErrClosedReader = errors.New("closed reader")

// reader provides random and sequential access to a single shard's data.
// It reads fragments from disk in batches of up to bufLen, opens each under
// AES-256-GCM, and copies the plaintext into the caller's buffer. readPos
// tracks the logical position for sequential Read() calls; ReadAt is stateless.
type reader struct {
	objectHash [32]byte
	shardIndex uint32
	storeID    uint32
	aead       cipher.AEAD // shared cipher.AEAD built once at Store.Open; safe for concurrent use.

	seg *segment
	ext extent

	buf     []byte
	readPos int64

	closed bool
}

// Read implements io.Reader by delegating to ReadAt at the current position.
func (r *reader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, ErrClosedReader
	}

	if r.readPos >= r.ext.LSize {
		return 0, io.EOF
	}

	n, err := r.ReadAt(p, r.readPos)
	r.readPos += int64(n)
	return n, err
}

// ReadAt reads len(p) bytes from the shard starting at logical byte offset.
// Translates logical offsets to on-disk fragment positions:
//
//	fragIndex = logicalPos / fragBodySize
//	diskOff   = ext.Off + fragIndex * totalFragSize
//
// Fragments are read in batches of up to bufLen at a time — one seg.ReadAt
// fills the buffer, then each fragment is opened in place. This mirrors the
// writer's one-WriteAt-per-window pattern so bufLen acts as a tunable
// syscall-batch knob symmetric on both sides.
//
// A failed Open wraps ErrIntegrity — disk corruption, tamper, and wrong
// master key share one path.
func (r *reader) ReadAt(p []byte, off int64) (int, error) {
	if r.closed {
		return 0, ErrClosedReader
	}

	if off >= r.ext.LSize {
		return 0, io.EOF
	}

	if off+int64(len(p)) > r.ext.LSize {
		p = p[:r.ext.LSize-off]
	}

	totalCopied := 0
	// Only the very first fragment we touch can start mid-body; all
	// subsequent fragments (within or across batches) begin at body offset 0
	// because each non-final copy consumes exactly fragBodySize logical bytes.
	bodyOffset := int(off % fragBodySize)

	batchCap := len(r.buf) / totalFragSize

	for totalCopied < len(p) {
		logicalPos := off + int64(totalCopied)
		startFragIdx := logicalPos / fragBodySize
		endFragIdx := (logicalPos + int64(len(p)-totalCopied) - 1) / fragBodySize
		batchFragCount := min(int(endFragIdx-startFragIdx+1), batchCap)
		batchDiskOff := r.ext.Off + startFragIdx*totalFragSize

		if _, err := r.seg.ReadAt(r.buf[:batchFragCount*totalFragSize], batchDiskOff); err != nil {
			return totalCopied, fmt.Errorf("read segment %d at offset %d: %w", r.ext.SegNum, batchDiskOff, err)
		}

		for i := 0; i < batchFragCount && totalCopied < len(p); i++ {
			pos := i * totalFragSize
			frag := (*fragment)(r.buf[pos : pos+totalFragSize])

			plaintext, err := frag.open(r.aead, r.objectHash, r.shardIndex, r.storeID)
			if err != nil {
				return totalCopied, fmt.Errorf("segment %d offset %d: %w", r.ext.SegNum, batchDiskOff+int64(pos), err)
			}

			n := copy(p[totalCopied:], plaintext[bodyOffset:])
			totalCopied += n
			bodyOffset = 0
		}
	}

	if off+int64(totalCopied) >= r.ext.LSize {
		return totalCopied, io.EOF
	}

	return totalCopied, nil
}

// WriteTo streams the full shard to w via io.SectionReader over ReadAt.
func (r *reader) WriteTo(w io.Writer) (int64, error) {
	if r.closed {
		return 0, ErrClosedReader
	}

	return io.Copy(w, io.NewSectionReader(r, 0, r.ext.LSize))
}

// Size returns the logical (data-only) size of the shard, excluding fragment headers.
func (r *reader) Size() int64 {
	return r.ext.LSize
}

// Close releases the segment reference. Must be called exactly once.
func (r *reader) Close() error {
	if r.closed {
		return fmt.Errorf("shard reader closed")
	}

	r.closed = true
	r.seg.releaseRef()
	return nil
}
