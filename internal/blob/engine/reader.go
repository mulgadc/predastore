package engine

import (
	"crypto/cipher"
	"errors"
	"fmt"
	"io"
)

var ErrClosedReader = errors.New("closed reader")

// reader gives random and sequential access to one value, decrypting fragments
// as it goes.
type reader struct {
	key     [32]byte
	index   uint32
	epoch   uint64
	storeID uint32
	aead    cipher.AEAD

	seg *segment
	ext extent

	buf []byte

	// held is the pooled window behind buf, or nil when buf is this reader's
	// own. Close gives it back.
	held *[]byte

	// Sequential position for Read; ReadAt is stateless.
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

// ReadAt reads len(p) bytes from the value's logical offset, translating it to
// on-disk fragment positions. A failed decrypt wraps ErrIntegrity, so
// corruption, tamper and a wrong master key all surface the same way.
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

	// Only the first fragment can start mid-body; every later one begins at 0,
	// since each non-final copy consumes exactly fragBodySize logical bytes.
	bodyOffset := int(off % fragBodySize)

	batchCap := len(r.buf) / totalFragSize

	// One seg.ReadAt fills the window, then each fragment is opened in place —
	// mirroring the writer, so bufLen tunes syscall batching on both sides.
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

			plaintext, err := frag.open(r.aead, r.key, r.index, r.storeID)
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

// WriteTo streams the full value to w via io.SectionReader over ReadAt.
func (r *reader) WriteTo(w io.Writer) (int64, error) {
	if r.closed {
		return 0, ErrClosedReader
	}

	return io.Copy(w, io.NewSectionReader(r, 0, r.ext.LSize))
}

// Size returns the logical (data-only) size of the value, excluding fragment headers.
// Epoch reports the write epoch this value was stored under. The read path
// compares it against the epoch the placement record names, so a shard left
// behind by a node that missed an overwrite reads as absent rather than as
// data.
func (r *reader) Epoch() uint64 { return r.epoch }

func (r *reader) Size() int64 {
	return r.ext.LSize
}

// Close releases the segment reference. Must be called exactly once.
func (r *reader) Close() error {
	if r.closed {
		return fmt.Errorf("closed reader")
	}

	r.closed = true
	dropFragWindow(r.held)
	r.held, r.buf = nil, nil
	r.seg.releaseRef()
	return nil
}
