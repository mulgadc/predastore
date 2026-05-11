package store

import (
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const readBufLen = 32

// shardReader provides random and sequential access to a single shard's data.
// It reads one fragment at a time from disk, opens it under AES-256-GCM, and
// copies the plaintext into the caller's buffer. bufPos tracks the logical
// read position for sequential Read() calls; ReadAt is stateless.
type shardReader struct {
	objectHash [32]byte
	shardIndex uint32
	storeID    uint32
	aead       cipher.AEAD // shared cipher.AEAD built once at Store.Open; safe for concurrent use.

	seg *segment // nil for 0-byte shards (no segment to read from).
	ext extent

	buf    []byte
	bufPos int64

	closed bool
}

var ErrClosedReader = errors.New("closed reader")

// Read implements io.Reader by delegating to ReadAt at the current position.
func (r *shardReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, ErrClosedReader
	}

	if r.bufPos >= r.ext.LSize {
		return 0, io.EOF
	}

	n, err := r.ReadAt(p, r.bufPos)
	r.bufPos += int64(n)
	return n, err
}

// ReadAt reads len(p) bytes from the shard starting at logical byte offset.
// Translates logical offsets to on-disk fragment positions:
//
//	fragIndex = logicalPos / fragBodySize
//	diskOff   = ext.Off + fragIndex * totalFragSize
//
// Fragments are read in batches of up to readBufLen at a time — one
// seg.ReadAt fills the buffer, then each fragment is opened in place. This
// mirrors the writer's one-WriteAt-per-window pattern so readBufLen acts as a
// tunable syscall-batch knob symmetric with writeBufLen.
//
// Each fragment's body+tag is opened under AES-256-GCM. A failed Open wraps
// ErrIntegrity — disk corruption, tamper, and wrong master key share one path.
func (r *shardReader) ReadAt(p []byte, off int64) (int, error) {
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

	for totalCopied < len(p) {
		logicalPos := off + int64(totalCopied)
		startFragIdx := logicalPos / fragBodySize
		endFragIdx := (logicalPos + int64(len(p)-totalCopied) - 1) / fragBodySize
		batchFragCount := min(int(endFragIdx-startFragIdx+1), readBufLen)
		batchDiskOff := r.ext.Off + startFragIdx*totalFragSize

		if _, err := r.seg.ReadAt(r.buf[:batchFragCount*totalFragSize], batchDiskOff); err != nil {
			return totalCopied, fmt.Errorf("read segment %d at offset %d: %w", r.ext.SegNum, batchDiskOff, err)
		}

		for i := 0; i < batchFragCount && totalCopied < len(p); i++ {
			pos := i * totalFragSize
			fragDiskOff := batchDiskOff + int64(pos)

			// Reconstruct AAD/nonce from the on-disk header. Header tamper
			// shifts AAD or nonce — Open then fails to authenticate.
			fragNum := binary.BigEndian.Uint64(r.buf[pos : pos+8])
			shardNum := binary.BigEndian.Uint64(r.buf[pos+8 : pos+16])
			aad := makeAAD(r.objectHash, r.shardIndex, shardNum, fragNum)
			nonce := makeNonce(fragNum, r.storeID)

			// plaintext aliases r.buf[pos+fragHeaderSize : pos+fragHeaderSize+fragBodySize].
			ciphertext := r.buf[pos+fragHeaderSize : pos+totalFragSize]
			plaintext, err := openFragment(r.aead, ciphertext, aad[:], nonce[:])
			if err != nil {
				return totalCopied, fmt.Errorf("segment %d offset %d: %w: %w", r.ext.SegNum, fragDiskOff, ErrIntegrity, err)
			}

			payloadLen := int(binary.BigEndian.Uint32(r.buf[pos+20 : pos+24]))
			if payloadLen > fragBodySize {
				return totalCopied, fmt.Errorf("invalid payload length %d in segment %d at offset %d", payloadLen, r.ext.SegNum, fragDiskOff)
			}

			n := copy(p[totalCopied:], plaintext[bodyOffset:payloadLen])
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
func (r *shardReader) WriteTo(w io.Writer) (int64, error) {
	if r.closed {
		return 0, ErrClosedReader
	}

	return io.Copy(w, io.NewSectionReader(r, 0, r.ext.LSize))
}

// Size returns the logical (data-only) size of the shard, excluding fragment headers.
func (r *shardReader) Size() int64 {
	return r.ext.LSize
}

// Close releases the segment reference. Must be called exactly once.
// 0-byte readers carry no segment reference, so Close is a no-op beyond
// flipping the closed flag.
func (r *shardReader) Close() error {
	if r.closed {
		return fmt.Errorf("shard reader closed")
	}

	r.closed = true
	if r.seg != nil {
		r.seg.refs.Add(-1)
	}

	return nil
}
