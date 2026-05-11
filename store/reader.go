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
//
// objectHash, shardIndex, and storeID are carried separately from extent to
// avoid changing the on-disk extent encoding for data already in the index;
// they feed AAD / nonce reconstruction at Open time.
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
// Each fragment's body+tag is opened under AES-256-GCM with AAD reconstructed
// from (objectHash, shardIndex, shardNum, fragNum) and nonce = (fragNum,
// storeID). A failed Open wraps ErrIntegrity — disk corruption, tamper, or
// the wrong master key all surface as the same sentinel.
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
	for totalCopied < len(p) {
		logicalPos := off + int64(totalCopied)
		fragIndex := logicalPos / fragBodySize
		bodyOffset := int(logicalPos % fragBodySize)

		diskOff := r.ext.Off + fragIndex*totalFragSize
		if _, err := r.seg.ReadAt(r.buf[:totalFragSize], diskOff); err != nil {
			return totalCopied, fmt.Errorf("read segment %d at offset %d: %w", r.ext.SegNum, diskOff, err)
		}

		// Reconstruct AAD/nonce from the on-disk header. Header tamper at rest
		// (e.g. flipping fragNum/shardNum) shifts the AAD or nonce — Open then
		// fails to authenticate.
		fragNum := binary.BigEndian.Uint64(r.buf[0:8])
		shardNum := binary.BigEndian.Uint64(r.buf[8:16])
		aad := makeAAD(r.objectHash, r.shardIndex, shardNum, fragNum)
		nonce := makeNonce(fragNum, r.storeID)

		// Open in place. plaintext aliases r.buf[fragHeaderSize:fragHeaderSize+fragBodySize].
		ciphertext := r.buf[fragHeaderSize:totalFragSize]
		plaintext, err := openFragment(r.aead, ciphertext, aad, nonce[:])
		if err != nil {
			return totalCopied, fmt.Errorf("segment %d offset %d: %w", r.ext.SegNum, diskOff, ErrIntegrity)
		}

		payloadLen := int(binary.BigEndian.Uint32(r.buf[20:24]))
		if payloadLen > fragBodySize {
			return totalCopied, fmt.Errorf("invalid payload length %d in segment %d at offset %d", payloadLen, r.ext.SegNum, diskOff)
		}

		n := copy(p[totalCopied:], plaintext[bodyOffset:payloadLen])
		totalCopied += n
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
