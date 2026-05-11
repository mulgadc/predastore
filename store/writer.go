package store

import (
	"bytes"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const writeBufLen = 32

// shardWriter writes a single shard as a contiguous sequence of fragments.
// The internal buffer is sized writeBufLen*totalFragSize and mirrors the on-disk
// layout exactly (headers interleaved with body data and tag slots). This allows
// flush to seal each body in place and issue a single WriteAt for the whole
// buffer without re-packing.
//
// Position tracking:
//   - bufPos: bytes written into the buffer (0 → ext.PSize), includes headers
//     and tag slots; advances past the 16-byte tag slot whenever a fragment body
//     fills, so the next iteration lands on the next fragment's header.
//   - extPos: bytes already flushed to disk; buf[0:bufPos-extPos] is unflushed.
//
// Fragment headers are written inline by writeHeader() whenever bufPos lands on
// a fragment boundary. Payload length, flags, and the GCM tag are deferred to
// flush(), where each body is sealed in place under aead with AAD bound to
// (objectHash, shardIndex, shardNum, fragNum) and nonce = (fragNum, storeID).
//
// objectHash, shardIndex, and storeID are carried separately from extent to
// avoid changing the on-disk extent encoding for data already in the index.
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

// ReadFrom streams from r into the shard until dataSize is reached or r returns
// an error. Unlike Write, it returns io.EOF from r without treating it as a
// fault — partial reads are flushed before the error propagates.
func (w *shardWriter) ReadFrom(r io.Reader) (total int64, err error) {
	if w.closed {
		return 0, ErrClosedWriter
	}

	for w.dataWritten() < w.ext.LSize {
		if w.bufPos%totalFragSize == 0 {
			w.writeHeader()
		}

		bufPos := int(w.bufPos - w.extPos)
		bodyLeft := fragHeaderSize + fragBodySize - int(w.bufPos%totalFragSize)
		dataLeft := int(w.ext.LSize - w.dataWritten())

		n, readErr := r.Read(w.buf[bufPos : bufPos+min(bodyLeft, dataLeft)])
		w.bufPos += int64(n)
		total += int64(n)

		// Jump over the tag slot at the end of each fragment so the next
		// iteration's bufPos%totalFragSize == 0 triggers writeHeader cleanly.
		// The tag bytes themselves are written in place by flush()'s Seal.
		if int(w.bufPos%totalFragSize) == fragHeaderSize+fragBodySize {
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

// writeHeader writes fragNum and shardNum into the buffer at the current
// position, zeroes the remaining header fields (payloadLen, flags, and the
// reserved post-CRC slot are filled — or left zero — by flush), and advances
// bufPos past the header.
func (w *shardWriter) writeHeader() {
	bufPos := int(w.bufPos - w.extPos)
	binary.BigEndian.PutUint64(w.buf[bufPos:], w.fragNum)
	binary.BigEndian.PutUint64(w.buf[bufPos+8:], w.shardNum)
	clear(w.buf[bufPos+16 : bufPos+fragHeaderSize])
	w.bufPos += fragHeaderSize
	w.fragNum++
}

// flush writes the buffered fragments to disk in a single WriteAt. Before the
// write it fills in each fragment's payloadLen and flags, then seals the body
// in place under AES-256-GCM. If final is true the last fragment gets
// flagEndOfShard, its body is zero-padded out to fragBodySize before the seal
// (so the ciphertext length is fixed regardless of payload), and payloadLen
// reflects the actual data (which may be < fragBodySize). The reserved CRC
// slot in the header is left zero — GCM is the sole integrity authority.
func (w *shardWriter) flush(final bool) error {
	bufUsed := int(w.bufPos - w.extPos)
	if bufUsed <= 0 {
		return nil
	}

	fragCount := (bufUsed + totalFragSize - 1) / totalFragSize
	writeLen := fragCount * totalFragSize

	for i := range fragCount {
		pos := i * totalFragSize

		// Determine actual payload length. The final fragment may be partial
		// (body not yet full → bufUsed - pos - fragHeaderSize is the data byte
		// count) or fully filled and already tag-skipped (bufUsed - pos -
		// fragHeaderSize would be fragBodySize + fragTagSize, so cap with min).
		// Pad the body tail with zeros so the ciphertext always covers the
		// full fragBodySize.
		bodySize := fragBodySize
		if final && i == fragCount-1 {
			bodySize = min(bufUsed-pos-fragHeaderSize, fragBodySize)
			if bodySize < fragBodySize {
				clear(w.buf[pos+fragHeaderSize+bodySize : pos+fragHeaderSize+fragBodySize])
			}
		}
		binary.BigEndian.PutUint32(w.buf[pos+20:pos+24], uint32(bodySize)) //nolint:gosec // bodySize bounded by fragBodySize (8 KiB).

		var flags fragFlags
		if final && i == fragCount-1 {
			flags = flagEndOfShard
		}
		binary.BigEndian.PutUint32(w.buf[pos+24:pos+28], uint32(flags))

		// Reconstruct AAD/nonce from the header fields just written: the
		// reader will do the same reconstruction at Open time. Header tamper
		// at rest changes the reader's AAD → tag fails → ErrIntegrity.
		fragNum := binary.BigEndian.Uint64(w.buf[pos : pos+8])
		shardNum := binary.BigEndian.Uint64(w.buf[pos+8 : pos+16])
		aad := makeAAD(w.objectHash, w.shardIndex, shardNum, fragNum)
		nonce := makeNonce(fragNum, w.storeID)

		// Seal in place: body slice has cap reaching exactly the tag slot, so
		// Seal's append lands ciphertext + tag in buf[pos+32 : pos+totalFragSize]
		// without reallocating.
		body := w.buf[pos+fragHeaderSize : pos+fragHeaderSize+fragBodySize : pos+totalFragSize]
		_ = sealFragment(w.aead, body, aad, nonce[:])
	}

	if _, err := w.seg.WriteAt(w.buf[:writeLen], w.ext.Off+w.extPos); err != nil {
		return fmt.Errorf("write to segment %d at offset %d: %w", w.ext.SegNum, w.ext.Off+w.extPos, err)
	}

	w.extPos = w.bufPos
	return nil
}
