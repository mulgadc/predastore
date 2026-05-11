package store

import (
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"
)

// Fragment layout (8240 bytes):
//
//	[0:32]      header (see below)
//	[32:8224]   body — AES-256-GCM ciphertext (same length as plaintext under GCM/CTR)
//	[8224:8240] tag  — 16-byte GCM authentication tag binding ciphertext + AAD
//
// Fragment header layout (32 bytes):
//
//	[0:8]   fragNum  — global fragment counter (monotonic across segments)
//	[8:16]  shardNum — shard identifier
//	[16:20] reserved
//	[20:24] size     — actual data bytes in this fragment's body (≤ fragBodySize)
//	[24:28] flags    — fragFlags (flagEndOfShard marks the last fragment of a shard)
//	[28:32] reserved
const (
	fragHeaderSize = 32
	fragBodySize   = 8 * KiB
	fragTagSize    = 16
	totalFragSize  = fragHeaderSize + fragBodySize + fragTagSize

	// bufLen is the per-shard fragment window: shardWriter buffers this many
	// fragments before issuing one WriteAt, and shardReader fills the same
	// window per seg.ReadAt. Trades RAM (≈ bufLen * 8 KiB per reader/writer)
	// for syscall amortization. Cap at shard's fragment count when smaller.
	bufLen = 32
)

// AAD layout (52 bytes, big-endian):
//
//	[0:32]   objectHash (SHA-256, already part of the shard key)
//	[32:36]  shardIndex
//	[36:44]  shardNum   (from the on-disk fragment header at read time)
//	[44:52]  fragNum    (from the on-disk fragment header at read time)
const aadSize = 52

type fragFlags uint32

const (
	flagEndOfShard fragFlags = 1 << iota
)

// ErrIntegrity is returned when a fragment's GCM tag fails to authenticate —
// either the ciphertext, the on-disk header bytes that feed AAD reconstruction,
// or the master key / storeID do not match what was bound at seal time.
var ErrIntegrity = errors.New("fragment integrity check failed")

// fragment is a fixed-size view over the on-disk fragment layout. Callers
// instantiate it by casting a slice of a window buffer:
//
//	frag := (*fragment)(buf[pos : pos+totalFragSize])
//
// Header reads/writes, body fill, seal/open, and AAD/nonce construction all
// live here so reader.go and writer.go can stay focused on streaming and
// batching.
type fragment [totalFragSize]byte

func (f *fragment) fragNum() uint64       { return binary.BigEndian.Uint64(f[0:8]) }
func (f *fragment) shardNum() uint64      { return binary.BigEndian.Uint64(f[8:16]) }
func (f *fragment) size() uint32          { return binary.BigEndian.Uint32(f[20:24]) }
func (f *fragment) setSize(n uint32)      { binary.BigEndian.PutUint32(f[20:24], n) }
func (f *fragment) setFlags(fl fragFlags) { binary.BigEndian.PutUint32(f[24:28], uint32(fl)) }

// stampHeader writes fragNum + shardNum and zeroes the rest of the 32-byte
// header. size and flags are filled in at seal time.
func (f *fragment) stampHeader(fragNum, shardNum uint64) {
	binary.BigEndian.PutUint64(f[0:8], fragNum)
	binary.BigEndian.PutUint64(f[8:16], shardNum)
	clear(f[16:fragHeaderSize])
}

// body returns the body slot. Capacity reaches the end of the tag slot so
// Seal can append the GCM tag in place.
func (f *fragment) body() []byte {
	return f[fragHeaderSize : fragHeaderSize+fragBodySize : totalFragSize]
}

// ciphertext returns body + tag as one slice for Open.
func (f *fragment) ciphertext() []byte {
	return f[fragHeaderSize:totalFragSize]
}

// seal writes size + flags into the header, zero-pads the body past size
// (the ciphertext is fixed-length under GCM), then encrypts the body in
// place under aead. shardNum and fragNum are read from the just-stamped
// header.
func (f *fragment) seal(aead cipher.AEAD, objectHash [32]byte, shardIndex, storeID uint32, size uint32, flags fragFlags) {
	if size < fragBodySize {
		clear(f[fragHeaderSize+size : fragHeaderSize+fragBodySize])
	}
	f.setSize(size)
	f.setFlags(flags)

	aad := makeAAD(objectHash, shardIndex, f.shardNum(), f.fragNum())
	nonce := makeNonce(f.fragNum(), storeID)
	aead.Seal(f.body()[:0], nonce[:], f.body(), aad[:])
}

// open decrypts the body in place, verifies the GCM tag, validates the
// header size field, and returns the plaintext slice (length == size).
// shardNum/fragNum come from the on-disk header.
func (f *fragment) open(aead cipher.AEAD, objectHash [32]byte, shardIndex, storeID uint32) ([]byte, error) {
	aad := makeAAD(objectHash, shardIndex, f.shardNum(), f.fragNum())
	nonce := makeNonce(f.fragNum(), storeID)
	plaintext, err := aead.Open(f.ciphertext()[:0], nonce[:], f.ciphertext(), aad[:])
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrIntegrity, err)
	}

	sz := int(f.size())
	if sz > fragBodySize {
		return nil, fmt.Errorf("invalid size %d", sz)
	}

	return plaintext[:sz], nil
}

// makeNonce builds the 12-byte GCM nonce: BE(fragNum) || BE(storeID).
// fragNum is monotonic per data dir (crash-safe via state.json high-water);
// storeID is 4 random bytes generated on first Store.Open. Together they are
// unique for the lifetime of the master key.
func makeNonce(fragNum uint64, storeID uint32) [12]byte {
	var nonce [12]byte
	binary.BigEndian.PutUint64(nonce[0:8], fragNum)
	binary.BigEndian.PutUint32(nonce[8:12], storeID)
	return nonce
}

// makeAAD binds each fragment to its logical position. A tampered fragment
// header or a fragment spliced into a different shard / object / position
// produces a different AAD at read time, and GCM Open fails. Returned by
// value so it stays on the stack on the per-fragment hot path.
func makeAAD(objectHash [32]byte, shardIndex uint32, shardNum, fragNum uint64) [aadSize]byte {
	var aad [aadSize]byte
	copy(aad[0:32], objectHash[:])
	binary.BigEndian.PutUint32(aad[32:36], shardIndex)
	binary.BigEndian.PutUint64(aad[36:44], shardNum)
	binary.BigEndian.PutUint64(aad[44:52], fragNum)
	return aad
}
